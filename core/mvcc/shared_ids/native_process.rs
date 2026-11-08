use super::io_protocol::{RecoveryIdentity, SharedIds};
use super::journal::{Domain, Owner};
use super::policy::{CommittedIdOracle, IdAction, IdFact, IdQuery, IdResult};
use super::*;
use crate::io::File;
use crate::mvcc::guarded_file::GuardedFile;
use crate::mvcc::shared_authority::{
    AuthorityGuard, PublishedPrefix, SharedMvccAuthority, SnapshotRegistration,
};
use crate::sync::Arc;
use crate::{IOResult, OpenFlags, IO};
use std::io::Write;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const HISTORY_LIMIT: u64 = 20 * PAGE_SIZE as u64;
const TEST_NAME: &str =
    "mvcc::shared_ids::native_process::native_id_reservations_survive_death_compaction_and_reopen";
const ROLE: &str = "TURSO_NATIVE_ID_TEST_ROLE";
const DIRECTORY: &str = "TURSO_NATIVE_ID_TEST_DIRECTORY";

struct NativeIds {
    io: Arc<dyn IO>,
    authority: Arc<SharedMvccAuthority>,
    files: Arc<SharedIds>,
    identity: RecoveryIdentity,
    _main: Arc<dyn File>,
}

impl NativeIds {
    fn initialize(dir: &Path) {
        let main = dir.join("identity");
        std::fs::File::create_new(&main)
            .unwrap()
            .sync_all()
            .unwrap();
        let id = crate::io::get_file_id(main.to_str().unwrap()).unwrap();
        let bytes = InitialIds::new(
            id,
            DatabaseGeneration {
                generation: 1,
                checkpoint_timestamp: 0,
            },
            -1,
        )
        .unwrap();
        for (name, bytes) in [
            ("ids-a", bytes.bank_a.as_slice()),
            ("ids-b", &[]),
            ("ids-selector", bytes.selector.as_slice()),
        ] {
            let mut file = std::fs::File::create_new(dir.join(name)).unwrap();
            file.write_all(bytes).unwrap();
            file.sync_all().unwrap();
        }
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let authority = SharedMvccAuthority::open(
            &io,
            dir.join("authority").to_str().unwrap(),
            id,
            HISTORY_LIMIT,
        )
        .unwrap();
        let mut guard = authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.reconcile_incarnation(0).unwrap();
        guard.finish().unwrap();
        std::fs::File::open(dir.join("authority"))
            .unwrap()
            .sync_all()
            .unwrap();
        std::fs::File::open(dir).unwrap().sync_all().unwrap();
    }

    fn open(dir: &Path) -> Self {
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        assert!(io.supports_shared_wal_coordination());
        let open = |name: &str| {
            io.open_file(dir.join(name).to_str().unwrap(), OpenFlags::NoLock, false)
                .unwrap()
        };
        let main = open("identity");
        let id = main.file_id().unwrap().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut reported_busy = false;
        let authority = loop {
            match SharedMvccAuthority::open_existing(
                &io,
                dir.join("authority").to_str().unwrap(),
                id,
                HISTORY_LIMIT,
            ) {
                Ok(authority) => break authority,
                Err(LimboError::Busy) if Instant::now() < deadline => {
                    if !reported_busy {
                        if let Ok(role) = std::env::var(ROLE) {
                            std::fs::write(dir.join(format!("{role}.open-busy")), []).unwrap();
                        }
                        reported_busy = true;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(error) => panic!("native ID authority open failed: {error}"),
            }
        };
        let files = SharedIds::open_existing(
            [
                GuardedFile::new(open("ids-a"), &authority),
                GuardedFile::new(open("ids-b"), &authority),
            ],
            GuardedFile::new(open("ids-selector"), &authority),
            id,
            HISTORY_LIMIT,
        )
        .unwrap();
        Self {
            io,
            authority,
            files,
            identity: RecoveryIdentity {
                database: DatabaseGeneration {
                    generation: 1,
                    checkpoint_timestamp: 0,
                },
                prefix: PublishedPrefix::default(),
            },
            _main: main,
        }
    }

    fn acquire(&self) -> AuthorityGuard {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match self.authority.try_acquire() {
                Ok(mut guard) => {
                    if guard.requires_recovery() {
                        guard.recover(self.identity.prefix).unwrap();
                    }
                    return guard;
                }
                Err(LimboError::Busy) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(2))
                }
                other => panic!(
                    "native ID authority acquisition failed: {}",
                    match other {
                        Err(e) => e.to_string(),
                        Ok(_) => unreachable!(),
                    }
                ),
            }
        }
    }

    fn register(&self) -> SnapshotRegistration {
        let mut guard = self.acquire();
        if !guard.incarnation_ready().unwrap() {
            let mut load = self.files.load(guard, self.identity).unwrap();
            let loaded = loop {
                match load.poll().unwrap() {
                    IOResult::IO(waiter) => self.io.wait_for_completion(waiter.0).unwrap(),
                    IOResult::Done(loaded) => break loaded,
                }
            };
            let mut mirror = loaded.mirror_current_witness().unwrap();
            let proof = loop {
                match mirror.poll().unwrap() {
                    IOResult::IO(waiter) => self.io.wait_for_completion(waiter.0).unwrap(),
                    IOResult::Done(proof) => break proof,
                }
            };
            let (recovered, maximum) = proof.into_guard_and_max_owner_epoch();
            guard = recovered;
            guard.reconcile_incarnation(maximum.unwrap_or(0)).unwrap();
        }
        let registration = guard.register_snapshot().unwrap();
        guard.finish().unwrap();
        registration
    }

    fn action(&self, action: IdAction) -> i64 {
        let mut operation = self
            .files
            .start(
                self.acquire(),
                self.identity,
                Box::new(EmptyOracle(self.identity)),
                self.io.clone(),
                action,
            )
            .unwrap();
        loop {
            match operation.poll().unwrap() {
                IOResult::IO(waiter) => self.io.wait_for_completion(waiter.0).unwrap(),
                IOResult::Done(result) => {
                    let IdResult::Reserved { value, .. } = result.result else {
                        panic!("expected reservation");
                    };
                    assert!(result.required_budget_bytes <= HISTORY_LIMIT);
                    result.guard.finish().unwrap();
                    return value;
                }
            }
        }
    }

    fn compact(&self) {
        let mut load = self.files.load(self.acquire(), self.identity).unwrap();
        let loaded = loop {
            match load.poll().unwrap() {
                IOResult::IO(waiter) => self.io.wait_for_completion(waiter.0).unwrap(),
                IOResult::Done(loaded) => break loaded,
            }
        };
        let mut compact = loaded.compact_floor_only(self.identity, -1).unwrap();
        loop {
            match compact.poll().unwrap() {
                IOResult::IO(waiter) => self.io.wait_for_completion(waiter.0).unwrap(),
                IOResult::Done(loaded) => {
                    assert_eq!(loaded.retained_bytes().unwrap(), INITIAL_RETAINED_BYTES);
                    loaded.into_guard().finish().unwrap();
                    break;
                }
            }
        }
    }
}

struct EmptyOracle(RecoveryIdentity);
impl CommittedIdOracle for EmptyOracle {
    fn identity(&self) -> RecoveryIdentity {
        self.0
    }
    fn poll(&mut self, _: &AuthorityGuard, query: &IdQuery) -> Result<IOResult<IdFact>> {
        Ok(IOResult::Done(match query {
            IdQuery::RowFacts { .. } => IdFact::RowFacts {
                maximum: None,
                autoincrement_floor: 0,
            },
            IdQuery::RowExists { .. } => IdFact::RowExists(false),
            IdQuery::LogicalFloor => IdFact::LogicalFloor(-1),
            IdQuery::SequenceState { .. } => panic!("fixture does not read sequence state"),
        }))
    }
}

fn owner(registration: &SnapshotRegistration) -> Owner {
    Owner {
        epoch: registration.epoch,
        begin: registration.begin_timestamp,
    }
}
fn domain() -> Domain {
    Domain {
        generation: 1,
        table: -2,
        sequence: false,
    }
}

struct TestChild(Child);
impl Drop for TestChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
fn spawn(dir: &Path, role: &str) -> TestChild {
    TestChild(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env(ROLE, role)
            .env(DIRECTORY, dir)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}
fn wait_exists(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}
fn read_result(dir: &Path, role: &str) -> (i64, i64) {
    let path = dir.join(format!("{role}.ready"));
    wait_exists(&path);
    let text = std::fs::read_to_string(path).unwrap();
    let (row, logical) = text.trim().split_once(',').unwrap();
    (row.parse().unwrap(), logical.parse().unwrap())
}
fn child_main(dir: &Path, role: &str) {
    let context = NativeIds::open(dir);
    let registration = context.register();
    let row = context.action(IdAction::ReserveRow {
        owner: owner(&registration),
        domain: domain(),
        autoincrement: false,
    });
    let logical = context.action(IdAction::ReserveLogicalTable {
        owner: owner(&registration),
    });
    let temporary = dir.join(format!("{role}.temporary"));
    let mut ready = std::fs::File::create_new(&temporary).unwrap();
    write!(ready, "{row},{logical}").unwrap();
    ready.sync_all().unwrap();
    std::fs::rename(temporary, dir.join(format!("{role}.ready"))).unwrap();
    wait_exists(&dir.join(format!("{role}.release")));
    drop(registration);
}

#[test]
#[cfg_attr(
    not(all(target_os = "linux", target_pointer_width = "64")),
    ignore = "native shared authority requires 64-bit Linux"
)]
fn native_id_reservations_survive_death_compaction_and_reopen() {
    if let Ok(role) = std::env::var(ROLE) {
        child_main(Path::new(&std::env::var(DIRECTORY).unwrap()), &role);
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let dir = directory.path();
    NativeIds::initialize(dir);
    let io = crate::io::PlatformIO::new().unwrap();
    let startup = io
        .open_file(
            dir.join("authority").to_str().unwrap(),
            OpenFlags::NoLock,
            false,
        )
        .unwrap();
    // Another opener can hold this lifetime byte exclusively during restart bookkeeping.
    startup
        .shared_wal_lock_byte(0, true, crate::io::SharedWalLockKind::LinuxOfd)
        .unwrap();
    let mut a = spawn(dir, "a");
    let mut b = spawn(dir, "b");
    wait_exists(&dir.join("a.open-busy"));
    wait_exists(&dir.join("b.open-busy"));
    startup
        .shared_wal_unlock_byte(0, crate::io::SharedWalLockKind::LinuxOfd)
        .unwrap();
    drop(startup);
    let a_result = read_result(dir, "a");
    let b_result = read_result(dir, "b");
    let mut rows = [a_result.0, b_result.0];
    rows.sort_unstable();
    assert_eq!(
        rows,
        [1, 2],
        "both registrations remain live before any DB commit"
    );
    let mut logical = [a_result.1, b_result.1];
    logical.sort_unstable();
    assert_eq!(logical, [-3, -2]);
    a.0.kill().unwrap();
    a.0.wait().unwrap();
    let context = NativeIds::open(dir);
    let registration = context.register();
    assert_eq!(
        context.action(IdAction::ClaimExplicitRow {
            owner: owner(&registration),
            domain: domain(),
            value: a_result.0
        }),
        a_result.0
    );
    assert_eq!(
        context.action(IdAction::ReserveLogicalTable {
            owner: owner(&registration)
        }),
        -4
    );
    std::fs::write(dir.join("b.release"), []).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = b.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "surviving child did not release its registration"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    drop(registration);
    context.compact();
    drop(context);
    let reopened = NativeIds::open(dir);
    let registration = reopened.register();
    assert_eq!(
        reopened.action(IdAction::ReserveLogicalTable {
            owner: owner(&registration)
        }),
        -5
    );
    drop(registration);
}
