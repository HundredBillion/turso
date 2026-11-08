use super::*;
use crate::io::{FileSyncType, SharedWalLockKind, SharedWalMappedRegion};
use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE};
use crate::sync::atomic::AtomicUsize;
use std::io::{BufRead, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub(super) enum CrashPoint {
    SecondIntentWrite,
    FirstMainBackfillWrite,
}
impl CrashPoint {
    fn name(self) -> &'static str {
        match self {
            Self::SecondIntentWrite => "second-intent",
            Self::FirstMainBackfillWrite => "first-main",
        }
    }
}

pub(super) struct IntentCrashFixture {
    pub runtime: Arc<SharedMvccRuntime>,
    pub io: Arc<dyn crate::IO>,
    pub path: PathBuf,
    pub main_before: Vec<u8>,
    pub durable_before: Vec<(PathBuf, Vec<u8>)>,
    _directory: tempfile::TempDir,
}
impl IntentCrashFixture {
    pub fn new(point: CrashPoint, acknowledged_commits: usize) -> Self {
        eprintln!(
            "checkpoint crash fixture {point:?}, acknowledged commits={acknowledged_commits}"
        );
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let main_before = std::fs::read(&path).unwrap();
        let mut child = CrashChild::spawn(&path, limit, point, acknowledged_commits);
        assert_eq!(
            child.receive(),
            format!("READY {} {acknowledged_commits}", point.name())
        );
        child.kill();
        assert_eq!(std::fs::read(&path).unwrap(), main_before);
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let canonical = path.canonicalize().unwrap();
        let main = io
            .open_file(canonical.to_str().unwrap(), crate::OpenFlags::NoLock, false)
            .unwrap();
        let database_id = main.file_id().unwrap().unwrap();
        let runtime = SharedMvccRuntime::open_existing(
            io.clone(),
            main,
            database_id,
            &canonical,
            limit,
            false,
        )
        .unwrap();
        let durable_before = [
            path.clone(),
            runtime.files.namespace.join("physical-wal"),
            runtime.files.namespace.join("logical"),
            runtime.files.namespace.join("ids-a"),
            runtime.files.namespace.join("ids-b"),
            runtime.files.namespace.join("ids-selector"),
        ]
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
        Self {
            runtime,
            io,
            path,
            main_before,
            durable_before,
            _directory: directory,
        }
    }
    pub fn assert_durable_unchanged(&self) {
        for (path, bytes) in &self.durable_before {
            assert_eq!(
                &std::fs::read(path).unwrap(),
                bytes,
                "recovery rewrote {}",
                path.display()
            );
        }
    }
}

struct CrashChild {
    child: Child,
    output: Receiver<std::io::Result<String>>,
}
impl CrashChild {
    fn spawn(path: &Path, limit: u64, point: CrashPoint, acknowledged_commits: usize) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "mvcc::database::shared_runtime::shared_checkpoint_intent_fixture::checkpoint_intent_crash_child", "--ignored", "--nocapture", "--test-threads=1"])
            .env("TURSO_INTENT_CRASH_PATH", path)
            .env("TURSO_INTENT_CRASH_LIMIT", limit.to_string())
            .env("TURSO_INTENT_CRASH_POINT", point.name())
            .env("TURSO_INTENT_CRASH_COMMITS", acknowledged_commits.to_string())
            .stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, output) = mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        Self { child, output }
    }
    fn receive(&self) -> String {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("checkpoint child acknowledgement exceeded deadline");
            let line = self
                .output
                .recv_timeout(remaining)
                .expect("checkpoint child failed before acknowledgement")
                .unwrap();
            if let Some((_, message)) = line.split_once("INTENT_CRASH ") {
                return message.to_owned();
            }
        }
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert_eq!(status.signal(), Some(9));
                return;
            }
            assert!(
                Instant::now() < deadline,
                "checkpoint child did not exit after SIGKILL"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for CrashChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
#[ignore = "subprocess helper; requires a parent-provided checkpoint fixture"]
fn checkpoint_intent_crash_child() {
    let path = std::env::var("TURSO_INTENT_CRASH_PATH").unwrap();
    let limit = std::env::var("TURSO_INTENT_CRASH_LIMIT")
        .unwrap()
        .parse()
        .unwrap();
    let point = std::env::var("TURSO_INTENT_CRASH_POINT").unwrap();
    let commits: usize = std::env::var("TURSO_INTENT_CRASH_COMMITS")
        .unwrap()
        .parse()
        .unwrap();
    let canonical = Path::new(&path).canonicalize().unwrap();
    let id = crate::io::get_file_id(canonical.to_str().unwrap()).unwrap();
    let namespace = crate::mvcc::mode_transition::namespace_for(&canonical, id).unwrap();
    let control = Arc::new(CrashControl {
        point: match point.as_str() {
            "second-intent" => CrashPoint::SecondIntentWrite,
            "first-main" => CrashPoint::FirstMainBackfillWrite,
            _ => panic!("invalid crash point"),
        },
        main: canonical,
        manifest: namespace.join("manifest"),
        wal: namespace.join("physical-wal"),
        intent_writes: AtomicUsize::new(0),
        intent_syncs: AtomicUsize::new(0),
        wal_syncs: AtomicUsize::new(0),
        last_manifest_intent: AtomicBool::new(false),
        pending: Mutex::new(Vec::new()),
    });
    let io: Arc<dyn crate::IO> = Arc::new(CrashIo {
        inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
        control: control.clone(),
    });
    let database = crate::Database::open(
        io.clone(),
        &path,
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let connection = database.connect(crate::ConnectOpts::new()).unwrap();
    for ordinal in 1..=commits {
        connection
            .execute(format!(
                "UPDATE preserved SET value='acknowledged before checkpoint {ordinal}' WHERE id=1"
            ))
            .unwrap();
    }
    let store = database.get_mv_store().as_ref().unwrap().clone();
    let mut checkpoint =
        super::shared_checkpoint::SharedCheckpointPublication::new(store, connection).unwrap();
    loop {
        match checkpoint.poll().unwrap() {
            IOResult::IO(waiter) => {
                if !control.pending.lock().is_empty() {
                    println!("INTENT_CRASH READY {point} {commits}");
                    std::io::stdout().flush().unwrap();
                    loop {
                        std::thread::park();
                    }
                }
                waiter.wait(io.as_ref()).unwrap();
            }
            IOResult::Done(_) => panic!("checkpoint passed its intended crash boundary"),
        }
    }
}

#[derive(Clone, Copy)]
enum FileRole {
    Main,
    Manifest,
    Wal,
    Other,
}
struct CrashControl {
    point: CrashPoint,
    main: PathBuf,
    manifest: PathBuf,
    wal: PathBuf,
    intent_writes: AtomicUsize,
    intent_syncs: AtomicUsize,
    wal_syncs: AtomicUsize,
    last_manifest_intent: AtomicBool,
    pending: Mutex<Vec<Completion>>,
}
struct CrashIo {
    inner: Arc<dyn crate::IO>,
    control: Arc<CrashControl>,
}
impl crate::Clock for CrashIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl crate::IO for CrashIo {
    fn open_file(
        &self,
        path: &str,
        flags: crate::OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        let role = if Path::new(path) == self.control.main {
            FileRole::Main
        } else if Path::new(path) == self.control.manifest {
            FileRole::Manifest
        } else if Path::new(path) == self.control.wal {
            FileRole::Wal
        } else {
            FileRole::Other
        };
        Ok(Arc::new(CrashFile {
            inner: self.inner.open_file(path, flags, direct)?,
            role,
            control: self.control.clone(),
        }))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }
    fn step(&self) -> Result<()> {
        self.inner.step()
    }
}
struct CrashFile {
    inner: Arc<dyn File>,
    role: FileRole,
    control: Arc<CrashControl>,
}
impl File for CrashFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        self.inner.file_id()
    }
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn pread(&self, offset: u64, completion: Completion) -> Result<Completion> {
        self.inner.pread(offset, completion)
    }
    fn pwrite(
        &self,
        offset: u64,
        buffer: Arc<crate::Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        let stop = match self.role {
            FileRole::Manifest => {
                let intent = buffer.len() >= RECORD_SIZE
                    && ManifestRecord::decode(&buffer.as_slice()[..RECORD_SIZE])?
                        .is_some_and(|record| record.checkpoint_intent().is_some());
                self.control
                    .last_manifest_intent
                    .store(intent, Ordering::Release);
                intent
                    && self
                        .control
                        .intent_writes
                        .fetch_add(usize::from(intent), Ordering::AcqRel)
                        + 1
                        == 2
                    && matches!(self.control.point, CrashPoint::SecondIntentWrite)
            }
            FileRole::Main => {
                matches!(self.control.point, CrashPoint::FirstMainBackfillWrite)
                    && self.control.intent_syncs.load(Ordering::Acquire) == 2
            }
            _ => false,
        };
        if stop {
            match self.control.point {
                CrashPoint::SecondIntentWrite => {
                    assert_eq!(self.control.intent_syncs.load(Ordering::Acquire), 1)
                }
                CrashPoint::FirstMainBackfillWrite => {
                    assert_eq!(self.control.intent_syncs.load(Ordering::Acquire), 2);
                    assert!(self.control.wal_syncs.load(Ordering::Acquire) > 0);
                }
            }
            self.control.pending.lock().push(completion.clone());
            return Ok(completion);
        }
        self.inner.pwrite(offset, buffer, completion)
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        let complete = self.inner.sync(completion, kind)?;
        assert!(complete.succeeded(), "fixture uses synchronous native I/O");
        match self.role {
            FileRole::Manifest
                if kind == FileSyncType::FullFsync
                    && self.control.last_manifest_intent.load(Ordering::Acquire) =>
            {
                self.control.intent_syncs.fetch_add(1, Ordering::AcqRel);
            }
            FileRole::Wal => {
                self.control.wal_syncs.fetch_add(1, Ordering::AcqRel);
            }
            _ => {}
        }
        Ok(complete)
    }
    fn truncate(&self, len: u64, completion: Completion) -> Result<Completion> {
        self.inner.truncate(len, completion)
    }
    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        self.inner.shared_wal_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.inner.shared_wal_try_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        self.inner.shared_wal_unlock_byte(offset, kind)
    }
    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.inner.shared_wal_set_len(len)
    }
    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        self.inner.shared_wal_map(offset, len)
    }
}
