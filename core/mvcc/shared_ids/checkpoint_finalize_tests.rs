use super::*;
use crate::io::{File, FileId, FileSyncType, OpenFlags};
use crate::mvcc::database::shared_completed_checkpoint_fixture;
use crate::mvcc::shared_authority::SharedMvccAuthority;
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

enum Mutation {
    Write(u64, Arc<Buffer>),
    Truncate(u64),
    Sync(FileSyncType),
}
struct Pending {
    file: Arc<dyn File>,
    path: PathBuf,
    mutation: Mutation,
    completion: Completion,
}
struct Control {
    armed: AtomicBool,
    step: AtomicUsize,
    fail: AtomicUsize,
    delay: AtomicUsize,
    pending: Mutex<Option<Pending>>,
    durable: Mutex<BTreeMap<String, Vec<u8>>>,
}
impl Default for Control {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            step: AtomicUsize::new(0),
            fail: AtomicUsize::new(usize::MAX),
            delay: AtomicUsize::new(usize::MAX),
            pending: Mutex::new(None),
            durable: Mutex::new(BTreeMap::new()),
        }
    }
}
impl Control {
    fn arm(&self, namespace: &Path) {
        for name in ["ids-a", "ids-b", "ids-selector"] {
            self.durable
                .lock()
                .insert(name.into(), std::fs::read(namespace.join(name)).unwrap());
        }
        self.armed.store(true, Ordering::Release);
    }
    fn perform(&self, pending: &Pending) -> Result<Completion> {
        let result = match &pending.mutation {
            Mutation::Write(offset, bytes) => {
                pending
                    .file
                    .pwrite(*offset, bytes.clone(), pending.completion.clone())
            }
            Mutation::Truncate(size) => pending.file.truncate(*size, pending.completion.clone()),
            Mutation::Sync(kind) => pending.file.sync(pending.completion.clone(), *kind),
        }?;
        assert!(result.finished(), "fixture uses synchronous native IO");
        if matches!(pending.mutation, Mutation::Sync(_)) && self.armed.load(Ordering::Acquire) {
            self.durable.lock().insert(
                pending.path.file_name().unwrap().to_str().unwrap().into(),
                std::fs::read(&pending.path).unwrap(),
            );
        }
        Ok(result)
    }
    fn mutation(
        &self,
        file: Arc<dyn File>,
        path: PathBuf,
        mutation: Mutation,
        completion: Completion,
    ) -> Result<Completion> {
        let pending = Pending {
            file,
            path,
            mutation,
            completion: completion.clone(),
        };
        if self.armed.load(Ordering::Acquire) {
            let step = self.step.fetch_add(1, Ordering::AcqRel);
            if step == self.fail.load(Ordering::Acquire) {
                return Err(invalid("injected ID finalization mutation failure"));
            }
            if step == self.delay.load(Ordering::Acquire) {
                assert!(self.pending.lock().replace(pending).is_none());
                return Ok(completion);
            }
        }
        self.perform(&pending)
    }
    fn assert_recoverable(&self, id: FileId, target: DatabaseGeneration, floor: i64) {
        let durable = self.durable.lock();
        let selector = &durable["ids-selector"];
        let selected = select(
            [&selector[..PAGE_SIZE], &selector[PAGE_SIZE..]],
            [&durable["ids-a"], &durable["ids-b"]],
            id,
            target,
        )
        .unwrap();
        let state = replay_selected(selected, [&durable["ids-a"], &durable["ids-b"]]).unwrap();
        assert!(state.logical_floor <= floor);
        assert_eq!(selected.selected.database, target);
    }
}
struct ControlledFile {
    inner: Arc<dyn File>,
    path: PathBuf,
    control: Arc<Control>,
}
impl File for ControlledFile {
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
        bytes: Arc<Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        self.control.mutation(
            self.inner.clone(),
            self.path.clone(),
            Mutation::Write(offset, bytes),
            completion,
        )
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.control.mutation(
            self.inner.clone(),
            self.path.clone(),
            Mutation::Truncate(size),
            completion,
        )
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        self.control.mutation(
            self.inner.clone(),
            self.path.clone(),
            Mutation::Sync(kind),
            completion,
        )
    }
}
struct ControlledIo {
    inner: Arc<dyn crate::IO>,
    control: Arc<Control>,
}
impl crate::Clock for ControlledIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl crate::IO for ControlledIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        if matches!(
            Path::new(path).file_name().and_then(|name| name.to_str()),
            Some("ids-a" | "ids-b" | "ids-selector")
        ) {
            Ok(Arc::new(ControlledFile {
                inner,
                path: path.into(),
                control: self.control.clone(),
            }))
        } else {
            Ok(inner)
        }
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
fn controlled_io(control: Arc<Control>) -> Arc<dyn crate::IO> {
    Arc::new(ControlledIo {
        inner: Arc::new(crate::io::PlatformIO::new().unwrap()),
        control,
    })
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn id_checkpoint_finalize_native_preserves_target_at_each_failure_boundary() {
    for failure in 0..=6 {
        let control = Arc::new(Control::default());
        let fixture = shared_completed_checkpoint_fixture(|_| controlled_io(control.clone()));
        let target = fixture.proof.target_identity();
        let floor = fixture.proof.materialized_floor();
        let id = fixture.ids.loaded.files.database_id;
        let old_bank = usize::from(fixture.ids.loaded.selection.selector.active.bank);
        let log_before = std::fs::read(fixture.namespace.join("logical")).unwrap();
        control.arm(&fixture.namespace);
        control.fail.store(failure, Ordering::Release);
        let mut finalizer = fixture.ids.finalize(fixture.guard, fixture.proof).unwrap();
        let result = finalizer.poll();
        if failure < 6 {
            assert!(result.is_err(), "failure {failure}");
        } else {
            let IOResult::Done(finalized) = result.unwrap() else {
                panic!("synchronous finalization")
            };
            assert_eq!(finalized.proof().target_identity(), target);
            assert_eq!(finalized.guard().published().unwrap(), target.prefix);
            let (loaded, _proof) = finalized.into_parts();
            assert_eq!(loaded.identity, target);
            assert!(loaded.selection.selector.prepared.is_none());
            assert_eq!(loaded.retained_bytes().unwrap(), INITIAL_RETAINED_BYTES);
            assert!(loaded.guard.io_retired());
            loaded.into_guard().finish().unwrap();
            assert!(std::fs::read(fixture.namespace.join(if old_bank == 0 {
                "ids-a"
            } else {
                "ids-b"
            }))
            .unwrap()
            .is_empty());
        }
        assert!(finalizer.poll().is_err());
        drop(finalizer);
        control.assert_recoverable(id, target.database, floor);
        assert_eq!(
            std::fs::read(fixture.namespace.join("logical")).unwrap(),
            log_before
        );
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn id_checkpoint_finalize_native_abort_retains_authority_through_backend_retirement() {
    for delayed in 0..6 {
        let control = Arc::new(Control::default());
        let fixture = shared_completed_checkpoint_fixture(|_| controlled_io(control.clone()));
        let target = fixture.proof.target_identity();
        let floor = fixture.proof.materialized_floor();
        let id = fixture.ids.loaded.files.database_id;
        let authority = SharedMvccAuthority::open_existing(
            &fixture.io,
            fixture.namespace.join("authority").to_str().unwrap(),
            id,
            fixture.limit,
        )
        .unwrap();
        control.arm(&fixture.namespace);
        control.delay.store(delayed, Ordering::Release);
        let mut finalizer = fixture.ids.finalize(fixture.guard, fixture.proof).unwrap();
        let IOResult::IO(waiter) = finalizer.poll().unwrap() else {
            panic!("deferred mutation {delayed}")
        };
        waiter.0.abort();
        drop(waiter);
        drop(finalizer);
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        let pending = control.pending.lock().take().unwrap();
        drop(control.perform(&pending).unwrap());
        assert!(
            matches!(authority.try_acquire(), Err(LimboError::Busy)),
            "physical callback still retained at {delayed}"
        );
        drop(pending);
        assert!(authority.try_acquire().unwrap().requires_recovery());
        control.assert_recoverable(id, target.database, floor);
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn id_checkpoint_finalize_native_rejects_foreign_proof_or_reacquired_lease() {
    for case in 0..3 {
        let control = Arc::new(Control::default());
        let fixture = shared_completed_checkpoint_fixture(|_| controlled_io(control.clone()));
        let other = shared_completed_checkpoint_fixture(|_| {
            Arc::new(crate::io::PlatformIO::new().unwrap())
        });
        control.arm(&fixture.namespace);
        let result = match case {
            0 => fixture.ids.finalize(other.guard, fixture.proof),
            1 => fixture.ids.finalize(fixture.guard, other.proof),
            _ => {
                let id = fixture.ids.loaded.files.database_id;
                fixture.guard.finish().unwrap();
                let authority = SharedMvccAuthority::open_existing(
                    &fixture.io,
                    fixture.namespace.join("authority").to_str().unwrap(),
                    id,
                    fixture.limit,
                )
                .unwrap();
                let guard = authority.try_acquire().unwrap();
                fixture.ids.finalize(guard, fixture.proof)
            }
        };
        assert!(matches!(result, Err(LimboError::InvalidArgument(_))));
        assert_eq!(control.step.load(Ordering::Acquire), 0);
    }
}
