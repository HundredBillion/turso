use super::*;
use crate::io::{FileId, OpenFlags};
use crate::mvcc::database::shared_prepared_checkpoint_fixture;
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
        for name in ["manifest"] {
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
                return Err(LimboError::InvalidArgument(
                    "injected checkpoint completion mutation failure".into(),
                ));
            }
            if step == self.delay.load(Ordering::Acquire) {
                assert!(self.pending.lock().replace(pending).is_none());
                return Ok(completion);
            }
        }
        self.perform(&pending)
    }
    fn selected_durable(&self, database_id: FileId) -> ManifestRecord {
        let durable = self.durable.lock();
        let bytes = &durable["manifest"];
        select_latest(
            [
                ManifestRecord::decode(&bytes[..RECORD_SIZE]).unwrap(),
                ManifestRecord::decode(&bytes[SLOT_SIZE..SLOT_SIZE + RECORD_SIZE]).unwrap(),
            ],
            database_id,
        )
        .unwrap()
        .1
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
            Some("manifest")
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
fn checkpoint_completion_native_failure_preserves_materialized_recovery_boundary() {
    for failure in 0..=4 {
        let control = Arc::new(Control::default());
        let fixture = shared_prepared_checkpoint_fixture(|_| controlled_io(control.clone()));
        let source = fixture.token.record;
        let target = fixture.prepared.target();
        let log_before = std::fs::read(fixture.namespace.join("logical")).unwrap();
        control.arm(&fixture.namespace);
        control.fail.store(failure, Ordering::Release);
        let mut publication = fixture
            .token
            .begin_completion(fixture.materialized, fixture.prepared)
            .unwrap();
        let result = publication.poll();
        if failure < 4 {
            assert!(result.is_err(), "boundary {failure}");
        } else {
            let IOResult::Done(completed) = result.unwrap() else {
                panic!("synchronous publication");
            };
            let (proof, mut guard, ids) = completed.into_parts();
            assert_eq!(proof.target_identity().database, target);
            assert!(proof.matches_lease(&guard));
            assert_eq!(guard.published().unwrap(), source.prefix());
            guard.publish_checkpoint(&proof).unwrap();
            drop(ids);
            guard.finish().unwrap();
        }
        assert!(publication.poll().is_err());
        drop(publication);
        let selected = control.selected_durable(source.database_id());
        if selected.checkpoint_intent().is_some() {
            assert_eq!(selected, source);
        } else {
            assert_eq!(selected.log_generation(), target.generation);
            assert_eq!(
                selected.prefix().committed_timestamp,
                target.checkpoint_timestamp
            );
            assert_eq!(selected.prefix().end_offset, 0);
        }
        assert_eq!(
            std::fs::read(fixture.namespace.join("logical")).unwrap(),
            log_before
        );
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_completion_native_abort_retains_physical_authority_at_every_boundary() {
    for delayed in 0..4 {
        let control = Arc::new(Control::default());
        let fixture = shared_prepared_checkpoint_fixture(|_| controlled_io(control.clone()));
        let contender = SharedMvccAuthority::open_existing(
            &fixture.io,
            fixture.namespace.join("authority").to_str().unwrap(),
            fixture.token.record.database_id(),
            fixture.limit,
        )
        .unwrap();
        control.arm(&fixture.namespace);
        control.delay.store(delayed, Ordering::Release);
        let mut publication = fixture
            .token
            .begin_completion(fixture.materialized, fixture.prepared)
            .unwrap();
        let IOResult::IO(waiter) = publication.poll().unwrap() else {
            panic!("deferred mutation");
        };
        waiter.0.abort();
        assert!(publication.poll().is_err());
        drop(waiter);
        drop(publication);
        assert!(matches!(contender.try_acquire(), Err(LimboError::Busy)));
        let pending = control.pending.lock().take().unwrap();
        let retained = pending.completion.clone();
        drop(control.perform(&pending).unwrap());
        drop(pending);
        assert!(matches!(contender.try_acquire(), Err(LimboError::Busy)));
        drop(retained);
        let guard = contender.try_acquire().unwrap();
        assert!(guard.requires_recovery());
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_completion_native_proof_waits_for_final_backend_retirement() {
    let control = Arc::new(Control::default());
    let fixture = shared_prepared_checkpoint_fixture(|_| controlled_io(control.clone()));
    control.arm(&fixture.namespace);
    control.delay.store(3, Ordering::Release);
    let mut publication = fixture
        .token
        .begin_completion(fixture.materialized, fixture.prepared)
        .unwrap();
    assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
    let pending = control.pending.lock().take().unwrap();
    let retained = pending.completion.clone();
    drop(control.perform(&pending).unwrap());
    drop(pending);
    let IOResult::IO(waiter) = publication.poll().unwrap() else {
        panic!("proof escaped before backend retirement");
    };
    assert!(!waiter.0.finished());
    drop(retained);
    assert!(waiter.0.finished());
    drop(waiter);
    let IOResult::Done(completed) = publication.poll().unwrap() else {
        panic!("retired publication");
    };
    let (proof, mut guard, ids) = completed.into_parts();
    assert!(proof.matches_lease(&guard));
    guard.publish_checkpoint(&proof).unwrap();
    drop(ids);
    guard.finish().unwrap();
}
