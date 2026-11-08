use super::*;
use crate::io::{FileId, OpenFlags};
use crate::sync::atomic::{AtomicUsize, Ordering};
use std::path::{Path, PathBuf};

struct PendingRead {
    file: Arc<dyn File>,
    offset: u64,
    completion: Completion,
}

#[derive(Default)]
struct ReadControl {
    next: AtomicUsize,
    target: AtomicUsize,
    pending: Mutex<Option<PendingRead>>,
}

impl ReadControl {
    fn arm(&self, index: usize) {
        assert!(self.pending.lock().is_none());
        self.next.store(0, Ordering::Release);
        self.target.store(index, Ordering::Release);
    }

    fn callback(&self) {
        let pending = self.pending.lock();
        let pending = pending.as_ref().unwrap();
        let completed = pending
            .file
            .pread(pending.offset, pending.completion.clone())
            .unwrap();
        assert!(
            completed.finished(),
            "fixture requires synchronous native reads"
        );
    }
}

struct ControlledFile {
    inner: Arc<dyn File>,
    control: Arc<ReadControl>,
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
        if self.control.next.fetch_add(1, Ordering::AcqRel)
            == self.control.target.load(Ordering::Acquire)
        {
            assert!(self
                .control
                .pending
                .lock()
                .replace(PendingRead {
                    file: self.inner.clone(),
                    offset,
                    completion: completion.clone(),
                })
                .is_none());
            return Ok(completion);
        }
        self.inner.pread(offset, completion)
    }
    fn pwrite(
        &self,
        offset: u64,
        bytes: Arc<crate::Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        self.inner.pwrite(offset, bytes, completion)
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        self.inner.sync(completion, kind)
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.inner.truncate(size, completion)
    }
}

struct ControlledIo {
    inner: Arc<dyn crate::IO>,
    path: PathBuf,
    control: Arc<ReadControl>,
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
        if Path::new(path) == self.path {
            Ok(Arc::new(ControlledFile {
                inner,
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

struct Fixture {
    _directory: tempfile::TempDir,
    io: Arc<dyn crate::IO>,
    connection: Arc<Connection>,
    store: Arc<crate::MvStore>,
    runtime: Arc<SharedMvccRuntime>,
    published: Option<PublishedSharedCheckpoint>,
    control: Arc<ReadControl>,
}

fn fixture() -> Fixture {
    let control = Arc::new(ReadControl::default());
    control.target.store(usize::MAX, Ordering::Release);
    let fixture = shared_completed_checkpoint_fixture(|path| {
        Arc::new(ControlledIo {
            inner: Arc::new(crate::io::PlatformIO::new().unwrap()),
            path: path.into(),
            control: control.clone(),
        })
    });
    let mut finalizer = fixture.ids.finalize(fixture.guard, fixture.proof).unwrap();
    let finalized = loop {
        match finalizer.poll().unwrap() {
            IOResult::IO(waiter) => fixture.io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(finalized) => break finalized,
        }
    };
    let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap().clone();
    Fixture {
        _directory: fixture.directory,
        io: fixture.io,
        connection: fixture.connection,
        store,
        runtime,
        published: Some(PublishedSharedCheckpoint {
            finalized,
            pause: fixture.pause,
        }),
        control,
    }
}

fn delayed_read(
    fixture: &mut Fixture,
    index: usize,
) -> SharedCheckpointPhysicalRead<crate::mvcc::MvccClock, crate::alloc::DynAllocator> {
    fixture.control.arm(index);
    let mut reading = fixture
        .published
        .take()
        .unwrap()
        .begin_physical_read(fixture.store.clone(), fixture.connection.clone())
        .unwrap();
    for _ in 0..100 {
        match reading.poll().unwrap() {
            IOResult::IO(waiter) if fixture.control.pending.lock().is_some() => {
                assert!(!waiter.0.finished());
                let pending = fixture.control.pending.lock();
                let pending = pending.as_ref().unwrap();
                if index == 0 {
                    assert_eq!(pending.offset, 0);
                    assert_eq!(pending.completion.as_read().buf_arc().len(), 512);
                } else {
                    assert!(
                        pending.completion.as_read().buf_arc().len() > 512,
                        "delay a schema page rather than a header"
                    );
                }
                return reading;
            }
            IOResult::IO(waiter) => fixture.io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(_) => panic!("physical staging bypassed delayed read"),
        }
    }
    panic!("physical staging did not submit the selected read")
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_physical_native_cancel_retains_authority_through_header_and_schema_read_retirement(
) {
    for index in [0, 1] {
        for before_cancel in [false, true] {
            let mut fixture = fixture();
            let schema = fixture.connection.db.schema.lock().clone();
            let cached = fixture.connection.get_pager().cache_get(1).unwrap();
            let main = std::fs::read(&fixture.connection.db.path).unwrap();
            let reading = delayed_read(&mut fixture, index);
            if before_cancel {
                fixture.control.callback();
            }
            drop(reading);
            assert!(
                matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ),
                "cancelled physical read released authority before backend retirement"
            );
            assert!(matches!(
                fixture.runtime.begin_open(),
                Err(LimboError::Busy)
            ));
            if !before_cancel {
                fixture.control.callback();
            }
            assert!(
                matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ),
                "callback readiness is not physical retirement"
            );
            assert!(Arc::ptr_eq(&schema, &fixture.connection.db.schema.lock()));
            match (cached, fixture.connection.get_pager().cache_get(1).unwrap()) {
                (Some(before), Some(after)) => assert!(Arc::ptr_eq(&before, &after)),
                (None, None) => {}
                _ => panic!("private read modified source pager cache"),
            }
            assert_eq!(std::fs::read(&fixture.connection.db.path).unwrap(), main);
            fixture.control.pending.lock().take();
            assert!(fixture
                .runtime
                .files
                .authority
                .try_acquire()
                .unwrap()
                .requires_recovery());
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_physical_native_evidence_waits_for_backend_retirement() {
    let mut fixture = fixture();
    let mut reading = delayed_read(&mut fixture, 1);
    fixture.control.callback();
    let mut retirement = None;
    for _ in 0..100 {
        match reading.poll().unwrap() {
            IOResult::IO(waiter) if !waiter.0.finished() => {
                assert!(
                    reading.parts.is_some(),
                    "finish fact collection before retirement wait"
                );
                retirement = Some(waiter.0);
                break;
            }
            IOResult::IO(waiter) => fixture.io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(_) => panic!("physical evidence escaped before backend retirement"),
        }
    }
    let retirement = retirement.expect("backend retirement must be observable");
    assert!(!retirement.finished());
    assert!(matches!(
        fixture.runtime.gate.try_enter_admission(),
        Err(LimboError::Busy)
    ));
    fixture.control.pending.lock().take();
    assert!(retirement.finished());
    let verified = fixture.io.block(|| reading.poll()).unwrap();
    let (_, published) = verified.into_parts();
    assert!(published.finalized.guard().io_retired());
}
