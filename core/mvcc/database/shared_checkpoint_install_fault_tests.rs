use super::*;
use crate::io::{FileId, OpenFlags};
use crate::sync::atomic::{AtomicUsize, Ordering};
use std::path::Path;

#[derive(Clone, Copy)]
enum Mutation {
    Truncate(u64),
    Sync(FileSyncType),
}

struct Pending {
    file: Arc<dyn File>,
    mutation: Mutation,
    completion: Completion,
}

struct Control {
    step: AtomicUsize,
    fail: AtomicUsize,
    delay: AtomicUsize,
    pending: Mutex<Option<Pending>>,
}

impl Default for Control {
    fn default() -> Self {
        Self {
            step: AtomicUsize::new(0),
            fail: AtomicUsize::new(usize::MAX),
            delay: AtomicUsize::new(usize::MAX),
            pending: Mutex::new(None),
        }
    }
}

impl Control {
    fn arm(&self, fail: usize, delay: usize) {
        assert!(self.pending.lock().is_none());
        self.step.store(0, Ordering::Release);
        self.fail.store(fail, Ordering::Release);
        self.delay.store(delay, Ordering::Release);
    }
    fn perform(pending: &Pending) -> Result<Completion> {
        match pending.mutation {
            Mutation::Truncate(size) => pending.file.truncate(size, pending.completion.clone()),
            Mutation::Sync(kind) => pending.file.sync(pending.completion.clone(), kind),
        }
    }
    fn callback(&self) {
        let pending = self.pending.lock();
        let completed = Self::perform(pending.as_ref().unwrap()).unwrap();
        assert!(
            completed.finished(),
            "fixture requires synchronous native mutations"
        );
    }
    fn mutation(
        &self,
        file: Arc<dyn File>,
        mutation: Mutation,
        completion: Completion,
    ) -> Result<Completion> {
        let pending = Pending {
            file,
            mutation,
            completion: completion.clone(),
        };
        let index = self.step.fetch_add(1, Ordering::AcqRel);
        if index == self.fail.load(Ordering::Acquire) {
            return Err(LimboError::InvalidArgument(
                "injected logical reclamation mutation failure".into(),
            ));
        }
        if index == self.delay.load(Ordering::Acquire) {
            assert!(self.pending.lock().replace(pending).is_none());
            return Ok(completion);
        }
        Self::perform(&pending)
    }
}

struct ControlledFile {
    inner: Arc<dyn File>,
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
        bytes: Arc<crate::Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        self.inner.pwrite(offset, bytes, completion)
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.control
            .mutation(self.inner.clone(), Mutation::Truncate(size), completion)
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        self.control
            .mutation(self.inner.clone(), Mutation::Sync(kind), completion)
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
        if Path::new(path).file_name().and_then(|name| name.to_str()) == Some("logical") {
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
    path: std::path::PathBuf,
    namespace: std::path::PathBuf,
    limit: u64,
    store: Arc<crate::MvStore>,
    connection: Arc<Connection>,
    runtime: Arc<SharedMvccRuntime>,
    verified: Option<VerifiedPhysicalBase>,
    control: Arc<Control>,
}

fn fixture() -> Fixture {
    let control = Arc::new(Control::default());
    let fixture = shared_completed_checkpoint_fixture(|_| {
        Arc::new(ControlledIo {
            inner: Arc::new(crate::io::PlatformIO::new().unwrap()),
            control: control.clone(),
        })
    });
    let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap().clone();
    let mut finalizer = fixture.ids.finalize(fixture.guard, fixture.proof).unwrap();
    let finalized = loop {
        match finalizer.poll().unwrap() {
            IOResult::IO(waiter) => fixture.io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(finalized) => break finalized,
        }
    };
    let published = PublishedSharedCheckpoint {
        finalized,
        pause: fixture.pause,
    };
    let mut reading = published
        .begin_physical_read(store.clone(), fixture.connection.clone())
        .unwrap();
    let verified = fixture.io.block(|| reading.poll()).unwrap();
    Fixture {
        _directory: fixture.directory,
        io: fixture.io,
        path: fixture.path,
        namespace: fixture.namespace,
        limit: fixture.limit,
        store,
        connection: fixture.connection,
        runtime,
        verified: Some(verified),
        control,
    }
}

type Installer = SharedCheckpointLocalInstall<crate::mvcc::MvccClock, crate::alloc::DynAllocator>;

fn installer(fixture: &mut Fixture) -> Installer {
    fixture
        .verified
        .take()
        .unwrap()
        .begin_local_install(fixture.store.clone(), fixture.connection.clone())
        .unwrap()
}

fn delayed(fixture: &Fixture, installing: &mut Installer, boundary: usize) {
    for _ in 0..100 {
        match installing.poll().unwrap() {
            IOResult::IO(waiter) if fixture.control.pending.lock().is_some() => {
                assert!(!waiter.0.finished());
                let pending = fixture.control.pending.lock();
                match pending.as_ref().unwrap().mutation {
                    Mutation::Truncate(size) => {
                        assert_eq!(boundary, 0);
                        assert_eq!(size, 0);
                    }
                    Mutation::Sync(kind) => {
                        assert_eq!(boundary, 1);
                        assert!(matches!(kind, FileSyncType::FullFsync));
                    }
                }
                return;
            }
            IOResult::IO(waiter) => fixture.io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(_) => panic!("local installation bypassed delayed logical mutation"),
        }
    }
    panic!("selected logical mutation was not submitted")
}

fn assert_fresh_payload(fixture: &Fixture) {
    let options = crate::OpenOptions::new(fixture.connection.db.dialect().clone())
        .with_shared_mvcc_for_test(fixture.limit);
    let database =
        crate::Database::open(fixture.io.clone(), fixture.path.to_str().unwrap(), options).unwrap();
    let reader = database.connect(crate::ConnectOpts::new()).unwrap();
    let mut values = Vec::new();
    reader
        .prepare("SELECT value FROM preserved WHERE id=1")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(values, ["materialized shared row"]);
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_install_native_mutation_failures_leave_old_maps_and_no_replay() {
    for boundary in [0, 1] {
        let mut fixture = fixture();
        let roots = fixture.store.table_id_to_rootpage.load_full();
        let schema = fixture.connection.db.schema.lock().clone();
        let main = std::fs::read(&fixture.path).unwrap();
        fixture.control.arm(boundary, usize::MAX);
        let mut installing = installer(&mut fixture);
        let result = fixture.io.block(|| installing.poll());
        assert!(
            matches!(result, Err(LimboError::InvalidArgument(message)) if message.contains("injected logical reclamation"))
        );
        assert!(
            fixture.runtime.replay.lock().is_none(),
            "failed local stage retained old replay readiness"
        );
        assert!(Arc::ptr_eq(
            &roots,
            &fixture.store.table_id_to_rootpage.load_full()
        ));
        assert!(Arc::ptr_eq(&schema, &fixture.connection.db.schema.lock()));
        assert_eq!(std::fs::read(&fixture.path).unwrap(), main);
        assert!(installing.poll().is_err());
        drop(installing);
        assert_fresh_payload(&fixture);
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_install_native_cancel_retains_logical_io_until_retirement() {
    for boundary in [0, 1] {
        for callback_first in [false, true] {
            let mut fixture = fixture();
            let roots = fixture.store.table_id_to_rootpage.load_full();
            fixture.control.arm(usize::MAX, boundary);
            let mut installing = installer(&mut fixture);
            delayed(&fixture, &mut installing, boundary);
            if callback_first {
                fixture.control.callback();
            }
            drop(installing);
            assert!(fixture.runtime.replay.lock().is_none());
            assert!(
                matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ),
                "cancelled logical mutation released authority before retirement"
            );
            if !callback_first {
                fixture.control.callback();
            }
            assert!(matches!(
                fixture.runtime.files.authority.try_acquire(),
                Err(LimboError::Busy)
            ));
            assert!(Arc::ptr_eq(
                &roots,
                &fixture.store.table_id_to_rootpage.load_full()
            ));
            fixture.control.pending.lock().take();
            assert!(fixture
                .runtime
                .files
                .authority
                .try_acquire()
                .unwrap()
                .requires_recovery());
            assert_fresh_payload(&fixture);
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_install_native_late_changes_reject_before_store_publication() {
    for change in [0, 1, 2] {
        let mut fixture = fixture();
        let roots = fixture.store.table_id_to_rootpage.load_full();
        let schema = fixture.connection.db.schema.lock().clone();
        fixture.control.arm(usize::MAX, 0);
        let mut installing = installer(&mut fixture);
        delayed(&fixture, &mut installing, 0);
        let mut dirty = None;
        match change {
            0 => fixture.connection.bump_prepare_context_generation(),
            1 => {
                fixture
                    .store
                    .schema_generation
                    .fetch_add(1, Ordering::AcqRel);
            }
            2 => {
                let (page, _) = fixture
                    .io
                    .block(|| fixture.connection.get_pager().read_page(1))
                    .unwrap();
                page.set_dirty();
                dirty = Some(page);
            }
            _ => unreachable!(),
        }
        fixture.control.callback();
        fixture.control.pending.lock().take();
        assert!(fixture.io.block(|| installing.poll()).is_err());
        if let Some(page) = dirty {
            page.clear_dirty();
        }
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(
            Arc::ptr_eq(&roots, &fixture.store.table_id_to_rootpage.load_full()),
            "failed reset or stale preparation changed the live store map"
        );
        assert!(Arc::ptr_eq(&schema, &fixture.connection.db.schema.lock()));
        drop(installing);
        assert_fresh_payload(&fixture);
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_install_native_reselect_rejects_a_new_intent_under_old_proof() {
    use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE, SLOT_SIZE};
    let mut fixture = fixture();
    let path = fixture.namespace.join("manifest");
    let original = std::fs::read(&path).unwrap();
    let record = ManifestRecord::decode(&original[..RECORD_SIZE]).unwrap();
    let intent = record
        .begin_checkpoint(record.prefix().committed_timestamp)
        .unwrap();
    let mut changed = original.clone();
    for offset in [0, SLOT_SIZE] {
        changed[offset..offset + RECORD_SIZE].copy_from_slice(&intent.encode());
    }
    std::fs::write(&path, changed).unwrap();
    let log = std::fs::read(fixture.namespace.join("logical")).unwrap();
    let roots = fixture.store.table_id_to_rootpage.load_full();
    let mut installing = installer(&mut fixture);
    assert!(matches!(
        fixture.io.block(|| installing.poll()),
        Err(LimboError::Corrupt(_))
    ));
    assert!(Arc::ptr_eq(
        &roots,
        &fixture.store.table_id_to_rootpage.load_full()
    ));
    assert_eq!(
        std::fs::read(fixture.namespace.join("logical")).unwrap(),
        log
    );
    std::fs::write(path, original).unwrap();
    drop(installing);
    assert_fresh_payload(&fixture);
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_install_native_waits_for_sync_backend_before_store_mutation() {
    let mut fixture = fixture();
    let roots = fixture.store.table_id_to_rootpage.load_full();
    fixture.control.arm(usize::MAX, 1);
    let mut installing = installer(&mut fixture);
    delayed(&fixture, &mut installing, 1);
    fixture.control.callback();
    let retirement = match installing.poll() {
        Ok(IOResult::IO(waiter)) => waiter.0,
        Err(error) => panic!("logical retirement was not awaited: {error}"),
        Ok(IOResult::Done(_)) => panic!("store metadata escaped before logical retirement"),
    };
    assert!(!retirement.finished());
    assert!(Arc::ptr_eq(
        &roots,
        &fixture.store.table_id_to_rootpage.load_full()
    ));
    assert!(fixture.runtime.replay.lock().is_none());
    fixture.control.pending.lock().take();
    assert!(retirement.finished());
    let installed = fixture.io.block(|| installing.poll()).unwrap();
    assert!(installed.guard.io_retired());
    assert!(!Arc::ptr_eq(
        &roots,
        &fixture.store.table_id_to_rootpage.load_full()
    ));
}
