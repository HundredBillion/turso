use super::*;
use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE, SLOT_SIZE};

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_confirmation_native_discards_old_proof_and_retains_unrecovered_owner() {
    let fixture =
        shared_completed_checkpoint_fixture(|_| Arc::new(crate::io::PlatformIO::new().unwrap()));
    let target = fixture.proof.target_identity();
    let source = fixture.proof.source_database();
    let runtime = fixture
        .database
        .get_mv_store()
        .as_ref()
        .unwrap()
        .shared_runtime
        .get()
        .unwrap()
        .clone();
    let io = fixture.io.clone();
    drop(fixture.ids);
    drop(fixture.guard);
    drop(fixture.proof);
    drop(fixture.pause);
    drop(fixture.connection);
    drop(fixture.database);
    let files = [
        fixture.path.clone(),
        fixture.namespace.join("logical"),
        fixture.namespace.join("physical-wal"),
        fixture.namespace.join("ids-a"),
        fixture.namespace.join("ids-b"),
        fixture.namespace.join("ids-selector"),
    ];
    let before = files.each_ref().map(|path| std::fs::read(path).unwrap());
    let mut opening = runtime.begin_checkpoint_recovery().unwrap();
    let selected = io.block(|| opening.poll()).unwrap();
    let record = selected.manifest.record();
    let mut physical = selected
        .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
        .unwrap();
    let physical = io.block(|| physical.poll()).unwrap();
    let mut loading = physical.begin_id_inspection().unwrap();
    let inspected = io.block(|| loading.poll()).unwrap();
    let maximum = inspected.ids.max_owner_epoch();
    let pages =
        runtime.files.main.size().unwrap() / u64::from(inspected.parts.header.page_size.get());
    let mut confirming = inspected.begin_confirmation().unwrap();
    let confirmed = io.block(|| confirming.poll()).unwrap();
    assert_eq!(confirmed.proof().target_identity(), target);
    assert_eq!(confirmed.proof().source_identity(), None);
    assert_eq!(confirmed.proof().source_database(), source);
    assert_eq!(confirmed.proof().materialized_floor(), -(pages as i64));
    assert!(confirmed.proof().matches_lease(confirmed.guard()));
    assert_eq!(confirmed.max_owner_epoch(), maximum);
    assert!(confirmed.guard().requires_recovery());
    assert!(confirmed.guard().published().is_err());
    assert!(matches!(
        runtime.gate.try_enter_admission(),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        runtime.files.authority.try_acquire(),
        Err(LimboError::Busy)
    ));
    let manifest = std::fs::read(fixture.namespace.join("manifest")).unwrap();
    for slot in 0..2 {
        assert_eq!(
            ManifestRecord::decode(&manifest[slot * SLOT_SIZE..slot * SLOT_SIZE + RECORD_SIZE])
                .unwrap(),
            Some(record)
        );
    }
    assert_eq!(
        files.each_ref().map(|path| std::fs::read(path).unwrap()),
        before
    );
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_confirmation_native_rejects_same_record_from_foreign_file() {
    let fixture =
        shared_completed_checkpoint_fixture(|_| Arc::new(crate::io::PlatformIO::new().unwrap()));
    let runtime = fixture
        .database
        .get_mv_store()
        .as_ref()
        .unwrap()
        .shared_runtime
        .get()
        .unwrap()
        .clone();
    let io = fixture.io.clone();
    drop(fixture.ids);
    drop(fixture.guard);
    drop(fixture.proof);
    drop(fixture.pause);
    drop(fixture.connection);
    drop(fixture.database);
    let original = std::fs::read(fixture.namespace.join("manifest")).unwrap();
    let mut opening = runtime.begin_checkpoint_recovery().unwrap();
    let selected = io.block(|| opening.poll()).unwrap();
    let mut physical = selected
        .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
        .unwrap();
    let physical = io.block(|| physical.poll()).unwrap();
    let mut loading = physical.begin_id_inspection().unwrap();
    let inspected = io.block(|| loading.poll()).unwrap();
    let (_retained, evidence, guard) = suspend_inspected(inspected).unwrap();
    let fake_io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
    let fake = fake_io
        .open_file("substitute-manifest", crate::OpenFlags::Create, false)
        .unwrap();
    fake_io
        .wait_for_completion(
            fake.pwrite(
                0,
                Arc::new(crate::Buffer::new(original.clone())),
                Completion::new_write(|_| {}),
            )
            .unwrap(),
        )
        .unwrap();
    let mut load = ManifestLoad::new(fake, guard, runtime.database_id).unwrap();
    let loaded = fake_io.block(|| Ok(load.poll()?)).unwrap();
    let error = match loaded.mirror_recovered_completion(evidence) {
        Err(error) => error,
        Ok(_) => panic!("substitute manifest file authorized recovered completion"),
    };
    assert!(
        error.to_string().contains("changed owner, files or target"),
        "{error}"
    );
    assert_eq!(
        std::fs::read(fixture.namespace.join("manifest")).unwrap(),
        original
    );
    assert!(matches!(
        runtime.gate.try_enter_admission(),
        Err(LimboError::Busy)
    ));
    let guard = runtime.files.authority.try_acquire().unwrap();
    assert!(guard.requires_recovery());
    assert!(guard.published().is_err());
}

use crate::io::{FileId, OpenFlags};
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::path::{Path, PathBuf};

enum Mutation {
    Write(u64, Arc<crate::Buffer>),
    Sync(FileSyncType),
}
struct Pending {
    file: Arc<dyn File>,
    path: PathBuf,
    mutation: Mutation,
    completion: Completion,
}
struct Control {
    enabled: AtomicBool,
    step: AtomicUsize,
    fail: AtomicUsize,
    callback_error: AtomicBool,
    delay: AtomicUsize,
    pending: Mutex<Option<Pending>>,
    durable: Mutex<Vec<u8>>,
    offsets: Mutex<Vec<u64>>,
}
impl Default for Control {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            step: AtomicUsize::new(0),
            fail: AtomicUsize::new(usize::MAX),
            callback_error: AtomicBool::new(false),
            delay: AtomicUsize::new(usize::MAX),
            pending: Mutex::new(None),
            durable: Mutex::new(Vec::new()),
            offsets: Mutex::new(Vec::new()),
        }
    }
}
impl Control {
    fn arm(&self, manifest: &Path, fail: usize, callback_error: bool, delay: usize) {
        assert!(self.pending.lock().is_none());
        *self.durable.lock() = std::fs::read(manifest).unwrap();
        self.offsets.lock().clear();
        self.step.store(0, Ordering::Release);
        self.fail.store(fail, Ordering::Release);
        self.callback_error.store(callback_error, Ordering::Release);
        self.delay.store(delay, Ordering::Release);
        self.enabled.store(true, Ordering::Release);
    }
    fn perform(&self, pending: &Pending) -> Result<Completion> {
        let completion = match &pending.mutation {
            Mutation::Write(offset, bytes) => {
                pending
                    .file
                    .pwrite(*offset, bytes.clone(), pending.completion.clone())
            }
            Mutation::Sync(kind) => pending.file.sync(pending.completion.clone(), *kind),
        }?;
        assert!(
            completion.finished(),
            "fixture requires synchronous native IO"
        );
        if matches!(pending.mutation, Mutation::Sync(_)) && self.enabled.load(Ordering::Acquire) {
            *self.durable.lock() = std::fs::read(&pending.path).unwrap();
        }
        Ok(completion)
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
        if self.enabled.load(Ordering::Acquire) {
            let step = self.step.fetch_add(1, Ordering::AcqRel);
            if let Mutation::Sync(kind) = &pending.mutation {
                assert!(matches!(kind, FileSyncType::FullFsync));
            }
            if let Mutation::Write(offset, _) = &pending.mutation {
                self.offsets.lock().push(*offset);
            }
            if step == self.fail.load(Ordering::Acquire) {
                if self.callback_error.load(Ordering::Acquire) {
                    completion.error(crate::CompletionError::IOError(
                        std::io::ErrorKind::Other,
                        "injected confirmation callback failure",
                    ));
                    return Ok(completion);
                }
                return Err(LimboError::InvalidArgument(
                    "injected confirmation submission failure".into(),
                ));
            }
            if step == self.delay.load(Ordering::Acquire) {
                assert!(self.pending.lock().replace(pending).is_none());
                return Ok(completion);
            }
        }
        self.perform(&pending)
    }
    fn complete(&self) {
        drop(self.perform(self.pending.lock().as_ref().unwrap()).unwrap());
    }
    fn retire(&self) {
        self.pending.lock().take();
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
        bytes: Arc<crate::Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        self.control.mutation(
            self.inner.clone(),
            self.path.clone(),
            Mutation::Write(offset, bytes),
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
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.inner.truncate(size, completion)
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
        if Path::new(path).file_name().and_then(|name| name.to_str()) == Some("manifest") {
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
pub(super) struct FaultFixture {
    pub(super) _directory: tempfile::TempDir,
    io: Arc<dyn crate::IO>,
    runtime: Arc<SharedMvccRuntime>,
    pub(super) path: PathBuf,
    pub(super) namespace: PathBuf,
    control: Arc<Control>,
    pub(super) inspected: Option<InspectedRecoveryIds>,
    pub(super) earlier_coordination: Vec<u8>,
    record: ManifestRecord,
}
fn inspect(fixture: &FaultFixture) -> InspectedRecoveryIds {
    let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
    let selected = fixture.io.block(|| Ok(opening.poll()?)).unwrap();
    let mut physical = selected
        .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
        .unwrap();
    let physical = fixture.io.block(|| Ok(physical.poll()?)).unwrap();
    let mut loading = physical.begin_id_inspection().unwrap();
    fixture.io.block(|| Ok(loading.poll()?)).unwrap()
}
fn fault_fixture(target_only: bool) -> FaultFixture {
    fault_fixture_with_seed(target_only, false)
}
pub(super) fn fault_fixture_with_seed(target_only: bool, seed_owner: bool) -> FaultFixture {
    let control = Arc::new(Control::default());
    let earlier = Arc::new(Mutex::new(Vec::new()));
    let fixture = shared_prepared_checkpoint_fixture(|path| {
        if seed_owner {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(path).unwrap();
            let id = FileId {
                dev: metadata.dev(),
                ino: metadata.ino(),
            };
            let namespace = crate::mvcc::mode_transition::namespace_for(path, id).unwrap();
            *earlier.lock() = std::fs::read(namespace.join("authority")).unwrap();
        }
        let io: Arc<dyn crate::IO> = Arc::new(ControlledIo {
            inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
            control: control.clone(),
        });
        if seed_owner {
            let database = crate::Database::open(
                io.clone(),
                path.to_str().unwrap(),
                crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                    .with_shared_mvcc_for_test(64 * 1024 * 1024),
            )
            .unwrap();
            let connection = database.connect(crate::ConnectOpts::new()).unwrap();
            connection
                .execute("BEGIN; SELECT value FROM preserved")
                .unwrap();
            let tx_id = connection.get_mv_tx_id_for_db(0).unwrap();
            let store = database.get_mv_store().as_ref().unwrap().clone();
            let mut state = crate::mvcc::database::SharedIdState::default();
            for _ in 0..32 {
                io.block(|| {
                    Ok(store.shared_id_nonblock(
                        &connection,
                        connection.get_pager(),
                        tx_id,
                        0,
                        crate::mvcc::database::SharedIdRequest::ReserveLogicalTable,
                        &mut state,
                    )?)
                })
                .unwrap();
            }
            state.reset();
            connection.execute("ROLLBACK").unwrap();
        }
        io
    });
    let runtime = fixture
        .database
        .get_mv_store()
        .as_ref()
        .unwrap()
        .shared_runtime
        .get()
        .unwrap()
        .clone();
    let manifest_path = fixture.namespace.join("manifest");
    let genuine_intent = std::fs::read(&manifest_path).unwrap();
    let mut publication = fixture
        .token
        .begin_completion(fixture.materialized, fixture.prepared)
        .unwrap();
    let completed = fixture.io.block(|| Ok(publication.poll()?)).unwrap();
    let (proof, mut guard, ids) = completed.into_parts();
    guard.publish_checkpoint(&proof).unwrap();
    let (guard, proof) = if target_only {
        let mut finalizer = ids.finalize(guard, proof).unwrap();
        let finalized = fixture.io.block(|| Ok(finalizer.poll()?)).unwrap();
        let (ids, proof) = finalized.into_parts();
        (ids.into_guard(), proof)
    } else {
        drop(ids);
        (guard, proof)
    };
    let mut manifest = std::fs::read(&manifest_path).unwrap();
    manifest[..SLOT_SIZE].copy_from_slice(&genuine_intent[..SLOT_SIZE]);
    std::fs::write(&manifest_path, manifest).unwrap();
    std::fs::File::open(&manifest_path)
        .unwrap()
        .sync_all()
        .unwrap();
    drop(proof);
    drop(guard);
    drop(fixture.pause);
    drop(fixture.connection);
    drop(fixture.database);
    let mut fixture = FaultFixture {
        _directory: fixture.directory,
        io: fixture.io,
        runtime,
        path: fixture.path,
        namespace: fixture.namespace,
        control,
        inspected: None,
        earlier_coordination: earlier.lock().clone(),
        record: ManifestRecord::decode(
            &std::fs::read(&manifest_path).unwrap()[SLOT_SIZE..SLOT_SIZE + RECORD_SIZE],
        )
        .unwrap()
        .unwrap(),
    };
    let inspected = inspect(&fixture);
    assert_eq!(inspected.record, fixture.record);
    fixture.inspected = Some(inspected);
    fixture
}
fn unchanged_files(fixture: &FaultFixture) -> Vec<Vec<u8>> {
    [
        fixture.path.clone(),
        fixture.namespace.join("logical"),
        fixture.namespace.join("physical-wal"),
        fixture.namespace.join("ids-a"),
        fixture.namespace.join("ids-b"),
        fixture.namespace.join("ids-selector"),
    ]
    .map(|path| std::fs::read(path).unwrap())
    .to_vec()
}
fn selected(bytes: &[u8], database_id: FileId) -> ManifestRecord {
    crate::mvcc::commit_manifest::select_latest(
        [
            ManifestRecord::decode(&bytes[..RECORD_SIZE]).unwrap(),
            ManifestRecord::decode(&bytes[SLOT_SIZE..SLOT_SIZE + RECORD_SIZE]).unwrap(),
        ],
        database_id,
    )
    .unwrap()
    .1
}
fn to_delay(
    fixture: &FaultFixture,
    state: &mut RecoveryCheckpointConfirmationState,
    boundary: usize,
) {
    for _ in 0..100 {
        match state.poll().unwrap() {
            IOResult::IO(waiter) => {
                if fixture.control.pending.lock().is_some() {
                    drop(waiter);
                    assert_eq!(fixture.control.step.load(Ordering::Acquire), boundary + 1);
                    return;
                }
                fixture.io.wait_for_completion(waiter.0).unwrap();
            }
            IOResult::Done(_) => panic!("confirmation bypassed selected mutation {boundary}"),
        }
    }
    panic!("confirmation did not reach selected mutation {boundary}")
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_confirmation_native_failures_preserve_selected_durable_normal_and_retry() {
    for callback_error in [false, true] {
        for boundary in 0..4 {
            let mut fixture = fault_fixture(false);
            let before = unchanged_files(&fixture);
            fixture.control.arm(
                &fixture.namespace.join("manifest"),
                boundary,
                callback_error,
                usize::MAX,
            );
            let mut state = fixture
                .inspected
                .take()
                .unwrap()
                .begin_confirmation()
                .unwrap();
            let error = match fixture.io.block(|| Ok(state.poll()?)) {
                Err(error) => error,
                Ok(_) => panic!("confirmation escaped failed mutation {boundary}/{callback_error}"),
            };
            assert!(
                error.to_string().contains("injected confirmation"),
                "{error}"
            );
            drop(state);
            assert_eq!(fixture.control.step.load(Ordering::Acquire), boundary + 1);
            assert_eq!(
                selected(&fixture.control.durable.lock(), fixture.runtime.database_id),
                fixture.record
            );
            assert_eq!(unchanged_files(&fixture), before);
            fixture.control.enabled.store(false, Ordering::Release);
            let guard = fixture.runtime.files.authority.try_acquire().unwrap();
            assert!(guard.requires_recovery());
            assert!(guard.published().is_err());
            std::fs::write(
                fixture.namespace.join("manifest"),
                fixture.control.durable.lock().clone(),
            )
            .unwrap();
            drop(guard);
            let inspected = inspect(&fixture);
            let mut retry = inspected.begin_confirmation().unwrap();
            let confirmed = fixture.io.block(|| Ok(retry.poll()?)).unwrap();
            assert!(confirmed.guard().requires_recovery());
            assert!(confirmed.proof().matches_lease(confirmed.guard()));
            assert!(matches!(
                fixture.runtime.gate.try_enter_admission(),
                Err(LimboError::Busy)
            ));
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_confirmation_native_cancel_keeps_backend_authority_for_each_mutation() {
    for after_callback in [false, true] {
        for boundary in 0..4 {
            let mut fixture = fault_fixture(false);
            let before = unchanged_files(&fixture);
            fixture.control.arm(
                &fixture.namespace.join("manifest"),
                usize::MAX,
                false,
                boundary,
            );
            let mut state = fixture
                .inspected
                .take()
                .unwrap()
                .begin_confirmation()
                .unwrap();
            to_delay(&fixture, &mut state, boundary);
            if after_callback {
                fixture.control.complete();
            }
            drop(state);
            assert!(
                matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ),
                "cancelled confirmation released physical mutation {boundary}/{after_callback}"
            );
            if !after_callback {
                fixture.control.complete();
            }
            assert!(
                matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ),
                "callback released confirmation mutation {boundary}/{after_callback}"
            );
            fixture.control.retire();
            let guard = fixture.runtime.files.authority.try_acquire().unwrap();
            assert!(guard.requires_recovery());
            assert!(guard.published().is_err());
            assert_eq!(unchanged_files(&fixture), before);
            assert!(fixture.runtime.gate.try_enter_admission().is_ok());
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_confirmation_native_waits_final_sync_backend_before_proof() {
    let mut fixture = fault_fixture(false);
    let before = unchanged_files(&fixture);
    fixture
        .control
        .arm(&fixture.namespace.join("manifest"), usize::MAX, false, 3);
    let mut state = fixture
        .inspected
        .take()
        .unwrap()
        .begin_confirmation()
        .unwrap();
    to_delay(&fixture, &mut state, 3);
    assert_eq!(*fixture.control.offsets.lock(), [0, SLOT_SIZE as u64]);
    fixture.control.complete();
    let waiter = match state.poll().unwrap() {
        IOResult::IO(waiter) => waiter,
        IOResult::Done(_) => panic!("confirmation proof escaped before final backend retirement"),
    };
    assert!(!waiter.0.finished());
    assert!(matches!(
        fixture.runtime.gate.try_enter_admission(),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        fixture.runtime.files.authority.try_acquire(),
        Err(LimboError::Busy)
    ));
    fixture.control.retire();
    fixture.io.wait_for_completion(waiter.0).unwrap();
    let confirmed = fixture.io.block(|| Ok(state.poll()?)).unwrap();
    assert!(confirmed.proof().matches_lease(confirmed.guard()));
    assert!(confirmed.guard().requires_recovery());
    assert!(confirmed.guard().published().is_err());
    assert_eq!(unchanged_files(&fixture), before);
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_confirmation_native_reselects_exact_record_before_writes() {
    let mut fixture = fault_fixture(false);
    let before = unchanged_files(&fixture);
    let newer = ManifestRecord::new(
        fixture.record.sequence() + 1,
        fixture.record.database_id(),
        fixture.record.log_generation(),
        fixture.record.log_salt(),
        fixture.record.prefix(),
    )
    .unwrap();
    let path = fixture.namespace.join("manifest");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[..RECORD_SIZE].copy_from_slice(&newer.encode());
    std::fs::write(&path, bytes).unwrap();
    fixture.control.arm(&path, usize::MAX, false, usize::MAX);
    let mut state = fixture
        .inspected
        .take()
        .unwrap()
        .begin_confirmation()
        .unwrap();
    let error = match fixture.io.block(|| Ok(state.poll()?)) {
        Err(error) => error,
        Ok(_) => panic!("stale inspection confirmed a newly selected manifest"),
    };
    assert!(
        error.to_string().contains("changed owner, files or target"),
        "{error}"
    );
    assert_eq!(fixture.control.step.load(Ordering::Acquire), 0);
    assert_eq!(unchanged_files(&fixture), before);
    fixture.control.enabled.store(false, Ordering::Release);
    let inspected = inspect(&fixture);
    assert_eq!(inspected.record, newer);
    let mut retry = inspected.begin_confirmation().unwrap();
    let confirmed = fixture.io.block(|| Ok(retry.poll()?)).unwrap();
    assert!(confirmed.guard().requires_recovery());
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_confirmation_native_finalized_selector_does_not_infer_source() {
    let mut fixture = fault_fixture(true);
    let before = unchanged_files(&fixture);
    let inspected = fixture.inspected.take().unwrap();
    assert_eq!(inspected.ids.source_database(), None);
    let maximum = inspected.ids.max_owner_epoch();
    fixture.control.arm(
        &fixture.namespace.join("manifest"),
        usize::MAX,
        false,
        usize::MAX,
    );
    let mut state = inspected.begin_confirmation().unwrap();
    let confirmed = fixture.io.block(|| Ok(state.poll()?)).unwrap();
    assert_eq!(confirmed.proof().source_identity(), None);
    assert_eq!(confirmed.proof().source_database(), None);
    assert_eq!(confirmed.max_owner_epoch(), maximum);
    assert_eq!(fixture.control.step.load(Ordering::Acquire), 4);
    assert_eq!(*fixture.control.offsets.lock(), [0, SLOT_SIZE as u64]);
    assert_eq!(unchanged_files(&fixture), before);
    assert!(confirmed.guard().requires_recovery());
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_confirmation_native_preserves_owner_epoch_and_distinct_physical_floor() {
    let mut fixture = fault_fixture_with_seed(false, true);
    let before = unchanged_files(&fixture);
    let inspected = fixture.inspected.take().unwrap();
    let maximum = inspected.ids.max_owner_epoch();
    assert!(
        maximum.is_some(),
        "fixture did not retain a genuine journal owner epoch"
    );
    let physical = -(i64::try_from(
        fixture.runtime.files.main.size().unwrap()
            / u64::from(inspected.parts.header.page_size.get()),
    )
    .unwrap());
    assert!(
        inspected.ids.logical_floor() < physical,
        "fixture did not distinguish permanent ID floor from physical extent"
    );
    let mut state = inspected.begin_confirmation().unwrap();
    let confirmed = fixture.io.block(|| Ok(state.poll()?)).unwrap();
    assert_eq!(
        confirmed.max_owner_epoch(),
        maximum,
        "confirmation lost inspected owner epoch"
    );
    assert_eq!(
        confirmed.proof().materialized_floor(),
        physical,
        "confirmation used permanent ID floor as physical evidence"
    );
    assert_eq!(confirmed.proof().source_identity(), None);
    assert!(confirmed.guard().requires_recovery());
    assert_eq!(unchanged_files(&fixture), before);
}
