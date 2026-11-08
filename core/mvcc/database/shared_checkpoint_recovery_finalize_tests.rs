use super::super::tests::{fault_fixture_with_seed, fault_fixture_with_seed_and_io};
use super::*;
use crate::io::FileId;
use crate::mvcc::shared_authority::{PublishedPrefix, SharedMvccAuthority};
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive(Clone)]
struct Expected {
    authority: crate::sync::Weak<SharedMvccAuthority>,
    runtime: crate::sync::Weak<SharedMvccRuntime>,
    maximum: Option<u64>,
    fresh: bool,
}
enum Mutation {
    Write(u64, Arc<crate::Buffer>),
    Sync(FileSyncType),
    Truncate(u64),
}
struct Pending {
    file: Arc<dyn File>,
    path: PathBuf,
    mutation: Mutation,
    completion: Completion,
}
struct EpochTrace {
    owner: Mutex<Option<Expected>>,
    enabled: AtomicBool,
    step: AtomicUsize,
    erasures: AtomicUsize,
    fail: AtomicUsize,
    callback_error: AtomicBool,
    delay: AtomicUsize,
    pending: Mutex<Option<Pending>>,
    durable: Mutex<BTreeMap<String, Vec<u8>>>,
}
impl Default for EpochTrace {
    fn default() -> Self {
        Self {
            owner: Mutex::new(None),
            enabled: AtomicBool::new(false),
            step: AtomicUsize::new(0),
            erasures: AtomicUsize::new(0),
            fail: AtomicUsize::new(usize::MAX),
            callback_error: AtomicBool::new(false),
            delay: AtomicUsize::new(usize::MAX),
            pending: Mutex::new(None),
            durable: Mutex::new(BTreeMap::new()),
        }
    }
}
impl EpochTrace {
    fn arm(
        &self,
        runtime: &Arc<SharedMvccRuntime>,
        maximum: Option<u64>,
        namespace: &Path,
        fail: usize,
        callback_error: bool,
        delay: usize,
    ) {
        assert!(self.pending.lock().is_none());
        let (_, ready, _) = runtime.files.authority.recovery_state_for_test().unwrap();
        *self.owner.lock() = Some(Expected {
            authority: Arc::downgrade(&runtime.files.authority),
            runtime: Arc::downgrade(runtime),
            maximum,
            fresh: !ready,
        });
        self.step.store(0, Ordering::Release);
        self.erasures.store(0, Ordering::Release);
        self.fail.store(fail, Ordering::Release);
        self.delay.store(delay, Ordering::Release);
        self.callback_error.store(callback_error, Ordering::Release);
        *self.durable.lock() = ["ids-a", "ids-b", "ids-selector"]
            .map(|name| {
                (
                    name.to_owned(),
                    std::fs::read(namespace.join(name)).unwrap(),
                )
            })
            .into_iter()
            .collect();
        self.enabled.store(true, Ordering::Release);
    }
    fn check_epoch(&self, expected: &Expected, message: &str) {
        let (epoch, ready, _) = expected
            .authority
            .upgrade()
            .unwrap()
            .recovery_state_for_test()
            .unwrap();
        let maximum = expected.maximum.unwrap_or(0);
        assert!(
            ready
                && if expected.fresh {
                    epoch > maximum
                } else {
                    epoch >= maximum
                },
            "{message}"
        );
    }
    fn perform(&self, pending: &Pending) -> Result<Completion> {
        let completion = match &pending.mutation {
            Mutation::Write(offset, bytes) => {
                pending
                    .file
                    .pwrite(*offset, bytes.clone(), pending.completion.clone())
            }
            Mutation::Sync(kind) => pending.file.sync(pending.completion.clone(), *kind),
            Mutation::Truncate(size) => pending.file.truncate(*size, pending.completion.clone()),
        }?;
        assert!(
            completion.finished(),
            "fixture requires synchronous native IO"
        );
        if self.enabled.load(Ordering::Acquire) && matches!(&pending.mutation, Mutation::Sync(_)) {
            self.durable.lock().insert(
                pending
                    .path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned(),
                std::fs::read(&pending.path).unwrap(),
            );
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
            let expected = self.owner.lock().as_ref().unwrap().clone();
            assert!(
                expected.runtime.upgrade().unwrap().replay.lock().is_none(),
                "ID mutation retained old runtime replay"
            );
            let index = self.step.fetch_add(1, Ordering::AcqRel);
            if expected.fresh && index < 4 {
                assert_eq!(pending.path.file_name().unwrap(), "ids-selector");
                assert!(matches!(
                    (&pending.mutation, index % 2),
                    (Mutation::Write(_, _), 0) | (Mutation::Sync(_), 1)
                ));
                assert!(
                    !expected
                        .authority
                        .upgrade()
                        .unwrap()
                        .recovery_state_for_test()
                        .unwrap()
                        .1,
                    "incarnation reconciled before current ID witness retired"
                );
            } else {
                self.check_epoch(
                    &expected,
                    "ID selector forgot retained owner epochs before reconciliation",
                );
            }
            if matches!(&pending.mutation, Mutation::Truncate(0)) {
                self.check_epoch(
                    &expected,
                    "old ID bank erased before retained owner epoch reconciliation",
                );
                self.erasures.fetch_add(1, Ordering::AcqRel);
            }
            if let Mutation::Sync(kind) = &pending.mutation {
                assert!(matches!(kind, FileSyncType::FullFsync));
            }
            if index == self.fail.load(Ordering::Acquire) {
                if self.callback_error.load(Ordering::Acquire) {
                    completion.error(crate::CompletionError::IOError(
                        std::io::ErrorKind::Other,
                        "injected recovered ID callback failure",
                    ));
                    return Ok(completion);
                }
                return Err(LimboError::InvalidArgument(
                    "injected recovered ID submission failure".into(),
                ));
            }
            if index == self.delay.load(Ordering::Acquire) {
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
    fn restore(&self, namespace: &Path) {
        assert!(self.pending.lock().is_none());
        self.enabled.store(false, Ordering::Release);
        for (name, bytes) in self.durable.lock().iter() {
            let path = namespace.join(name);
            std::fs::write(&path, bytes).unwrap();
            std::fs::File::open(path).unwrap().sync_all().unwrap();
        }
    }
}
struct EpochFile {
    inner: Arc<dyn File>,
    path: PathBuf,
    trace: Arc<EpochTrace>,
}
impl File for EpochFile {
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
        self.trace.mutation(
            self.inner.clone(),
            self.path.clone(),
            Mutation::Write(offset, bytes),
            completion,
        )
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        self.trace.mutation(
            self.inner.clone(),
            self.path.clone(),
            Mutation::Sync(kind),
            completion,
        )
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.trace.mutation(
            self.inner.clone(),
            self.path.clone(),
            Mutation::Truncate(size),
            completion,
        )
    }
}
struct EpochIo {
    inner: Arc<dyn crate::IO>,
    trace: Arc<EpochTrace>,
}
impl crate::Clock for EpochIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl crate::IO for EpochIo {
    fn open_file(
        &self,
        path: &str,
        flags: crate::OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        if matches!(
            Path::new(path).file_name().and_then(|name| name.to_str()),
            Some("ids-a" | "ids-b" | "ids-selector")
        ) {
            Ok(Arc::new(EpochFile {
                inner,
                path: path.into(),
                trace: self.trace.clone(),
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
type AppendImage = (
    Option<crate::mvcc::persistent_storage::logical_log::LogHeader>,
    u64,
    u32,
);
struct Fixture {
    io: Arc<dyn crate::IO>,
    runtime: Arc<SharedMvccRuntime>,
    namespace: PathBuf,
    trace: Arc<EpochTrace>,
    confirmed: Option<RecoveredCheckpointConfirmation>,
    paths: [PathBuf; 7],
    before: [Vec<u8>; 7],
    maximum: Option<u64>,
    target: RecoveryIdentity,
    initial_epoch: u64,
    initial_next: u64,
    old_append: Option<AppendImage>,
    options: crate::OpenOptions,
    _directory: tempfile::TempDir,
}
impl Fixture {
    fn start(
        &mut self,
        fail: usize,
        callback_error: bool,
        delay: usize,
    ) -> RecoveryIdFinalizationState {
        self.trace.arm(
            &self.runtime,
            self.confirmed.as_ref().unwrap().max_owner_epoch(),
            &self.namespace,
            fail,
            callback_error,
            delay,
        );
        self.confirmed
            .take()
            .unwrap()
            .begin_id_finalization()
            .unwrap()
    }
    fn assert_data_unchanged(&self) {
        for index in [0, 1, 2, 3] {
            assert_eq!(
                std::fs::read(&self.paths[index]).unwrap(),
                self.before[index]
            );
        }
    }
    fn reconfirm(&self) -> RecoveredCheckpointConfirmation {
        let mut opening = self.runtime.begin_checkpoint_recovery().unwrap();
        let selected = self.io.block(|| opening.poll()).unwrap();
        let mut physical = selected.begin_physical_read(self.options.clone()).unwrap();
        let physical = self.io.block(|| physical.poll()).unwrap();
        let mut loading = physical.begin_id_inspection().unwrap();
        let inspected = self.io.block(|| loading.poll()).unwrap();
        let mut confirming = inspected.begin_confirmation().unwrap();
        self.io.block(|| confirming.poll()).unwrap()
    }
}
fn fresh_fixture(target_only: bool) -> Fixture {
    fresh_fixture_with_io(target_only, |io| io)
}
fn fresh_fixture_with_io(
    target_only: bool,
    decorate: impl FnOnce(Arc<dyn crate::IO>) -> Arc<dyn crate::IO>,
) -> Fixture {
    fresh_fixture_with_access(target_only, false, decorate)
}
fn fresh_fixture_with_access(
    target_only: bool,
    readonly: bool,
    decorate: impl FnOnce(Arc<dyn crate::IO>) -> Arc<dyn crate::IO>,
) -> Fixture {
    fresh_fixture_with_options(
        target_only,
        readonly,
        decorate,
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
    )
}
fn fresh_fixture_with_options(
    target_only: bool,
    readonly: bool,
    decorate: impl FnOnce(Arc<dyn crate::IO>) -> Arc<dyn crate::IO>,
    options: crate::OpenOptions,
) -> Fixture {
    let (_directory, path, namespace, earlier, old_maximum, target, old_append) = {
        let mut old = fault_fixture_with_seed(target_only, true);
        let inspected = old.inspected.take().unwrap();
        let maximum = inspected.ids.max_owner_epoch();
        if !target_only {
            assert!(maximum.is_some(), "genuine owner epoch missing");
        }
        let target = inspected.ids.identity();
        drop(inspected);
        let old_append = {
            let log = old.runtime.storage.logical_log.read();
            (log.header().cloned(), log.offset, log.running_crc)
        };
        assert!(old_append.0.is_some() && old_append.1 > 0);
        (
            old._directory,
            old.path,
            old.namespace,
            old.earlier_coordination,
            maximum,
            target,
            old_append,
        )
    };
    let paths = [
        path.clone(),
        namespace.join("manifest"),
        namespace.join("logical"),
        namespace.join("physical-wal"),
        namespace.join("ids-a"),
        namespace.join("ids-b"),
        namespace.join("ids-selector"),
    ];
    let before = paths.each_ref().map(|path| std::fs::read(path).unwrap());
    let authority_path = namespace.join("authority");
    let old_metadata = std::fs::metadata(&authority_path).unwrap();
    assert_eq!(old_metadata.len(), earlier.len() as u64);
    let earlier_epoch = SharedMvccAuthority::recovery_image_epoch_for_test(&earlier).unwrap();
    let old_id = (old_metadata.dev(), old_metadata.ino());
    let mut restore = std::fs::OpenOptions::new()
        .write(true)
        .open(&authority_path)
        .unwrap();
    restore.write_all(&earlier).unwrap();
    restore.sync_all().unwrap();
    drop(restore);
    let metadata = std::fs::metadata(&authority_path).unwrap();
    assert_eq!((metadata.dev(), metadata.ino()), old_id);
    assert_eq!(
        paths.each_ref().map(|path| std::fs::read(path).unwrap()),
        before
    );
    let trace = Arc::new(EpochTrace::default());
    let io: Arc<dyn crate::IO> = decorate(Arc::new(EpochIo {
        inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
        trace: trace.clone(),
    }));
    let canonical = std::fs::canonicalize(&path).unwrap();
    let flags = if readonly {
        crate::OpenFlags::ReadOnly | crate::OpenFlags::NoLock
    } else {
        crate::OpenFlags::NoLock
    };
    let main = io
        .open_file(canonical.to_str().unwrap(), flags, false)
        .unwrap();
    let id = main.file_id().unwrap().unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    assert_eq!(
        id,
        FileId {
            dev: metadata.dev(),
            ino: metadata.ino()
        }
    );
    let runtime = SharedMvccRuntime::open_existing(
        io.clone(),
        main,
        id,
        &canonical,
        64 * 1024 * 1024,
        readonly,
    )
    .unwrap();
    let (epoch, ready, next_timestamp) = runtime.files.authority.recovery_state_for_test().unwrap();
    assert_eq!(
        epoch,
        earlier_epoch + 1,
        "fresh open did not reacquire exclusive lifetime ownership"
    );
    assert!(!ready, "fixture retained an old authority incarnation");
    if let Some(maximum) = old_maximum {
        assert!(
            epoch <= maximum,
            "fixture does not require retained owner reconciliation"
        );
    }
    let mut opening = runtime.begin_checkpoint_recovery().unwrap();
    let selected = io.block(|| opening.poll()).unwrap();
    let mut physical = selected.begin_physical_read(options.clone()).unwrap();
    let physical = io.block(|| physical.poll()).unwrap();
    let mut loading = physical.begin_id_inspection().unwrap();
    let inspected = io.block(|| loading.poll()).unwrap();
    assert_eq!(inspected.ids.max_owner_epoch(), old_maximum);
    let mut confirming = inspected.begin_confirmation().unwrap();
    let confirmed = io.block(|| confirming.poll()).unwrap();
    assert_eq!(confirmed.proof().source_identity(), None);
    assert_eq!(confirmed.proof().target_identity(), target);
    assert_eq!(confirmed.max_owner_epoch(), old_maximum);
    assert!(confirmed.guard().requires_recovery());
    let before_finalizing = paths.each_ref().map(|path| std::fs::read(path).unwrap());
    for index in [0, 2, 3, 4, 5, 6] {
        assert_eq!(before_finalizing[index], before[index]);
    }
    Fixture {
        io,
        runtime,
        namespace,
        trace,
        confirmed: Some(confirmed),
        paths,
        before: before_finalizing,
        maximum: old_maximum,
        target,
        initial_epoch: epoch,
        initial_next: next_timestamp,
        old_append: Some(old_append),
        options,
        _directory,
    }
}

fn ready_fixture(target_only: bool) -> Fixture {
    let trace = Arc::new(EpochTrace::default());
    let wrapped_trace = trace.clone();
    let mut old = fault_fixture_with_seed_and_io(target_only, true, move |inner| {
        let io: Arc<dyn crate::IO> = Arc::new(EpochIo {
            inner,
            trace: wrapped_trace,
        });
        io
    });
    let io = old.io.clone();
    let runtime = old.runtime.clone();
    let (initial_epoch, ready, initial_next) =
        runtime.files.authority.recovery_state_for_test().unwrap();
    assert!(ready);
    assert!(
        runtime.replay.lock().is_some(),
        "fixture lost genuine old replay context"
    );
    let inspected = old.inspected.take().unwrap();
    let maximum = inspected.ids.max_owner_epoch();
    let target = inspected.ids.identity();
    let mut confirming = inspected.begin_confirmation().unwrap();
    let confirmed = io.block(|| confirming.poll()).unwrap();
    let paths = [
        old.path.clone(),
        old.namespace.join("manifest"),
        old.namespace.join("logical"),
        old.namespace.join("physical-wal"),
        old.namespace.join("ids-a"),
        old.namespace.join("ids-b"),
        old.namespace.join("ids-selector"),
    ];
    let before = paths.each_ref().map(|path| std::fs::read(path).unwrap());
    Fixture {
        io,
        runtime,
        namespace: old.namespace,
        trace,
        confirmed: Some(confirmed),
        paths,
        before,
        maximum,
        target,
        initial_epoch,
        initial_next,
        old_append: None,
        options: crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
        _directory: old._directory,
    }
}
fn assert_finalized(fixture: &Fixture, finalized: &FinalizedRecoveredCheckpoint) {
    let (epoch, ready, next) = fixture
        .runtime
        .files
        .authority
        .recovery_state_for_test()
        .unwrap();
    let expected = fixture.trace.owner.lock().as_ref().unwrap().clone();
    assert_eq!(fixture.trace.erasures.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture.trace.step.load(Ordering::Acquire),
        if expected.fresh { 10 } else { 6 }
    );
    assert!(ready);
    if expected.fresh {
        assert!(epoch > fixture.maximum.unwrap_or(0));
    } else {
        assert_eq!(epoch, fixture.initial_epoch);
    }
    assert!(next >= fixture.initial_next && next > fixture.target.prefix.committed_timestamp);
    assert!(fixture.runtime.replay.lock().is_none());
    assert_eq!(
        finalized.guard().published().unwrap(),
        fixture.target.prefix
    );
    assert_eq!(finalized.proof().source_identity(), None);
    assert!(finalized.proof().matches_lease(finalized.guard()));
    assert!(matches!(
        fixture.runtime.gate.try_enter_admission(),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        fixture.runtime.files.authority.try_acquire(),
        Err(LimboError::Busy)
    ));
    fixture.assert_data_unchanged();
}
fn to_delay(fixture: &Fixture, state: &mut RecoveryIdFinalizationState, boundary: usize) {
    for _ in 0..100 {
        match state.poll().unwrap() {
            IOResult::IO(waiter) => {
                if fixture.trace.pending.lock().is_some() {
                    drop(waiter);
                    assert_eq!(fixture.trace.step.load(Ordering::Acquire), boundary + 1);
                    return;
                }
                fixture.io.wait_for_completion(waiter.0).unwrap();
            }
            IOResult::Done(_) => panic!("recovered finalization bypassed mutation {boundary}"),
        }
    }
    panic!("recovered finalization never submitted mutation {boundary}")
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_finalize_native_fresh_incarnation_preserves_epoch_without_admission() {
    let mut fixture = fresh_fixture(false);
    let mut state = fixture.start(usize::MAX, false, usize::MAX);
    let finalized = fixture.io.block(|| state.poll()).unwrap();
    assert_finalized(&fixture, &finalized);
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_finalize_native_ready_and_finalized_targets_stay_unadmitted() {
    for (fresh, target_only) in [(false, false), (false, true), (true, true)] {
        eprintln!("recovered ID state fresh={fresh} finalized={target_only}");
        let mut fixture = if fresh {
            fresh_fixture(target_only)
        } else {
            ready_fixture(target_only)
        };
        if target_only {
            assert_eq!(
                fixture
                    .confirmed
                    .as_ref()
                    .unwrap()
                    .proof()
                    .source_database(),
                None
            );
        }
        let mut state = fixture.start(usize::MAX, false, usize::MAX);
        let finalized = fixture.io.block(|| state.poll()).unwrap();
        assert_finalized(&fixture, &finalized);
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_finalize_native_rejects_live_and_foreign_proof_and_ids() {
    let live =
        shared_completed_checkpoint_fixture(|_| Arc::new(crate::io::SyscallIO::new().unwrap()));
    let mut guard = live.guard;
    assert!(live.proof.source_identity().is_some());
    let prefix = guard.published().unwrap();
    assert!(guard.recover_completed_checkpoint(&live.proof).is_err());
    assert_eq!(guard.published().unwrap(), prefix);
    drop(guard);
    drop(live.ids);
    drop(live.proof);
    drop(live.pause);
    for foreign_ids in [false, true] {
        let mut left = fresh_fixture(false);
        let mut right = fresh_fixture(false);
        if foreign_ids {
            std::mem::swap(
                &mut left.confirmed.as_mut().unwrap().ids,
                &mut right.confirmed.as_mut().unwrap().ids,
            );
        } else {
            let before = left
                .runtime
                .files
                .authority
                .recovery_state_for_test()
                .unwrap();
            let own = left.confirmed.as_mut().unwrap();
            let foreign = right.confirmed.as_ref().unwrap();
            assert!(own
                .guard
                .recover_completed_checkpoint(&foreign.proof)
                .is_err());
            assert!(own.guard.requires_recovery());
            assert_eq!(
                left.runtime
                    .files
                    .authority
                    .recovery_state_for_test()
                    .unwrap(),
                before
            );
            std::mem::swap(
                &mut left.confirmed.as_mut().unwrap().proof,
                &mut right.confirmed.as_mut().unwrap().proof,
            );
        }
        left.trace.arm(
            &left.runtime,
            left.maximum,
            &left.namespace,
            usize::MAX,
            false,
            usize::MAX,
        );
        let error = match left.confirmed.take().unwrap().begin_id_finalization() {
            Err(error) => error,
            Ok(_) => panic!("foreign recovery capability authorized ID finalization"),
        };
        assert!(matches!(error, LimboError::InvalidArgument(_)), "{error}");
        assert_eq!(left.trace.step.load(Ordering::Acquire), 0);
        assert_eq!(
            left.paths
                .each_ref()
                .map(|path| std::fs::read(path).unwrap()),
            left.before
        );
        assert_eq!(
            right
                .paths
                .each_ref()
                .map(|path| std::fs::read(path).unwrap()),
            right.before
        );
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_finalize_native_never_rewinds_and_preserves_timestamp_clock() {
    {
        let mut fixture = fresh_fixture(false);
        let confirmed = fixture.confirmed.as_mut().unwrap();
        confirmed
            .guard
            .recover_completed_checkpoint(&confirmed.proof)
            .unwrap();
        for _ in 0..5 {
            confirmed.guard.allocate_timestamp().unwrap();
        }
        let before = fixture
            .runtime
            .files
            .authority
            .recovery_state_for_test()
            .unwrap();
        confirmed
            .guard
            .recover_completed_checkpoint(&confirmed.proof)
            .unwrap();
        assert_eq!(
            fixture
                .runtime
                .files
                .authority
                .recovery_state_for_test()
                .unwrap(),
            before
        );
        assert_eq!(confirmed.guard.published().unwrap(), fixture.target.prefix);
    }
    let mut fixture = fresh_fixture(false);
    let different = PublishedPrefix {
        committed_timestamp: fixture.target.prefix.committed_timestamp + 7,
        end_offset: 0,
        running_crc: 0,
    };
    let confirmed = fixture.confirmed.as_mut().unwrap();
    confirmed.guard.recover(different).unwrap();
    let before = fixture
        .runtime
        .files
        .authority
        .recovery_state_for_test()
        .unwrap();
    assert!(confirmed
        .guard
        .recover_completed_checkpoint(&confirmed.proof)
        .is_err());
    assert_eq!(confirmed.guard.published().unwrap(), different);
    assert_eq!(
        fixture
            .runtime
            .files
            .authority
            .recovery_state_for_test()
            .unwrap(),
        before
    );
    fixture.trace.arm(
        &fixture.runtime,
        fixture.maximum,
        &fixture.namespace,
        usize::MAX,
        false,
        usize::MAX,
    );
    assert!(fixture
        .confirmed
        .take()
        .unwrap()
        .begin_id_finalization()
        .is_err());
    assert_eq!(fixture.trace.step.load(Ordering::Acquire), 0);
    assert!(fixture.runtime.replay.lock().is_none());
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_finalize_native_failures_restore_durable_ids_and_retry() {
    for callback_error in [false, true] {
        for boundary in 0..10 {
            eprintln!("recovered ID failure {boundary}/{callback_error}");
            let mut fixture = fresh_fixture(false);
            let mut state = fixture.start(boundary, callback_error, usize::MAX);
            let error = match fixture.io.block(|| state.poll()) {
                Err(error) => error,
                Ok(_) => {
                    panic!("recovered IDs escaped failed mutation {boundary}/{callback_error}")
                }
            };
            assert!(
                error.to_string().contains("injected recovered ID"),
                "{error}"
            );
            drop(state);
            assert!(fixture.runtime.replay.lock().is_none());
            fixture.assert_data_unchanged();
            let guard = fixture.runtime.files.authority.try_acquire().unwrap();
            assert!(guard.requires_recovery());
            fixture.trace.restore(&fixture.namespace);
            drop(guard);
            let confirmed = fixture.reconfirm();
            fixture.trace.arm(
                &fixture.runtime,
                confirmed.max_owner_epoch(),
                &fixture.namespace,
                usize::MAX,
                false,
                usize::MAX,
            );
            let mut retry = confirmed.begin_id_finalization().unwrap();
            let finalized = fixture.io.block(|| retry.poll()).unwrap();
            assert_eq!(
                finalized.guard().published().unwrap(),
                fixture.target.prefix
            );
            assert!(finalized.proof().matches_lease(finalized.guard()));
            assert!(fixture.runtime.replay.lock().is_none());
            assert!(matches!(
                fixture.runtime.gate.try_enter_admission(),
                Err(LimboError::Busy)
            ));
            fixture.assert_data_unchanged();
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_finalize_native_cancellation_retains_each_physical_mutation() {
    for after_callback in [false, true] {
        for boundary in 0..10 {
            eprintln!("recovered ID cancellation {boundary}/{after_callback}");
            let mut fixture = fresh_fixture(false);
            let mut state = fixture.start(usize::MAX, false, boundary);
            to_delay(&fixture, &mut state, boundary);
            if after_callback {
                fixture.trace.complete();
            }
            drop(state);
            assert!(fixture.runtime.replay.lock().is_none());
            assert!(
                matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ),
                "cancelled ID mutation released authority {boundary}/{after_callback}"
            );
            if !after_callback {
                fixture.trace.complete();
            }
            assert!(
                matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ),
                "ID callback released physical authority {boundary}/{after_callback}"
            );
            fixture.trace.retire();
            let guard = fixture.runtime.files.authority.try_acquire().unwrap();
            assert!(guard.requires_recovery());
            fixture.assert_data_unchanged();
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_finalize_native_witness_and_final_bank_wait_for_physical_retirement() {
    for boundary in [3, 9] {
        let mut fixture = fresh_fixture(false);
        let mut state = fixture.start(usize::MAX, false, boundary);
        to_delay(&fixture, &mut state, boundary);
        fixture.trace.complete();
        let waiter = match state.poll().unwrap() {
            IOResult::IO(waiter) => waiter,
            IOResult::Done(_) => {
                panic!("recovered ID finalization escaped before physical retirement {boundary}")
            }
        };
        assert!(!waiter.0.finished());
        let (epoch, ready, _) = fixture
            .runtime
            .files
            .authority
            .recovery_state_for_test()
            .unwrap();
        if boundary == 3 {
            assert!(
                !ready && epoch <= fixture.maximum.unwrap(),
                "epoch changed before ID witness retired"
            );
            assert_eq!(fixture.trace.erasures.load(Ordering::Acquire), 0);
        } else {
            assert!(ready && epoch > fixture.maximum.unwrap());
        }
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(matches!(
            fixture.runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        fixture.trace.retire();
        fixture.io.wait_for_completion(waiter.0).unwrap();
        let finalized = fixture.io.block(|| state.poll()).unwrap();
        assert_finalized(&fixture, &finalized);
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_reclaim_native_fresh_log_without_admission() {
    for target_only in [false, true] {
        let mut fixture = fresh_fixture(target_only);
        let mut finalizing = fixture.start(usize::MAX, false, usize::MAX);
        let finalized = fixture.io.block(|| finalizing.poll()).unwrap();
        assert_finalized(&fixture, &finalized);
        assert!(
            !std::fs::read(fixture.namespace.join("logical"))
                .unwrap()
                .is_empty(),
            "fixture must retain real obsolete logical records"
        );
        let preserved =
            [0, 1, 3, 4, 5, 6].map(|index| std::fs::read(&fixture.paths[index]).unwrap());
        let mut reclaiming = finalized.begin_log_reclamation().unwrap();
        let reclaimed = fixture.io.block(|| reclaiming.poll()).unwrap();
        assert_eq!(reclaimed.proof().source_identity(), None);
        assert_eq!(reclaimed.proof().target_identity(), fixture.target);
        assert_eq!(
            reclaimed.guard().published().unwrap(),
            fixture.target.prefix
        );
        assert!(fixture.runtime.reserve_store().is_err());
        assert!(std::fs::read(fixture.namespace.join("logical"))
            .unwrap()
            .is_empty());
        assert_eq!(
            [0, 1, 3, 4, 5, 6].map(|index| std::fs::read(&fixture.paths[index]).unwrap()),
            preserved
        );
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(matches!(
            fixture.runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        assert!(matches!(
            fixture.runtime.files.authority.try_acquire(),
            Err(LimboError::Busy)
        ));
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_reclaim_native_rejects_second_store() {
    let fixture = fresh_fixture(false);
    let first = Arc::new(
        MvStore::new(
            crate::mvcc::clock::MvccClock::new(),
            fixture.runtime.storage(),
            false,
        )
        .unwrap(),
    );
    let barrier = Arc::new(std::sync::Barrier::new(8));
    std::thread::scope(|scope| {
        let mut joins = Vec::new();
        for _ in 0..8 {
            let first = first.clone();
            let runtime = fixture.runtime.clone();
            let barrier = barrier.clone();
            joins.push(scope.spawn(move || {
                barrier.wait();
                first.attach_shared_runtime(runtime)
            }));
        }
        for join in joins {
            join.join().unwrap().unwrap();
        }
    });
    let second = MvStore::new(
        crate::mvcc::clock::MvccClock::new(),
        fixture.runtime.storage(),
        false,
    )
    .unwrap();
    assert!(
        second
            .attach_shared_runtime(fixture.runtime.clone())
            .is_err(),
        "a second Store attached to an already claimed runtime"
    );
    drop(first);
    assert!(
        second.attach_shared_runtime(fixture.runtime).is_err(),
        "dropping the first Store reopened its runtime claim"
    );
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_reclaim_native_finalized_suspension() {
    let mut fixture = fresh_fixture(false);
    let mut finalizing = fixture.start(usize::MAX, false, usize::MAX);
    let finalized = fixture.io.block(|| finalizing.poll()).unwrap();
    let (ids, guard) = finalized.finalized.suspend();
    assert_eq!(ids.proof().target_identity(), fixture.target);
    let ids = ids.rejoin(guard).unwrap();
    assert_eq!(ids.guard().published().unwrap(), fixture.target.prefix);
    assert!(matches!(
        fixture.runtime.files.authority.try_acquire(),
        Err(LimboError::Busy)
    ));
    let (ids, original_guard) = ids.suspend();
    let foreign = fresh_fixture(false);
    let guard = foreign.confirmed.unwrap().guard;
    assert!(ids.rejoin(guard).is_err());
    assert_eq!(original_guard.published().unwrap(), fixture.target.prefix);
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_reclaim_native_reservation_is_linear() {
    let fixture = fresh_fixture(false);
    let store = MvStore::new(
        crate::mvcc::clock::MvccClock::new(),
        fixture.runtime.storage(),
        false,
    )
    .unwrap();
    let reservation = fixture.runtime.reserve_store().unwrap();
    reservation.validate(&fixture.runtime).unwrap();
    assert!(fixture.runtime.reserve_store().is_err());
    assert!(matches!(
        store.attach_shared_runtime(fixture.runtime.clone()),
        Err(LimboError::Busy)
    ));
    drop(reservation);
    let mismatch: Arc<dyn crate::mvcc::persistent_storage::DurableStorage> = Arc::new(
        Storage::new(fixture.runtime.logical.clone(), fixture.io.clone(), None),
    );
    let wrong = MvStore::new(crate::mvcc::clock::MvccClock::new(), mismatch, false).unwrap();
    assert!(wrong
        .attach_shared_runtime(fixture.runtime.clone())
        .is_err());
    let reservation = fixture.runtime.reserve_store().unwrap();
    let other = fresh_fixture(false);
    assert!(reservation.validate(&other.runtime).is_err());
    drop(reservation);
    store
        .attach_shared_runtime(fixture.runtime.clone())
        .unwrap();
    assert!(fixture.runtime.reserve_store().is_err());
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_reclaim_native_rejects_preexisting_store_before_mutation() {
    let mut fixture = ready_fixture(false);
    let mut finalizing = fixture.start(usize::MAX, false, usize::MAX);
    let finalized = fixture.io.block(|| finalizing.poll()).unwrap();
    assert!(fixture.runtime.replay.lock().is_none());
    let before = std::fs::read(fixture.namespace.join("logical")).unwrap();
    assert!(!before.is_empty());
    assert!(
        matches!(finalized.begin_log_reclamation(), Err(LimboError::Busy)),
        "reclamation accepted a runtime with a preexisting Store"
    );
    assert_eq!(
        std::fs::read(fixture.namespace.join("logical")).unwrap(),
        before
    );
    assert!(fixture.runtime.replay.lock().is_none());
    assert!(fixture.runtime.reserve_store().is_err());
}

#[path = "shared_checkpoint_recovery_reclaim_tests.rs"]
pub(super) mod reclaim_tests;

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_readonly_requires_writable_recovery() {
    let mut fixture = fresh_fixture_with_access(false, true, |io| io);
    assert!(fixture.runtime.original_readonly);
    let before = [0, 2, 3].map(|index| std::fs::read(&fixture.paths[index]).unwrap());
    let mut finalizing = fixture.start(usize::MAX, false, usize::MAX);
    let finalized = fixture.io.block(|| finalizing.poll()).unwrap();
    let error = match finalized.begin_log_reclamation() {
        Ok(_) => panic!("content-readonly runtime began log reclamation"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("requires a writable participant"));
    assert_eq!(
        [0, 2, 3].map(|index| std::fs::read(&fixture.paths[index]).unwrap()),
        before
    );
    assert!(fixture.runtime.replay.lock().is_none());
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_original_optin_survives() {
    let options = crate::OpenOptions::new(Arc::new(crate::EngineDialect))
        .with_shared_mvcc_for_test(64 * 1024 * 1024);
    let mut fixture = fresh_fixture_with_options(false, false, |io| io, options);
    let original = fixture.confirmed.as_ref().unwrap().config.clone();
    let mut finalizing = fixture.start(usize::MAX, false, usize::MAX);
    let finalized = fixture.io.block(|| finalizing.poll()).unwrap();
    let mut reclaiming = finalized.begin_log_reclamation().unwrap();
    let reclaimed = fixture.io.block(|| reclaiming.poll()).unwrap();
    assert!(Arc::ptr_eq(reclaimed.config_for_test(), &original));
    let mut bootstrap = reclaimed.begin_bootstrap().unwrap();
    let error = match fixture.io.block(|| bootstrap.poll()) {
        Ok(_) => panic!("missing factory produced readiness"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("normal recovered database bootstrap is not implemented"));
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_rejects_changed_history() {
    let mut fixture = fresh_fixture(false);
    let before = [0, 2, 3].map(|index| std::fs::read(&fixture.paths[index]).unwrap());
    fixture.confirmed.take();
    let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
    let selected = fixture.io.block(|| opening.poll()).unwrap();
    let error = match selected.begin_physical_read(
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(1),
    ) {
        Ok(_) => panic!("physical recovery accepted a mismatched original history limit"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("validated shared history budget"));
    assert_eq!(
        [0, 2, 3].map(|index| std::fs::read(&fixture.paths[index]).unwrap()),
        before
    );
}
