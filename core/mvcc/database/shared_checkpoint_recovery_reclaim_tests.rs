use super::super::shared_checkpoint_recovery_reclaim::{
    ReclaimedRecoveredCheckpoint, RecoveryLogReclamation,
};
use super::*;

struct LogTrace {
    transport: EpochTrace,
    runtime: Mutex<Option<crate::sync::Weak<SharedMvccRuntime>>>,
    enabled: AtomicBool,
}
impl Default for LogTrace {
    fn default() -> Self {
        Self {
            transport: EpochTrace::default(),
            runtime: Mutex::new(None),
            enabled: AtomicBool::new(false),
        }
    }
}
impl LogTrace {
    fn arm(
        &self,
        runtime: &Arc<SharedMvccRuntime>,
        fail: usize,
        callback_error: bool,
        delay: usize,
    ) {
        assert!(self.transport.pending.lock().is_none());
        *self.runtime.lock() = Some(Arc::downgrade(runtime));
        self.transport.step.store(0, Ordering::Release);
        self.transport.fail.store(fail, Ordering::Release);
        self.transport
            .callback_error
            .store(callback_error, Ordering::Release);
        self.transport.delay.store(delay, Ordering::Release);
        self.enabled.store(true, Ordering::Release);
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
            let runtime = self.runtime.lock().as_ref().unwrap().upgrade().unwrap();
            assert!(
                runtime.replay.lock().is_none(),
                "logical reclamation published replay readiness"
            );
            assert!(matches!(
                runtime.gate.try_enter_admission(),
                Err(LimboError::Busy)
            ));
            assert!(matches!(
                runtime.files.authority.try_acquire(),
                Err(LimboError::Busy)
            ));
            let step = self.transport.step.fetch_add(1, Ordering::AcqRel);
            assert!(
                matches!(
                    (&pending.mutation, step),
                    (Mutation::Truncate(0), 0) | (Mutation::Sync(FileSyncType::FullFsync), 1)
                ),
                "logical reclamation mutation order changed"
            );
            if step == self.transport.fail.load(Ordering::Acquire) {
                if self.transport.callback_error.load(Ordering::Acquire) {
                    completion.error(crate::CompletionError::IOError(
                        std::io::ErrorKind::Other,
                        "injected logical reclamation callback failure",
                    ));
                    return Ok(completion);
                }
                return Err(LimboError::InvalidArgument(
                    "injected logical reclamation submission failure".into(),
                ));
            }
            if step == self.transport.delay.load(Ordering::Acquire) {
                assert!(self.transport.pending.lock().replace(pending).is_none());
                return Ok(completion);
            }
        }
        self.transport.perform(&pending)
    }
}
struct LogFile {
    inner: Arc<dyn File>,
    path: PathBuf,
    trace: Arc<LogTrace>,
}
impl File for LogFile {
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
struct LogIo {
    inner: Arc<dyn crate::IO>,
    trace: Arc<LogTrace>,
}
impl crate::Clock for LogIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl crate::IO for LogIo {
    fn open_file(
        &self,
        path: &str,
        flags: crate::OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        if Path::new(path).file_name().and_then(|name| name.to_str()) == Some("logical") {
            Ok(Arc::new(LogFile {
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
fn log_fixture(
    fail: usize,
    callback_error: bool,
    delay: usize,
) -> (Fixture, Arc<LogTrace>, FinalizedRecoveredCheckpoint) {
    let trace = Arc::new(LogTrace::default());
    let wrapping = trace.clone();
    let mut fixture = fresh_fixture_with_io(false, move |inner| {
        Arc::new(LogIo {
            inner,
            trace: wrapping,
        })
    });
    let mut finalizing = fixture.start(usize::MAX, false, usize::MAX);
    let finalized = fixture.io.block(|| finalizing.poll()).unwrap();
    assert_finalized(&fixture, &finalized);
    let (header, offset, crc) = fixture.old_append.take().unwrap();
    assert!(header.is_some() && offset > 0);
    assert!(fixture.runtime.files.logical.size().unwrap() >= offset);
    fixture
        .runtime
        .storage
        .adopt_shared_prefix(
            header,
            offset,
            crc,
            fixture.target.database.checkpoint_timestamp,
        )
        .unwrap();
    trace.arm(&fixture.runtime, fail, callback_error, delay);
    (fixture, trace, finalized)
}
fn preserved(fixture: &Fixture) -> [Vec<u8>; 6] {
    [0, 1, 3, 4, 5, 6].map(|index| std::fs::read(&fixture.paths[index]).unwrap())
}
fn assert_reclaimed(fixture: &Fixture, trace: &LogTrace, reclaimed: &ReclaimedRecoveredCheckpoint) {
    assert_eq!(trace.transport.step.load(Ordering::Acquire), 2);
    assert_eq!(reclaimed.proof().target_identity(), fixture.target);
    assert_eq!(
        reclaimed.guard().published().unwrap(),
        fixture.target.prefix
    );
    assert!(reclaimed.guard().io_retired());
    assert_eq!(fixture.runtime.files.logical.size().unwrap(), 0);
    let logical: Arc<dyn File> = fixture.runtime.logical.clone();
    let snapshot = fixture
        .runtime
        .storage
        .shared_commit_snapshot(&logical)
        .unwrap();
    assert!(snapshot.offset == 0 && snapshot.running_crc == 0 && snapshot.header.is_none());
    assert!(fixture.runtime.replay.lock().is_none());
    assert!(fixture.runtime.reserve_store().is_err());
    assert!(matches!(
        fixture.runtime.gate.try_enter_admission(),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        fixture.runtime.files.authority.try_acquire(),
        Err(LimboError::Busy)
    ));
}
fn retry(fixture: &mut Fixture, trace: &LogTrace) -> ReclaimedRecoveredCheckpoint {
    trace.enabled.store(false, Ordering::Release);
    fixture.confirmed = Some(fixture.reconfirm());
    let mut finalizing = fixture.start(usize::MAX, false, usize::MAX);
    let finalized = fixture.io.block(|| finalizing.poll()).unwrap();
    trace.arm(&fixture.runtime, usize::MAX, false, usize::MAX);
    let mut reclaiming = finalized.begin_log_reclamation().unwrap();
    fixture.io.block(|| reclaiming.poll()).unwrap()
}
fn to_log_delay(
    fixture: &Fixture,
    trace: &LogTrace,
    state: &mut RecoveryLogReclamation,
    boundary: usize,
) {
    for _ in 0..64 {
        match state.poll().unwrap() {
            IOResult::IO(waiter) => {
                if trace.transport.pending.lock().is_some() {
                    assert_eq!(trace.transport.step.load(Ordering::Acquire), boundary + 1);
                    return;
                }
                fixture.io.wait_for_completion(waiter.0).unwrap();
            }
            IOResult::Done(_) => {
                panic!("logical reclamation returned before held boundary {boundary}")
            }
        }
    }
    panic!("logical reclamation never reached held boundary {boundary}");
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_reclaim_native_log_failures_retry() {
    for boundary in 0..2 {
        for callback_error in [false, true] {
            let (mut fixture, trace, finalized) = log_fixture(boundary, callback_error, usize::MAX);
            let before = preserved(&fixture);
            let mut reclaiming = finalized.begin_log_reclamation().unwrap();
            let error = match fixture.io.block(|| reclaiming.poll()) {
                Ok(_) => panic!("logical reclamation ignored injected failure {boundary}"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("injected logical reclamation"));
            assert!(fixture.runtime.replay.lock().is_none());
            assert_eq!(preserved(&fixture), before);
            drop(reclaiming);
            let reclaimed = retry(&mut fixture, &trace);
            assert_reclaimed(&fixture, &trace, &reclaimed);
            assert_eq!(preserved(&fixture), before);
        }
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_reclaim_native_log_cancellation_retains_authority() {
    for boundary in 0..2 {
        for after_callback in [false, true] {
            let (mut fixture, trace, finalized) = log_fixture(usize::MAX, false, boundary);
            let before = preserved(&fixture);
            let mut reclaiming = finalized.begin_log_reclamation().unwrap();
            to_log_delay(&fixture, &trace, &mut reclaiming, boundary);
            if after_callback {
                trace.transport.complete();
            }
            drop(reclaiming);
            assert!(fixture.runtime.replay.lock().is_none());
            assert!(
                matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ),
                "cancelled reclamation released backend authority at boundary {boundary}"
            );
            if !after_callback {
                trace.transport.complete();
            }
            assert!(
                matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ),
                "reclamation callback released physical authority at boundary {boundary}"
            );
            trace.transport.retire();
            let reclaimed = retry(&mut fixture, &trace);
            assert_reclaimed(&fixture, &trace, &reclaimed);
            assert_eq!(preserved(&fixture), before);
        }
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_reclaim_native_log_waits_for_physical_retirement() {
    for boundary in 0..2 {
        let (fixture, trace, finalized) = log_fixture(usize::MAX, false, boundary);
        let before = preserved(&fixture);
        let mut reclaiming = finalized.begin_log_reclamation().unwrap();
        to_log_delay(&fixture, &trace, &mut reclaiming, boundary);
        assert!(matches!(reclaiming.poll(), Ok(IOResult::IO(_))));
        trace.transport.complete();
        assert!(
            matches!(reclaiming.poll(), Ok(IOResult::IO(_))),
            "logical reclamation escaped before physical retirement {boundary}"
        );
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(fixture.runtime.reserve_store().is_err());
        trace.transport.retire();
        let reclaimed = fixture.io.block(|| reclaiming.poll()).unwrap();
        assert_reclaimed(&fixture, &trace, &reclaimed);
        assert_eq!(preserved(&fixture), before);
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_reclaim_native_log_rejects_changed_evidence() {
    for variant in 0..6 {
        let (fixture, trace, mut finalized) = log_fixture(usize::MAX, false, usize::MAX);
        let other = fresh_fixture(false);
        match variant {
            0 => finalized.runtime = other.runtime.clone(),
            1 => {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&fixture.paths[3])
                    .unwrap();
                file.write_all(&[1]).unwrap();
                file.sync_all().unwrap();
            }
            2 => {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&fixture.paths[0])
                    .unwrap();
                file.set_len(
                    file.metadata().unwrap().len()
                        + u64::from(finalized.parts.header.page_size.get()),
                )
                .unwrap();
                file.sync_all().unwrap();
            }
            3 => finalized.parts.persistent_timestamp += 1,
            4 => {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&fixture.paths[1])
                    .unwrap();
                file.write_all(&std::fs::read(&other.paths[1]).unwrap())
                    .unwrap();
                file.sync_all().unwrap();
            }
            5 => {
                let mut other = other;
                let mut finalizing = other.start(usize::MAX, false, usize::MAX);
                let foreign = other.io.block(|| finalizing.poll()).unwrap();
                finalized.finalized = foreign.finalized;
            }
            _ => unreachable!(),
        }
        let rejected = match finalized.begin_log_reclamation() {
            Err(_) => true,
            Ok(mut state) => fixture.io.block(|| state.poll()).is_err(),
        };
        assert!(
            rejected,
            "logical reclamation accepted changed evidence {variant}"
        );
        assert_eq!(
            trace.transport.step.load(Ordering::Acquire),
            0,
            "changed evidence mutated logical file {variant}"
        );
        assert!(fixture.runtime.replay.lock().is_none());
    }
}
