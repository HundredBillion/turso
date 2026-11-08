use super::super::super::tests::reclaim_tests::bootstrap_fixture;
use super::*;
use crate::alloc::{AllocError, ApiAllocator, DynAllocator, Global, Layout};
use crate::sync::atomic::Ordering;
use std::ptr::NonNull;

#[derive(Clone)]
struct RejectAllocation;
unsafe impl ApiAllocator for RejectAllocation {
    fn allocate(&self, _: Layout) -> std::result::Result<NonNull<[u8]>, AllocError> {
        Err(AllocError)
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(pointer, layout) }
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_retains_exactly_one_store() {
    let (_directory, _io, runtime, reclaimed) = bootstrap_fixture();
    let config = reclaimed.recovered.config.clone();
    let mut bootstrap = reclaimed.begin_bootstrap().unwrap();
    bootstrap.prepare_store().unwrap();
    let evidence = bootstrap.evidence.as_ref().unwrap();
    let store = evidence.store.clone();
    let weak = Arc::downgrade(&store);
    assert!(Arc::ptr_eq(store.shared_runtime.get().unwrap(), &runtime));
    assert!(Arc::ptr_eq(&evidence.config, &config));
    let expected: Arc<dyn crate::mvcc::persistent_storage::DurableStorage> = runtime.storage();
    assert!(Arc::ptr_eq(&store.storage, &expected));
    for _ in 0..8 {
        bootstrap.prepare_store().unwrap();
        assert!(Arc::ptr_eq(
            &bootstrap.evidence.as_ref().unwrap().store,
            &store
        ));
    }
    store.attach_shared_runtime(runtime.clone()).unwrap();
    assert!(store.txs.is_empty());
    assert!(runtime.replay.lock().is_none());
    assert!(runtime.reserve_store().is_err());
    assert!(matches!(
        runtime.gate.try_enter_admission(),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        runtime.files.authority.try_acquire(),
        Err(LimboError::Busy)
    ));
    drop(store);
    drop(bootstrap);
    assert!(
        weak.upgrade().is_none(),
        "cancelled bootstrap retained its Store"
    );
    assert!(runtime.replay.lock().is_none());
    assert!(
        runtime.reserve_store().is_err(),
        "cancelled attached Store reset its permanent claim"
    );
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_failed_allocation_releases_unused_claim() {
    let (_directory, _io, runtime, reclaimed) = bootstrap_fixture();
    let ReclaimedRecoveredCheckpoint {
        recovered,
        record: _,
        reservation,
    } = reclaimed;
    assert!(reservation
        .unwrap()
        .construct_store(DynAllocator::new(RejectAllocation))
        .is_err());
    let available = runtime.reserve_store().unwrap();
    available.validate(&runtime).unwrap();
    assert!(runtime.replay.lock().is_none());
    assert!(matches!(
        runtime.files.authority.try_acquire(),
        Err(LimboError::Busy)
    ));
    assert_eq!(
        recovered.guard().published().unwrap(),
        recovered.proof().target_identity().prefix
    );
    drop(available);
    drop(recovered);
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_ready_reopens_admission_and_commits() {
    let (_directory, io, runtime, reclaimed) = bootstrap_fixture();
    let target = reclaimed.proof().target_identity();
    let path = runtime.path.clone();
    let mut bootstrap = reclaimed.begin_bootstrap().unwrap();
    bootstrap.prepare_store().unwrap();
    let store = Arc::downgrade(&bootstrap.evidence.as_ref().unwrap().store);
    let ready = io.block(|| bootstrap.poll()).unwrap();
    let (database, connection) = ready.into_parts();
    assert!(bootstrap.evidence.is_none());
    assert!(store.upgrade().is_some());
    assert!(runtime.reserve_store().is_err());
    assert_eq!(
        runtime.replay.lock().as_ref().unwrap().identity().unwrap(),
        target
    );
    assert!(runtime.gate.try_enter_admission().is_ok());
    drop(runtime.files.authority.try_acquire().unwrap());
    let mut before = Vec::new();
    connection
        .prepare("SELECT value FROM preserved WHERE id=1")
        .unwrap()
        .run_with_row_callback(|row| {
            before.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(before, ["materialized shared row"]);
    connection.execute("UPDATE preserved SET value='normal recovery committed' WHERE id=1; INSERT INTO preserved VALUES(2,'new row')").unwrap();
    assert!(
        runtime
            .replay
            .lock()
            .as_ref()
            .unwrap()
            .identity()
            .unwrap()
            .prefix
            .end_offset
            > 0
    );
    let fresh = crate::Database::open(
        io,
        path.to_str().unwrap(),
        crate::OpenOptions::new(database.dialect().clone())
            .with_shared_mvcc_for_test(runtime.files.authority.configured_history_limit()),
    )
    .unwrap();
    let peer = fresh.connect(crate::ConnectOpts::new()).unwrap();
    let mut values = Vec::new();
    peer.prepare("SELECT value FROM preserved ORDER BY id")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(values, ["normal recovery committed", "new row"]);
}

struct HeldRead {
    file: Arc<dyn File>,
    offset: u64,
    completion: Completion,
}
impl HeldRead {
    fn complete(&self) {
        drop(
            self.file
                .pread(self.offset, self.completion.clone())
                .unwrap(),
        );
        assert!(self.completion.finished());
    }
}
#[derive(Default)]
struct ReadControl {
    target: Option<crate::io::FileId>,
    skip: usize,
    held: Option<HeldRead>,
    armed: bool,
}
#[derive(Default)]
struct ReleaseObserver {
    read: Mutex<ReadControl>,
    fail_finish: crate::sync::atomic::AtomicBool,
    store: Mutex<Option<crate::sync::Weak<crate::MvStore>>>,
    releases: crate::sync::atomic::AtomicUsize,
    live_store_at_release: crate::sync::atomic::AtomicBool,
}
struct ReleaseFile {
    authority: bool,
    inner: Arc<dyn File>,
    observer: Arc<ReleaseObserver>,
}
impl File for ReleaseFile {
    fn file_id(&self) -> Result<Option<crate::io::FileId>> {
        self.inner.file_id()
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, offset: u64, completion: Completion) -> Result<Completion> {
        let mut read = self.observer.read.lock();
        if read.armed && read.target == self.inner.file_id()? {
            if read.skip == 0 {
                assert!(read
                    .held
                    .replace(HeldRead {
                        file: self.inner.clone(),
                        offset,
                        completion: completion.clone(),
                    })
                    .is_none());
                read.armed = false;
                return Ok(completion);
            }
            read.skip -= 1;
        }
        drop(read);
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
    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: crate::io::SharedWalLockKind,
    ) -> Result<()> {
        self.inner.shared_wal_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: crate::io::SharedWalLockKind,
    ) -> Result<bool> {
        self.inner.shared_wal_try_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_probe_exclusive_byte(
        &self,
        offset: u64,
        kind: crate::io::SharedWalLockKind,
    ) -> Result<bool> {
        self.inner.shared_wal_probe_exclusive_byte(offset, kind)
    }
    fn shared_wal_probe_exclusive_while_shared_byte(
        &self,
        offset: u64,
        kind: crate::io::SharedWalLockKind,
    ) -> Result<bool> {
        self.inner
            .shared_wal_probe_exclusive_while_shared_byte(offset, kind)
    }
    fn shared_wal_unlock_byte(
        &self,
        offset: u64,
        kind: crate::io::SharedWalLockKind,
    ) -> Result<()> {
        if let Some(store) = self.observer.store.lock().as_ref() {
            self.observer.releases.fetch_add(1, Ordering::AcqRel);
            if store.upgrade().is_some() {
                self.observer
                    .live_store_at_release
                    .store(true, Ordering::Release);
            }
        }
        self.inner.shared_wal_unlock_byte(offset, kind)?;
        if self.authority && offset == 1 && self.observer.fail_finish.swap(false, Ordering::AcqRel)
        {
            return Err(LimboError::InvalidArgument(
                "injected recovered publication unlock failure".into(),
            ));
        }
        Ok(())
    }
    fn shared_wal_set_len(&self, size: u64) -> Result<()> {
        self.inner.shared_wal_set_len(size)
    }
    fn shared_wal_map(
        &self,
        offset: u64,
        length: usize,
    ) -> Result<Box<dyn crate::io::SharedWalMappedRegion>> {
        self.inner.shared_wal_map(offset, length)
    }
}
struct ReleaseIo {
    inner: Arc<dyn crate::IO>,
    observer: Arc<ReleaseObserver>,
}
impl crate::Clock for ReleaseIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl crate::IO for ReleaseIo {
    fn open_file(
        &self,
        path: &str,
        flags: crate::OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(ReleaseFile {
            authority: std::path::Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                == Some("authority"),
            inner: file,
            observer: self.observer.clone(),
        }))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn step(&self) -> Result<()> {
        self.inner.step()
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_last_arc_drops_store_before_authority() {
    use super::super::super::tests::reclaim_tests::bootstrap_fixture_with_io;
    let observer = Arc::new(ReleaseObserver::default());
    let wrapping = observer.clone();
    let (_directory, _io, runtime, reclaimed) = bootstrap_fixture_with_io(move |inner| {
        Arc::new(ReleaseIo {
            inner,
            observer: wrapping,
        })
    });
    let mut bootstrap = reclaimed.begin_bootstrap().unwrap();
    bootstrap.prepare_store().unwrap();
    let evidence = Arc::new(bootstrap.evidence.take().unwrap());
    let store = Arc::downgrade(&evidence.store);
    *observer.store.lock() = Some(store.clone());
    let last = evidence.clone();
    drop(bootstrap);
    drop(evidence);
    assert!(store.upgrade().is_some());
    assert_eq!(observer.releases.load(Ordering::Acquire), 0);
    assert!(matches!(
        runtime.files.authority.try_acquire(),
        Err(LimboError::Busy)
    ));
    drop(last);
    assert!(store.upgrade().is_none());
    assert!(observer.releases.load(Ordering::Acquire) > 0);
    assert!(
        !observer.live_store_at_release.load(Ordering::Acquire),
        "recovery evidence released authority before dropping its Store"
    );
    observer.store.lock().take();
    assert!(runtime.replay.lock().is_none());
    assert!(runtime.reserve_store().is_err());
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_factory_keeps_actual_store_private() {
    let (_directory, io, runtime, reclaimed) = bootstrap_fixture();
    let mut bootstrap = reclaimed.begin_bootstrap().unwrap();
    bootstrap.prepare_store().unwrap();
    let evidence = bootstrap.evidence.as_mut().unwrap();
    let store = Arc::downgrade(&evidence.store);
    let mut factory =
        crate::database::shared_recovered::RecoveredDatabaseFactory::new(evidence).unwrap();
    io.block(|| factory.poll(evidence)).unwrap();
    evidence.validate().unwrap();
    assert!(evidence.guard().io_retired());
    assert!(runtime.replay.lock().is_none());
    assert!(matches!(
        runtime.gate.try_enter_admission(),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        runtime.files.authority.try_acquire(),
        Err(LimboError::Busy)
    ));
    drop(factory);
    drop(bootstrap);
    assert!(store.upgrade().is_none());
    assert!(runtime.replay.lock().is_none());
    assert!(runtime.reserve_store().is_err());
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_factory_cancellation_retains_physical_reads() {
    use super::super::super::tests::reclaim_tests::bootstrap_fixture_with_io;
    for skip in 0..2 {
        for after_callback in [false, true] {
            let observer = Arc::new(ReleaseObserver::default());
            let wrapping = observer.clone();
            let (_directory, _io, runtime, reclaimed) = bootstrap_fixture_with_io(move |inner| {
                Arc::new(ReleaseIo {
                    inner,
                    observer: wrapping,
                })
            });
            let mut bootstrap = reclaimed.begin_bootstrap().unwrap();
            bootstrap.prepare_store().unwrap();
            let store = Arc::downgrade(&bootstrap.evidence.as_ref().unwrap().store);
            *observer.read.lock() = ReadControl {
                target: runtime.main_file().file_id().unwrap(),
                skip,
                held: None,
                armed: true,
            };
            let waiting = match bootstrap.poll().unwrap() {
                IOResult::IO(waiting) => waiting,
                IOResult::Done(_) => panic!("recovered connection escaped a delayed physical read"),
            };
            let held = observer
                .read
                .lock()
                .held
                .take()
                .expect("factory did not read retained main");
            if after_callback {
                held.complete();
            }
            drop(waiting);
            drop(bootstrap);
            assert!(store.upgrade().is_none());
            assert!(runtime.replay.lock().is_none());
            assert!(runtime.reserve_store().is_err());
            assert!(
                matches!(runtime.files.authority.try_acquire(), Err(LimboError::Busy)),
                "factory cancellation released authority before backend retirement"
            );
            if !after_callback {
                held.complete();
            }
            assert!(
                matches!(runtime.files.authority.try_acquire(), Err(LimboError::Busy)),
                "physical callback released authority before backend retirement"
            );
            drop(held);
            drop(runtime.files.authority.try_acquire().unwrap());
        }
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_factory_waits_for_physical_retirement() {
    use super::super::super::tests::reclaim_tests::bootstrap_fixture_with_io;
    for skip in 0..2 {
        let observer = Arc::new(ReleaseObserver::default());
        let wrapping = observer.clone();
        let (_directory, io, runtime, reclaimed) = bootstrap_fixture_with_io(move |inner| {
            Arc::new(ReleaseIo {
                inner,
                observer: wrapping,
            })
        });
        let mut bootstrap = reclaimed.begin_bootstrap().unwrap();
        bootstrap.prepare_store().unwrap();
        let evidence = bootstrap.evidence.as_mut().unwrap();
        let mut factory =
            crate::database::shared_recovered::RecoveredDatabaseFactory::new(evidence).unwrap();
        *observer.read.lock() = ReadControl {
            target: runtime.main_file().file_id().unwrap(),
            skip,
            held: None,
            armed: true,
        };
        assert!(matches!(factory.poll(evidence).unwrap(), IOResult::IO(_)));
        let held = observer.read.lock().held.take().unwrap();
        held.complete();
        let waiter = match factory.poll(evidence).unwrap() {
            IOResult::IO(waiter) => waiter,
            IOResult::Done(_) => panic!("factory completed before retained physical read retired"),
        };
        drop(waiter);
        assert!(!evidence.guard().io_retired());
        drop(held);
        io.block(|| factory.poll(evidence)).unwrap();
        assert!(evidence.guard().io_retired());
        assert!(runtime.replay.lock().is_none());
        drop(factory);
        drop(bootstrap);
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn checkpoint_recovery_bootstrap_native_late_failures_leave_no_ready_replay() {
    use super::super::super::tests::reclaim_tests::bootstrap_fixture_with_io;
    for boundary in 0..4 {
        let observer = Arc::new(ReleaseObserver::default());
        let wrapping = observer.clone();
        let (_directory, io, runtime, reclaimed) = bootstrap_fixture_with_io(move |inner| {
            Arc::new(ReleaseIo {
                inner,
                observer: wrapping,
            })
        });
        let mut bootstrap = reclaimed.begin_bootstrap().unwrap();
        bootstrap.prepare_store().unwrap();
        let evidence = bootstrap.evidence.as_mut().unwrap();
        let mut factory =
            crate::database::shared_recovered::RecoveredDatabaseFactory::new(evidence).unwrap();
        io.block(|| factory.poll(evidence)).unwrap();
        let (original, _) = factory.test_connections().unwrap();
        evidence.install(&original).unwrap();
        let (_, foreign) = factory.test_connections().unwrap();
        assert!(Arc::ptr_eq(&foreign.schema.read(), &original.schema.read()));
        let pager = original.get_pager();
        match boundary {
            0 => {
                evidence
                    .store
                    .last_committed_tx_ts
                    .fetch_add(1, Ordering::AcqRel);
            }
            1 => pager.begin_read_tx().unwrap(),
            2 => observer.fail_finish.store(true, Ordering::Release),
            3 => {}
            _ => unreachable!(),
        }
        let offered = if boundary == 3 {
            foreign.clone()
        } else {
            original.clone()
        };
        let error = match evidence.finish_ready(original.db.clone(), offered) {
            Ok(_) => panic!("recovered readiness ignored late fault {boundary}"),
            Err(error) => error,
        };
        match boundary {
            0 | 3 => assert!(
                error.to_string().contains("not ready for admission"),
                "{error}"
            ),
            1 => {
                assert!(matches!(error, LimboError::Busy), "{error}");
                pager.end_read_tx();
            }
            2 => assert!(
                error
                    .to_string()
                    .contains("injected recovered publication unlock failure"),
                "{error}"
            ),
            _ => unreachable!(),
        }
        assert!(runtime.replay.lock().is_none());
        drop(pager);
        drop(foreign);
        drop(original);
        drop(factory);
        drop(bootstrap);
        assert!(runtime.replay.lock().is_none());
        assert!(runtime.gate.try_enter_admission().is_ok());
        assert!(runtime.reserve_store().is_err());
    }
}
