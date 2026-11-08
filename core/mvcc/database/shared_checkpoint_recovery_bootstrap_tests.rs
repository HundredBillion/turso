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
    assert!(Arc::ptr_eq(&evidence.recovered.config, &config));
    let expected = runtime.storage();
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
fn checkpoint_recovery_bootstrap_native_missing_factory_withdraws_store() {
    let (_directory, _io, runtime, reclaimed) = bootstrap_fixture();
    let mut bootstrap = reclaimed.begin_bootstrap().unwrap();
    bootstrap.prepare_store().unwrap();
    let weak = Arc::downgrade(&bootstrap.evidence.as_ref().unwrap().store);
    let error = match bootstrap.poll() {
        Ok(_) => panic!("unimplemented recovery factory produced readiness"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("normal recovered database bootstrap is not implemented"));
    assert!(bootstrap.evidence.is_none());
    assert!(weak.upgrade().is_none());
    assert!(runtime.replay.lock().is_none());
    assert!(runtime.reserve_store().is_err());
    assert!(bootstrap.prepare_store().is_err());
}

#[derive(Default)]
struct ReleaseObserver {
    store: Mutex<Option<crate::sync::Weak<crate::MvStore>>>,
    releases: crate::sync::atomic::AtomicUsize,
    live_store_at_release: crate::sync::atomic::AtomicBool,
}
struct ReleaseFile {
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
        self.inner.shared_wal_unlock_byte(offset, kind)
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
        if std::path::Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            == Some("authority")
        {
            Ok(Arc::new(ReleaseFile {
                inner: file,
                observer: self.observer.clone(),
            }))
        } else {
            Ok(file)
        }
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
