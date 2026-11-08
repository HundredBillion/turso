use super::*;

#[derive(Default)]
pub(super) enum StoreClaimState {
    #[default]
    Free,
    Reserved,
    Attached,
}

#[allow(dead_code)]
pub(super) struct SharedStoreReservation {
    runtime: Option<Arc<SharedMvccRuntime>>,
}

impl SharedMvccRuntime {
    pub(super) fn attach_store<C: LogicalClock, A: ConcurrentAllocator>(
        self: &Arc<Self>,
        store: &MvStore<C, A>,
    ) -> Result<()> {
        let mut claim = self.store_claim.lock();
        // The identity check and attachment share this lock for concurrent idempotence.
        if let Some(existing) = store.shared_runtime.get() {
            return if Arc::ptr_eq(existing, self) {
                Ok(())
            } else {
                Err(LimboError::InvalidArgument(
                    "MVCC store already has another shared runtime".into(),
                ))
            };
        }
        if !matches!(*claim, StoreClaimState::Free) {
            return Err(LimboError::Busy);
        }
        store
            .shared_runtime
            .set(self.clone())
            .map_err(|_| LimboError::Busy)?;
        *claim = StoreClaimState::Attached;
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) fn reserve_store(self: &Arc<Self>) -> Result<SharedStoreReservation> {
        let mut claim = self.store_claim.lock();
        if !matches!(*claim, StoreClaimState::Free) {
            return Err(LimboError::Busy);
        }
        *claim = StoreClaimState::Reserved;
        Ok(SharedStoreReservation {
            runtime: Some(self.clone()),
        })
    }
}

#[allow(dead_code)]
impl SharedStoreReservation {
    pub(super) fn construct_store(
        mut self,
        allocator: crate::alloc::DynAllocator,
    ) -> Result<Arc<crate::MvStore>> {
        let runtime = self.runtime.as_ref().unwrap().clone();
        self.validate(&runtime)?;
        let store = Arc::new(MvStore::new_in(
            crate::mvcc::MvccClock::new(),
            runtime.storage(),
            allocator,
            false,
        )?);
        let mut claim = runtime.store_claim.lock();
        assert!(matches!(*claim, StoreClaimState::Reserved));
        assert!(store.shared_runtime.set(runtime.clone()).is_ok());
        *claim = StoreClaimState::Attached;
        self.runtime = None;
        Ok(store)
    }

    pub(super) fn validate(&self, runtime: &Arc<SharedMvccRuntime>) -> Result<()> {
        let owner = self.runtime.as_ref().unwrap();
        if !Arc::ptr_eq(runtime, owner)
            || !matches!(*owner.store_claim.lock(), StoreClaimState::Reserved)
        {
            return Err(LimboError::InvalidArgument(
                "fresh Store reservation changed owner".into(),
            ));
        }
        Ok(())
    }
}
impl Drop for SharedStoreReservation {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            let mut claim = runtime.store_claim.lock();
            assert!(matches!(*claim, StoreClaimState::Reserved));
            *claim = StoreClaimState::Free;
        }
    }
}
