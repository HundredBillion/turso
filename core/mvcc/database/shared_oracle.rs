use super::*;
use crate::mvcc::read_pinned_file::ReadPinnedFile;
use crate::mvcc::shared_authority::{AuthorityGuard, IoPin, PublicationLease, MAX_TIMESTAMP};
use crate::mvcc::shared_ids::io_protocol::RecoveryIdentity;
use crate::sync::Weak;

pub(in crate::mvcc) struct SharedOracleTransaction<C: LogicalClock, A: ConcurrentAllocator> {
    store: Arc<MvStore<C, A>>,
    pager: Arc<Pager>,
    tx_id: TxID,
    identity: RecoveryIdentity,
    lease: Weak<PublicationLease>,
    _pin: IoPin,
}

impl<C: LogicalClock, A: ConcurrentAllocator> SharedOracleTransaction<C, A> {
    pub(in crate::mvcc) fn store(&self) -> &Arc<MvStore<C, A>> {
        &self.store
    }
    pub(in crate::mvcc) fn pager(&self) -> &Arc<Pager> {
        &self.pager
    }
    pub(in crate::mvcc) fn tx_id(&self) -> TxID {
        self.tx_id
    }
    pub(in crate::mvcc) fn identity(&self) -> RecoveryIdentity {
        self.identity
    }
    pub(in crate::mvcc) fn matches_guard(&self, guard: &AuthorityGuard) -> bool {
        Weak::ptr_eq(&self.lease, &guard.io_lease())
            && guard
                .published()
                .is_ok_and(|prefix| prefix == self.identity.prefix)
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> Drop for SharedOracleTransaction<C, A> {
    fn drop(&mut self) {
        self.store.rollback_tx_inner(self.tx_id, None, 0);
        self.pager.end_read_tx();
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> MvStore<C, A> {
    pub(in crate::mvcc) fn begin_shared_oracle(
        self: &Arc<Self>,
        connection: &Connection,
        guard: &AuthorityGuard,
        identity: RecoveryIdentity,
    ) -> Result<SharedOracleTransaction<C, A>> {
        let runtime = self.shared_runtime.get().ok_or_else(|| {
            LimboError::InvalidArgument("oracle requires shared MVCC runtime".into())
        })?;
        runtime.validate_oracle_identity(guard, identity)?;
        if self.last_committed_tx_ts.load(Ordering::Acquire) != identity.prefix.committed_timestamp
        {
            return Err(LimboError::InvalidArgument(
                "oracle local committed frontier differs".into(),
            ));
        }
        if self
            .txs
            .iter()
            .any(|entry| matches!(entry.value().state.load(), TransactionState::Preparing(_)))
        {
            return Err(LimboError::Busy);
        }
        let timestamp = identity
            .prefix
            .committed_timestamp
            .checked_add(1)
            .filter(|timestamp| *timestamp < MAX_TIMESTAMP)
            .ok_or_else(|| LimboError::InvalidArgument("oracle timestamp exhausted".into()))?;
        let lease = guard.io_lease();
        let pin = PublicationLease::pin(&lease)?;
        let main = ReadPinnedFile::new(runtime.main_file(), guard)?;
        let wal = ReadPinnedFile::new(runtime.physical_wal_file(), guard)?;
        let pager =
            connection
                .db
                .shared_oracle_pager(&connection.get_pager(), runtime, main, wal)?;
        let checkpoint = self.try_acquire_checkpoint_read_guard_for_fresh_mvcc_begin()?;
        if let Err(error) = pager.begin_read_tx() {
            self.release_unadopted_checkpoint_read_guard(checkpoint);
            return Err(error);
        }
        let tx_id = match self.begin_tx_at_snapshot(
            pager.clone(),
            None,
            checkpoint,
            false,
            BeginTimestamp::SharedOracle(timestamp),
        ) {
            Ok(tx_id) => tx_id,
            Err(error) => {
                pager.end_read_tx();
                return Err(error);
            }
        };
        Ok(SharedOracleTransaction {
            store: self.clone(),
            pager,
            tx_id,
            identity,
            lease,
            _pin: pin,
        })
    }
}
