#[path = "shared_checkpoint_recovery_reclaim.rs"]
mod shared_checkpoint_recovery_reclaim;
pub(crate) use shared_checkpoint_recovery_reclaim::{
    RecoveredDatabaseReady, RecoveryBootstrapEvidence, RecoveryConstructionPermit,
};

use super::*;
use crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::finalize::FinalizedIds;

#[allow(dead_code)]
pub(in crate::mvcc) struct FinalizedRecoveredCheckpoint {
    runtime: Arc<SharedMvccRuntime>,
    config: Arc<RecoveryOpenConfig>,
    parts: PhysicalBaseParts,
    finalized: FinalizedIds,
    pause: CheckpointPause,
}
#[allow(dead_code)]
impl FinalizedRecoveredCheckpoint {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.finalized.guard()
    }
    pub(in crate::mvcc) fn proof(&self) -> &CompletedCheckpointProof {
        self.finalized.proof()
    }
}

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryIdFinalizationState {
    runtime: Arc<SharedMvccRuntime>,
    config: Arc<RecoveryOpenConfig>,
    parts: Option<PhysicalBaseParts>,
    pause: Option<CheckpointPause>,
    finalizer:
        Option<crate::mvcc::shared_ids::io_protocol::checkpoint_recovery::RecoveredIdFinalization>,
    finished: bool,
}
#[allow(dead_code)]
impl RecoveredCheckpointConfirmation {
    pub(in crate::mvcc) fn begin_id_finalization(mut self) -> Result<RecoveryIdFinalizationState> {
        let target = self.proof.target_identity();
        let bytes = self.runtime.files.main.size()?;
        let page_size = u64::from(self.parts.header.page_size.get());
        let floor = i64::try_from(bytes / page_size)
            .ok()
            .and_then(i64::checked_neg)
            .filter(|floor| *floor < 0 && *floor != i64::MIN)
            .ok_or(LimboError::DatabaseFull)?;
        if !self.runtime.owns_authority(&self.guard)
            || !self.proof.matches_lease(&self.guard)
            || self.proof.source_identity().is_some()
            || self.runtime.files.main.file_id()? != Some(self.runtime.database_id)
            || self.runtime.files.physical_wal.size()? != 0
            || bytes % page_size != 0
            || bytes / page_size < u64::from(self.parts.header.database_size.get())
            || self.parts.persistent_timestamp != target.database.checkpoint_timestamp
            || self.proof.materialized_floor() != floor
            || target.prefix.end_offset != 0
            || target.prefix.running_crc != 0
            || target.database.checkpoint_timestamp != target.prefix.committed_timestamp
        {
            return Err(LimboError::InvalidArgument(
                "recovered ID finalization owner or physical target changed".into(),
            ));
        }
        self.guard.ensure_no_snapshot_locks()?;
        if !self.guard.io_retired() {
            return Err(LimboError::Busy);
        }
        self.runtime.replay.lock().take();
        self.guard.recover_completed_checkpoint(&self.proof)?;
        let finalizer = self
            .ids
            .begin_finalize(self.guard, self.proof, self.max_owner_epoch)?;
        Ok(RecoveryIdFinalizationState {
            runtime: self.runtime,
            config: self.config,
            parts: Some(self.parts),
            pause: Some(self.pause),
            finalizer: Some(finalizer),
            finished: false,
        })
    }
}
#[allow(dead_code)]
impl RecoveryIdFinalizationState {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<FinalizedRecoveredCheckpoint> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn poll_inner(&mut self) -> IOResultOr<FinalizedRecoveredCheckpoint> {
        if self.finished {
            return Err(
                LimboError::InvalidArgument("recovered ID finalization has ended".into()).into(),
            );
        }
        let finalized = return_if_io!(self.finalizer.as_mut().unwrap().poll());
        self.finalizer = None;
        self.finished = true;
        Ok(IOResult::Done(FinalizedRecoveredCheckpoint {
            runtime: self.runtime.clone(),
            config: self.config.clone(),
            parts: self.parts.take().unwrap(),
            finalized,
            pause: self.pause.take().unwrap(),
        }))
    }
    fn abort(&mut self) {
        self.finalizer = None;
        self.parts = None;
        self.pause = None;
        self.finished = true;
    }
}
impl Drop for RecoveryIdFinalizationState {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_recovery_finalize_tests.rs"]
mod tests;
