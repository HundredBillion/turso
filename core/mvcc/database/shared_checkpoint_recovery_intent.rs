use super::shared_checkpoint_recovery::CheckpointRecoverySelection;
use super::*;
use crate::mvcc::commit_manifest::checkpoint_publication::MirroredCheckpointIntent;

pub(in crate::mvcc) struct RecoveredCheckpointIntent {
    runtime: Arc<SharedMvccRuntime>,
    intent: MirroredCheckpointIntent,
    pause: crate::mvcc::checkpoint_gate::CheckpointPause,
}

impl RecoveredCheckpointIntent {
    pub(in crate::mvcc) fn record(&self) -> crate::mvcc::commit_manifest::ManifestRecord {
        self.intent.record()
    }
    pub(in crate::mvcc) fn guard(&self) -> &crate::mvcc::shared_authority::AuthorityGuard {
        self.intent.guard()
    }
}

pub(in crate::mvcc) struct CheckpointIntentRecovery {
    selection: Option<CheckpointRecoverySelection>,
}

impl CheckpointRecoverySelection {
    pub(in crate::mvcc) fn begin_intent_mirror(self) -> Result<CheckpointIntentRecovery> {
        Ok(CheckpointIntentRecovery {
            selection: Some(self),
        })
    }
}

impl CheckpointIntentRecovery {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredCheckpointIntent> {
        Err(LimboError::InvalidArgument(
            "interrupted checkpoint intent mirroring is not implemented".into(),
        )
        .into())
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_recovery_intent_tests.rs"]
mod tests;
