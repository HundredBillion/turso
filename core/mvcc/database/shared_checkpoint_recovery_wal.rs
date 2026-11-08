use super::*;
use crate::mvcc::shared_authority::AuthorityGuard;
use crate::storage::shared_wal_coordination::SharedWalRecoveryLocks;

impl SharedMvccRuntime {
    pub(in crate::mvcc) fn bind_physical_recovery_files(
        &self,
        guard: &AuthorityGuard,
        physical: &Arc<SharedWalRecoveryLocks>,
    ) -> Result<Vec<FileBinding>> {
        if !self.owns_authority(guard)
            || self.files.main.file_id()? != Some(self.database_id)
            || self.files.physical_wal.file_id()?.is_none()
            || self.files.physical_coordination.file_id()?.is_none()
        {
            return Err(LimboError::InvalidArgument(
                "physical recovery binding requires its retained runtime files".into(),
            ));
        }
        guard.ensure_no_snapshot_locks()?;
        let mut bindings = Vec::new();
        for file in [&self.main, &self.wal] {
            physical.validate_mapping(&self.files.physical_coordination)?;
            bindings.push(file.bind_physical_recovery(guard, physical.clone())?);
        }
        Ok(bindings)
    }
}

pub(in crate::mvcc) struct RecoveredIntentPhysicalBase {
    intent: super::shared_checkpoint_recovery_intent::RecoveredCheckpointIntent,
    parts: crate::mvcc::database::shared_bootstrap::PhysicalBaseParts,
}
impl RecoveredIntentPhysicalBase {
    pub(in crate::mvcc) fn record(&self) -> crate::mvcc::commit_manifest::ManifestRecord {
        self.intent.record()
    }
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.intent.guard()
    }
}

pub(in crate::mvcc) struct CheckpointPhysicalWalRecovery {
    _intent: super::shared_checkpoint_recovery_intent::RecoveredCheckpointIntent,
    _options: crate::OpenOptions,
    finished: bool,
}
impl CheckpointPhysicalWalRecovery {
    pub(super) fn new(
        intent: super::shared_checkpoint_recovery_intent::RecoveredCheckpointIntent,
        options: crate::OpenOptions,
    ) -> Result<Self> {
        Ok(Self {
            _intent: intent,
            _options: options,
            finished: false,
        })
    }
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredIntentPhysicalBase> {
        if self.finished {
            return Err(
                LimboError::InvalidArgument("physical WAL recovery has ended".into()).into(),
            );
        }
        self.finished = true;
        Err(LimboError::InvalidArgument(
            "committed physical WAL recovery is not implemented".into(),
        )
        .into())
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_recovery_wal_tests.rs"]
mod tests;
