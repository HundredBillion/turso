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
        physical.validate_mapping(&self.files.physical_coordination)?;
        guard.ensure_no_snapshot_locks()?;
        Ok(vec![
            self.main.bind_physical_recovery(guard, physical.clone())?,
            self.wal.bind_physical_recovery(guard, physical.clone())?,
        ])
    }
}
