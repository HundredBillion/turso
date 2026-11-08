use super::*;

#[allow(dead_code)]
pub(in crate::mvcc) struct ReclaimedRecoveredCheckpoint {
    recovered: FinalizedRecoveredCheckpoint,
}
#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryLogReclamation {
    recovered: Option<FinalizedRecoveredCheckpoint>,
}
#[allow(dead_code)]
impl FinalizedRecoveredCheckpoint {
    pub(in crate::mvcc) fn begin_log_reclamation(self) -> Result<RecoveryLogReclamation> {
        Ok(RecoveryLogReclamation {
            recovered: Some(self),
        })
    }
}
#[allow(dead_code)]
impl RecoveryLogReclamation {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<ReclaimedRecoveredCheckpoint> {
        Err(
            LimboError::InvalidArgument("recovered logical reclamation is not implemented".into())
                .into(),
        )
    }
}
