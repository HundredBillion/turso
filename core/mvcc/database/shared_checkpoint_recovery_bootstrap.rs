use super::*;

#[allow(dead_code)]
pub(crate) struct RecoveredDatabaseReady {
    database: Arc<crate::Database>,
    connection: Arc<Connection>,
}
#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryBootstrap {
    reclaimed: Option<ReclaimedRecoveredCheckpoint>,
}
#[allow(dead_code)]
impl ReclaimedRecoveredCheckpoint {
    pub(in crate::mvcc) fn begin_bootstrap(self) -> Result<RecoveryBootstrap> {
        Ok(RecoveryBootstrap {
            reclaimed: Some(self),
        })
    }
}
#[allow(dead_code)]
impl RecoveryBootstrap {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredDatabaseReady> {
        if self.reclaimed.take().is_none() {
            return Err(
                LimboError::InvalidArgument("normal recovered bootstrap has ended".into()).into(),
            );
        }
        Err(LimboError::InvalidArgument(
            "normal recovered database bootstrap is not implemented".into(),
        )
        .into())
    }
}
