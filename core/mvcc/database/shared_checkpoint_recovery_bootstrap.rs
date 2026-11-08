use super::*;

#[allow(dead_code)]
pub(crate) struct RecoveredDatabaseReady {
    database: Arc<crate::Database>,
    connection: Arc<Connection>,
}
#[allow(dead_code)]
pub(crate) struct RecoveryBootstrapEvidence {
    store: Arc<crate::MvStore>,
    record: crate::mvcc::commit_manifest::ManifestRecord,
    recovered: FinalizedRecoveredCheckpoint,
}
#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryBootstrap {
    reclaimed: Option<ReclaimedRecoveredCheckpoint>,
    evidence: Option<RecoveryBootstrapEvidence>,
}
#[allow(dead_code)]
impl ReclaimedRecoveredCheckpoint {
    pub(in crate::mvcc) fn begin_bootstrap(self) -> Result<RecoveryBootstrap> {
        Ok(RecoveryBootstrap {
            reclaimed: Some(self),
            evidence: None,
        })
    }
}
#[allow(dead_code)]
impl RecoveryBootstrap {
    fn prepare_store(&mut self) -> Result<()> {
        if self.evidence.is_some() {
            return Ok(());
        }
        let reclaimed = self.reclaimed.take().ok_or_else(|| {
            LimboError::InvalidArgument("normal recovered bootstrap has ended".into())
        })?;
        let runtime = &reclaimed.recovered.runtime;
        reclaimed
            .recovered
            .config
            .validate_history(runtime.files.authority.configured_history_limit())?;
        validate_target(
            runtime,
            &reclaimed.recovered.parts,
            reclaimed.proof(),
            reclaimed.guard(),
        )?;
        reclaimed.reservation.validate(runtime)?;
        let target = reclaimed.proof().target_identity();
        let logical: Arc<dyn File> = runtime.logical.clone();
        let snapshot = runtime.storage.shared_commit_snapshot(&logical)?;
        if !reclaimed.guard().io_retired() {
            return Err(LimboError::Busy);
        }
        if reclaimed.record.requires_recovery()
            || reclaimed.record.database_id() != runtime.database_id
            || reclaimed.record.log_generation() != target.database.generation
            || reclaimed.record.prefix() != target.prefix
            || reclaimed.record.log_salt() != 0
            || runtime.logical.size()? != 0
            || snapshot.offset != 0
            || snapshot.running_crc != 0
            || snapshot.header.is_some()
        {
            return Err(LimboError::InvalidArgument(
                "bootstrap no longer matches reclaimed checkpoint".into(),
            ));
        }
        let store = reclaimed
            .reservation
            .construct_store(reclaimed.recovered.config.store_allocator())?;
        self.evidence = Some(RecoveryBootstrapEvidence {
            recovered: reclaimed.recovered,
            record: reclaimed.record,
            store,
        });
        Ok(())
    }
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredDatabaseReady> {
        let result = (|| {
            self.prepare_store()?;
            Err(LimboError::InvalidArgument(
                "normal recovered database bootstrap is not implemented".into(),
            )
            .into())
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn abort(&mut self) {
        if let Some(evidence) = self.evidence.take() {
            evidence.recovered.runtime.replay.lock().take();
            drop(evidence.store);
            drop(evidence.recovered);
        }
        self.reclaimed = None;
    }
}
impl Drop for RecoveryBootstrap {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_recovery_bootstrap_tests.rs"]
mod tests;
