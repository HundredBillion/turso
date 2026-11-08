#[path = "shared_checkpoint_recovery_bootstrap.rs"]
mod shared_checkpoint_recovery_bootstrap;

use super::*;
use crate::io::FileSyncType;
use crate::mvcc::database::shared_runtime::shared_store_claim::SharedStoreReservation;
use crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::finalize::SuspendedFinalizedIds;

#[allow(dead_code)]
pub(in crate::mvcc) struct ReclaimedRecoveredCheckpoint {
    recovered: FinalizedRecoveredCheckpoint,
    record: crate::mvcc::commit_manifest::ManifestRecord,
    reservation: SharedStoreReservation,
}
#[allow(dead_code)]
impl ReclaimedRecoveredCheckpoint {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.recovered.guard()
    }
    pub(in crate::mvcc) fn proof(&self) -> &CompletedCheckpointProof {
        self.recovered.proof()
    }
    #[cfg(test)]
    pub(in crate::mvcc) fn config_for_test(&self) -> &Arc<RecoveryOpenConfig> {
        &self.recovered.config
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Manifest,
    Truncate,
    Sync,
    Retire,
    Finished,
}
#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryLogReclamation {
    runtime: Arc<SharedMvccRuntime>,
    config: Arc<RecoveryOpenConfig>,
    parts: Option<PhysicalBaseParts>,
    finalized: Option<FinalizedIds>,
    suspended: Option<SuspendedFinalizedIds>,
    pause: Option<CheckpointPause>,
    reservation: Option<SharedStoreReservation>,
    load: Option<ManifestLoad>,
    loaded: Option<LoadedManifest>,
    record: Option<crate::mvcc::commit_manifest::ManifestRecord>,
    binding: Option<FileBinding>,
    pending: Option<Completion>,
    phase: Phase,
}

#[allow(dead_code)]
impl FinalizedRecoveredCheckpoint {
    pub(in crate::mvcc) fn begin_log_reclamation(self) -> Result<RecoveryLogReclamation> {
        if self.runtime.original_readonly {
            return Err(LimboError::InvalidArgument(
                "shared checkpoint recovery requires a writable participant".into(),
            ));
        }
        validate_target(&self.runtime, &self.parts, self.proof(), self.guard())?;
        if !self.guard().io_retired() {
            return Err(LimboError::Busy);
        }
        let reservation = self.runtime.reserve_store()?;
        let (suspended, guard) = self.finalized.suspend();
        let load = ManifestLoad::new(
            self.runtime.files.manifest.clone(),
            guard,
            self.runtime.database_id,
        )?;
        Ok(RecoveryLogReclamation {
            runtime: self.runtime,
            config: self.config,
            parts: Some(self.parts),
            finalized: None,
            suspended: Some(suspended),
            pause: Some(self.pause),
            reservation: Some(reservation),
            load: Some(load),
            loaded: None,
            record: None,
            binding: None,
            pending: None,
            phase: Phase::Manifest,
        })
    }
}

#[allow(dead_code)]
impl RecoveryLogReclamation {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<ReclaimedRecoveredCheckpoint> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn poll_inner(&mut self) -> IOResultOr<ReclaimedRecoveredCheckpoint> {
        loop {
            if let Some(completion) = self.pending.take() {
                if !completion.finished() {
                    self.pending = Some(completion.clone());
                    return Ok(IOResult::IO(IOCompletions(completion)));
                }
                if let Some(error) = completion.get_error() {
                    return Err(error.into());
                }
            }
            match self.phase {
                Phase::Manifest => {
                    if self.loaded.is_none() {
                        self.loaded = Some(return_if_io!(self.load.as_mut().unwrap().poll()));
                        self.load = None;
                    }
                    let loaded = self.loaded.as_ref().unwrap();
                    let guard = loaded.guard()?;
                    if !guard.io_retired() {
                        return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
                    }
                    let record = loaded.record();
                    let proof = self.suspended.as_ref().unwrap().proof();
                    let target = proof.target_identity();
                    if record.requires_recovery()
                        || record.database_id() != self.runtime.database_id
                        || record.log_generation() != target.database.generation
                        || record.prefix() != target.prefix
                        || record.log_salt() != 0
                        || !proof.matches_lease(guard)
                    {
                        return Err(LimboError::InvalidArgument(
                            "recovered reclamation manifest changed target".into(),
                        )
                        .into());
                    }
                    let guard = self.loaded.take().unwrap().into_guard()?;
                    self.finalized = Some(self.suspended.take().unwrap().rejoin(guard)?);
                    self.record = Some(record);
                    self.validate_owner()?;
                    self.binding = Some(
                        self.runtime
                            .logical
                            .bind_checkpoint(self.finalized.as_ref().unwrap().guard())?,
                    );
                    self.phase = Phase::Truncate;
                }
                Phase::Truncate => {
                    self.validate_owner()?;
                    self.pending = Some(
                        self.runtime
                            .logical
                            .truncate(0, Completion::new_trunc(|_| {}))?,
                    );
                    self.phase = Phase::Sync;
                }
                Phase::Sync => {
                    self.pending = Some(
                        self.runtime
                            .logical
                            .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                    );
                    self.phase = Phase::Retire;
                }
                Phase::Retire => {
                    self.validate_owner()?;
                    let finalized = self.finalized.as_ref().unwrap();
                    let guard = finalized.guard();
                    if !guard.io_retired() {
                        return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
                    }
                    if self.runtime.logical.size()? != 0 {
                        return Err(LimboError::InvalidArgument(
                            "recovered logical file is not empty after reclamation".into(),
                        )
                        .into());
                    }
                    self.runtime.storage.adopt_shared_prefix(
                        None,
                        0,
                        0,
                        finalized
                            .proof()
                            .target_identity()
                            .database
                            .checkpoint_timestamp,
                    )?;
                    let logical: Arc<dyn File> = self.runtime.logical.clone();
                    let snapshot = self.runtime.storage.shared_commit_snapshot(&logical)?;
                    if snapshot.offset != 0
                        || snapshot.running_crc != 0
                        || snapshot.header.is_some()
                    {
                        return Err(LimboError::InvalidArgument(
                            "recovered logical append state is not empty".into(),
                        )
                        .into());
                    }
                    self.validate_owner()?;
                    self.binding = None;
                    self.phase = Phase::Finished;
                    return Ok(IOResult::Done(ReclaimedRecoveredCheckpoint {
                        recovered: FinalizedRecoveredCheckpoint {
                            runtime: self.runtime.clone(),
                            config: self.config.clone(),
                            parts: self.parts.take().unwrap(),
                            finalized: self.finalized.take().unwrap(),
                            pause: self.pause.take().unwrap(),
                        },
                        record: self.record.take().unwrap(),
                        reservation: self.reservation.take().unwrap(),
                    }));
                }
                Phase::Finished => {
                    return Err(LimboError::InvalidArgument(
                        "recovered logical reclamation has ended".into(),
                    )
                    .into())
                }
            }
        }
    }
    fn validate_owner(&self) -> Result<()> {
        self.reservation.as_ref().unwrap().validate(&self.runtime)?;
        let finalized = self.finalized.as_ref().unwrap();
        validate_target(
            &self.runtime,
            self.parts.as_ref().unwrap(),
            finalized.proof(),
            finalized.guard(),
        )?;
        let logical: Arc<dyn File> = self.runtime.logical.clone();
        self.runtime.storage.shared_commit_snapshot(&logical)?;
        Ok(())
    }
    fn abort(&mut self) {
        self.pending = None;
        self.binding = None;
        self.load = None;
        self.loaded = None;
        self.finalized = None;
        self.suspended = None;
        self.parts = None;
        self.pause = None;
        self.reservation = None;
        self.phase = Phase::Finished;
    }
}
impl Drop for RecoveryLogReclamation {
    fn drop(&mut self) {
        self.abort();
    }
}

fn validate_target(
    runtime: &Arc<SharedMvccRuntime>,
    parts: &PhysicalBaseParts,
    proof: &CompletedCheckpointProof,
    guard: &AuthorityGuard,
) -> Result<()> {
    let target = proof.target_identity();
    let bytes = runtime.files.main.size()?;
    let page_size = u64::from(parts.header.page_size.get());
    let floor = i64::try_from(bytes / page_size)
        .ok()
        .and_then(i64::checked_neg)
        .filter(|floor| *floor < 0 && *floor != i64::MIN)
        .ok_or(LimboError::DatabaseFull)?;
    if !runtime.owns_authority(guard)
        || !proof.matches_lease(guard)
        || proof.source_identity().is_some()
        || runtime.replay.lock().is_some()
        || runtime.files.main.file_id()? != Some(runtime.database_id)
        || runtime.files.physical_wal.size()? != 0
        || bytes % page_size != 0
        || bytes / page_size < u64::from(parts.header.database_size.get())
        || parts.persistent_timestamp != target.database.checkpoint_timestamp
        || proof.materialized_floor() != floor
        || target.database.generation == 0
        || target.prefix.end_offset != 0
        || target.prefix.running_crc != 0
        || target.database.checkpoint_timestamp != target.prefix.committed_timestamp
        || guard.published()? != target.prefix
        || !guard.incarnation_ready()?
    {
        return Err(LimboError::InvalidArgument(
            "recovered reclamation owner or completed target changed".into(),
        ));
    }
    guard.ensure_no_snapshot_locks()?;
    Ok(())
}
