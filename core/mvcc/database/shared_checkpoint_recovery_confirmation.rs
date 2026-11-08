use super::*;
use crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof;
use crate::mvcc::shared_authority::AuthorityGuard;
use crate::mvcc::shared_ids::io_protocol::checkpoint_recovery::SuspendedRecoveryIds;

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveredCheckpointConfirmation {
    runtime: Arc<SharedMvccRuntime>,
    parts: PhysicalBaseParts,
    ids: SuspendedRecoveryIds,
    max_owner_epoch: Option<u64>,
    guard: AuthorityGuard,
    proof: CompletedCheckpointProof,
    pause: CheckpointPause,
}

#[allow(dead_code)]
impl RecoveredCheckpointConfirmation {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        &self.guard
    }
    pub(in crate::mvcc) fn proof(&self) -> &CompletedCheckpointProof {
        &self.proof
    }
    pub(in crate::mvcc) fn max_owner_epoch(&self) -> Option<u64> {
        self.max_owner_epoch
    }
}

pub(in crate::mvcc) struct RecoveryCompletionEvidence {
    runtime: Arc<SharedMvccRuntime>,
    manifest: Arc<dyn File>,
    manifest_id: crate::io::FileId,
    record: ManifestRecord,
    target: RecoveryIdentity,
    source_database: Option<DatabaseGeneration>,
    physical_floor: i64,
    physical_bytes: u64,
    lease: crate::sync::Weak<crate::mvcc::shared_authority::PublicationLease>,
}

impl RecoveryCompletionEvidence {
    pub(in crate::mvcc) fn validate(
        &self,
        file: &Arc<dyn File>,
        record: ManifestRecord,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        if !Arc::ptr_eq(file, &self.manifest)
            || file.file_id()? != Some(self.manifest_id)
            || record != self.record
            || record.requires_recovery()
            || !self.runtime.owns_authority(guard)
            || !crate::sync::Weak::ptr_eq(&self.lease, &guard.io_lease())
            || self.runtime.files.main.file_id()? != Some(self.runtime.database_id)
            || self.runtime.files.main.size()? != self.physical_bytes
            || self.runtime.files.physical_wal.size()? != 0
        {
            return Err(LimboError::InvalidArgument(
                "recovered completion evidence changed owner, files or target".into(),
            ));
        }
        guard.ensure_no_snapshot_locks()?;
        crate::mvcc::shared_authority::PublicationLease::pin(&self.lease)?;
        Ok(())
    }
    pub(in crate::mvcc) fn target(&self) -> RecoveryIdentity {
        self.target
    }
    pub(in crate::mvcc) fn source_database(&self) -> Option<DatabaseGeneration> {
        self.source_database
    }
    pub(in crate::mvcc) fn physical_floor(&self) -> i64 {
        self.physical_floor
    }
}

struct RetainedConfirmation {
    runtime: Arc<SharedMvccRuntime>,
    parts: PhysicalBaseParts,
    ids: SuspendedRecoveryIds,
    max_owner_epoch: Option<u64>,
    pause: CheckpointPause,
}

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryCheckpointConfirmationState {
    retained: Option<RetainedConfirmation>,
    evidence: Option<RecoveryCompletionEvidence>,
    load: Option<ManifestLoad>,
    selected: Option<LoadedManifest>,
    mirror: Option<crate::mvcc::commit_manifest::checkpoint_publication::checkpoint_recovery_confirmation::RecoveryCompletionMirror>,
    finished: bool,
}

fn suspend_inspected(
    inspected: InspectedRecoveryIds,
) -> Result<(
    RetainedConfirmation,
    RecoveryCompletionEvidence,
    AuthorityGuard,
)> {
    let InspectedRecoveryIds {
        runtime,
        parts,
        record,
        pause,
        ids,
    } = inspected;
    let target = ids.identity();
    let physical_bytes = runtime.files.main.size()?;
    let page_size = u64::from(parts.header.page_size.get());
    let physical_floor = i64::try_from(physical_bytes / page_size)
        .ok()
        .and_then(i64::checked_neg)
        .filter(|floor| *floor < 0 && *floor != i64::MIN)
        .ok_or(LimboError::DatabaseFull)?;
    if record.requires_recovery()
        || record.database_id() != runtime.database_id
        || record.log_generation() != target.database.generation
        || record.prefix() != target.prefix
        || record.log_salt() != 0
        || target.prefix.end_offset != 0
        || target.prefix.running_crc != 0
        || target.database.checkpoint_timestamp != target.prefix.committed_timestamp
        || parts.persistent_timestamp != target.database.checkpoint_timestamp
        || physical_bytes % page_size != 0
        || physical_bytes / page_size < u64::from(parts.header.database_size.get())
        || ids.logical_floor() > physical_floor
    {
        return Err(LimboError::InvalidArgument(
            "confirmation requires inspected completed physical target".into(),
        ));
    }
    let manifest = runtime.files.manifest.clone();
    let manifest_id = manifest.file_id()?.ok_or_else(|| {
        LimboError::InvalidArgument("confirmation requires native manifest identity".into())
    })?;
    let evidence = RecoveryCompletionEvidence {
        runtime: runtime.clone(),
        manifest,
        manifest_id,
        record,
        target,
        source_database: ids.source_database(),
        physical_floor,
        physical_bytes,
        lease: ids.guard().io_lease(),
    };
    evidence.validate(&evidence.manifest, record, ids.guard())?;
    let max_owner_epoch = ids.max_owner_epoch();
    let (ids, guard) = ids.suspend();
    Ok((
        RetainedConfirmation {
            runtime,
            parts,
            ids,
            max_owner_epoch,
            pause,
        },
        evidence,
        guard,
    ))
}

#[allow(dead_code)]
impl InspectedRecoveryIds {
    pub(in crate::mvcc) fn begin_confirmation(self) -> Result<RecoveryCheckpointConfirmationState> {
        let (retained, evidence, guard) = suspend_inspected(self)?;
        let load = ManifestLoad::new(
            retained.runtime.files.manifest.clone(),
            guard,
            retained.runtime.database_id,
        )?;
        Ok(RecoveryCheckpointConfirmationState {
            retained: Some(retained),
            evidence: Some(evidence),
            load: Some(load),
            selected: None,
            mirror: None,
            finished: false,
        })
    }
}

#[allow(dead_code)]
impl RecoveryCheckpointConfirmationState {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredCheckpointConfirmation> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn poll_inner(&mut self) -> IOResultOr<RecoveredCheckpointConfirmation> {
        if self.finished {
            return Err(
                LimboError::InvalidArgument("recovered confirmation has ended".into()).into(),
            );
        }
        if self.mirror.is_none() {
            if self.selected.is_none() {
                self.selected = Some(return_if_io!(self.load.as_mut().unwrap().poll()));
                self.load = None;
            }
            let selected = self.selected.as_ref().unwrap();
            let guard = selected.guard()?;
            if !guard.io_retired() {
                return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
            }
            self.mirror = Some(
                self.selected
                    .take()
                    .unwrap()
                    .mirror_recovered_completion(self.evidence.take().unwrap())?,
            );
        }
        let mirrored = return_if_io!(self.mirror.as_mut().unwrap().poll());
        self.mirror = None;
        let (proof, guard) = mirrored.into_parts();
        let retained = self.retained.take().unwrap();
        self.finished = true;
        Ok(IOResult::Done(RecoveredCheckpointConfirmation {
            runtime: retained.runtime,
            parts: retained.parts,
            ids: retained.ids,
            max_owner_epoch: retained.max_owner_epoch,
            guard,
            proof,
            pause: retained.pause,
        }))
    }
    fn abort(&mut self) {
        self.mirror = None;
        self.load = None;
        self.selected = None;
        self.evidence = None;
        self.retained = None;
        self.finished = true;
    }
}
impl Drop for RecoveryCheckpointConfirmationState {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_recovery_confirmation_tests.rs"]
mod tests;
