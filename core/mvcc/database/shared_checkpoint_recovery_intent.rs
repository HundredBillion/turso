use super::shared_checkpoint_recovery::CheckpointRecoverySelection;
use super::*;
use crate::mvcc::checkpoint_gate::CheckpointPause;
use crate::mvcc::commit_manifest::checkpoint_publication::{
    CheckpointIntentPublication, MirroredCheckpointIntent,
};
use crate::mvcc::commit_manifest::ManifestRecord;
use crate::mvcc::shared_authority::{AuthorityGuard, PublicationLease};

pub(in crate::mvcc) struct RecoveredCheckpointIntent {
    runtime: Arc<SharedMvccRuntime>,
    intent: MirroredCheckpointIntent,
    pause: CheckpointPause,
}

impl RecoveredCheckpointIntent {
    pub(super) fn begin_source_id_inspection(
        self,
        input: super::shared_checkpoint_recovery_wal::SourceIdInspectionInput,
    ) -> Result<super::shared_checkpoint_recovery_wal::SourceIdInspection> {
        input.begin(self.runtime, self.intent, self.pause)
    }

    pub(super) fn retained_runtime(&self) -> &Arc<SharedMvccRuntime> {
        &self.runtime
    }

    pub(super) fn mirrored(&self) -> &MirroredCheckpointIntent {
        &self.intent
    }

    pub(in crate::mvcc) fn begin_physical_wal_recovery(
        self,
        options: crate::OpenOptions,
    ) -> Result<super::shared_checkpoint_recovery_wal::CheckpointPhysicalWalRecovery> {
        super::shared_checkpoint_recovery_wal::CheckpointPhysicalWalRecovery::new(self, options)
    }

    pub(in crate::mvcc) fn record(&self) -> ManifestRecord {
        self.intent.record()
    }
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.intent.guard()
    }
}

pub(in crate::mvcc) struct RecoveryIntentEvidence {
    runtime: Arc<SharedMvccRuntime>,
    manifest: Arc<dyn File>,
    manifest_id: FileId,
    main_id: FileId,
    wal_id: FileId,
    record: ManifestRecord,
    lease: Weak<PublicationLease>,
}

impl RecoveryIntentEvidence {
    fn new(runtime: Arc<SharedMvccRuntime>, selected: &LoadedManifest) -> Result<Self> {
        validate_selection(&runtime, selected)?;
        let native_id = |file: &Arc<dyn File>| {
            file.file_id()?.ok_or_else(|| {
                LimboError::InvalidArgument("intent recovery requires retained native files".into())
            })
        };
        let evidence = Self {
            manifest_id: native_id(&runtime.files.manifest)?,
            main_id: native_id(&runtime.files.main)?,
            wal_id: native_id(&runtime.files.physical_wal)?,
            manifest: runtime.files.manifest.clone(),
            record: selected.record(),
            lease: selected.guard()?.io_lease(),
            runtime,
        };
        evidence.validate(&evidence.manifest, selected.record(), selected.guard()?)?;
        Ok(evidence)
    }

    pub(in crate::mvcc) fn validate(
        &self,
        file: &Arc<dyn File>,
        record: ManifestRecord,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        if !Arc::ptr_eq(file, &self.manifest)
            || !Arc::ptr_eq(file, &self.runtime.files.manifest)
            || file.file_id()? != Some(self.manifest_id)
            || self.runtime.files.main.file_id()? != Some(self.main_id)
            || self.main_id != self.runtime.database_id
            || self.runtime.files.physical_wal.file_id()? != Some(self.wal_id)
            || record != self.record
            || record.database_id() != self.runtime.database_id
            || record.checkpoint_intent().is_none()
            || record.downgrade_intent().is_some()
            || !self.runtime.owns_authority(guard)
            || !Weak::ptr_eq(&self.lease, &guard.io_lease())
        {
            return Err(LimboError::InvalidArgument(
                "intent recovery evidence changed owner, files or selected record".into(),
            ));
        }
        guard.ensure_no_snapshot_locks()?;
        PublicationLease::pin(&self.lease)?;
        Ok(())
    }
}

pub(in crate::mvcc) struct CheckpointIntentRecovery {
    runtime: Arc<SharedMvccRuntime>,
    expected: ManifestRecord,
    lease: Weak<PublicationLease>,
    load: Option<ManifestLoad>,
    selected: Option<LoadedManifest>,
    evidence: Option<RecoveryIntentEvidence>,
    mirror: Option<CheckpointIntentPublication>,
    finished: bool,
    pause: Option<CheckpointPause>,
}

impl CheckpointRecoverySelection {
    pub(in crate::mvcc) fn begin_intent_mirror(self) -> Result<CheckpointIntentRecovery> {
        validate_selection(&self.runtime, &self.manifest)?;
        let expected = self.manifest.record();
        let lease = self.manifest.guard()?.io_lease();
        let load = ManifestLoad::new(
            self.runtime.files.manifest.clone(),
            self.manifest.into_guard()?,
            self.runtime.database_id,
        )?;
        Ok(CheckpointIntentRecovery {
            runtime: self.runtime,
            expected,
            lease,
            load: Some(load),
            selected: None,
            evidence: None,
            mirror: None,
            finished: false,
            pause: Some(self.pause),
        })
    }
}

impl CheckpointIntentRecovery {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredCheckpointIntent> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<RecoveredCheckpointIntent> {
        if self.finished {
            return Err(
                LimboError::InvalidArgument("intent recovery mirror has ended".into()).into(),
            );
        }
        if self.mirror.is_none() {
            if self.selected.is_none() {
                let selected = return_if_io!(self.load.as_mut().unwrap().poll());
                self.load = None;
                if selected.record() != self.expected
                    || !Weak::ptr_eq(&self.lease, &selected.guard()?.io_lease())
                {
                    return Err(LimboError::InvalidArgument(
                        "intent recovery selection changed before mirroring".into(),
                    )
                    .into());
                }
                self.evidence = Some(RecoveryIntentEvidence::new(
                    self.runtime.clone(),
                    &selected,
                )?);
                self.selected = Some(selected);
            }
            let guard = self.selected.as_ref().unwrap().guard()?;
            if !guard.io_retired() {
                return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
            }
            self.mirror = Some(
                self.selected
                    .take()
                    .unwrap()
                    .mirror_recovered_checkpoint_intent(self.evidence.as_ref().unwrap())?,
            );
        }
        let mirrored = return_if_io!(self.mirror.as_mut().unwrap().poll());
        self.mirror = None;
        self.evidence.as_ref().unwrap().validate(
            &self.runtime.files.manifest,
            mirrored.record(),
            mirrored.guard(),
        )?;
        self.finished = true;
        Ok(IOResult::Done(RecoveredCheckpointIntent {
            runtime: self.runtime.clone(),
            intent: mirrored,
            pause: self.pause.take().unwrap(),
        }))
    }

    fn abort(&mut self) {
        self.mirror = None;
        self.load = None;
        self.selected = None;
        self.evidence = None;
        self.finished = true;
        self.pause = None;
    }
}

impl Drop for CheckpointIntentRecovery {
    fn drop(&mut self) {
        self.abort();
    }
}

fn validate_selection(runtime: &SharedMvccRuntime, selected: &LoadedManifest) -> Result<()> {
    if !selected.belongs_to_file(&runtime.files.manifest)
        || selected.record().database_id() != runtime.database_id
        || selected.record().checkpoint_intent().is_none()
        || selected.record().downgrade_intent().is_some()
        || !runtime.owns_authority(selected.guard()?)
        || runtime.files.main.file_id()? != Some(runtime.database_id)
        || runtime.files.manifest.file_id()?.is_none()
        || runtime.files.physical_wal.file_id()?.is_none()
    {
        return Err(LimboError::InvalidArgument(
            "intent recovery requires its actual retained checkpoint selection".into(),
        ));
    }
    selected.guard()?.ensure_no_snapshot_locks()?;
    Ok(())
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_recovery_intent_tests.rs"]
mod tests;
