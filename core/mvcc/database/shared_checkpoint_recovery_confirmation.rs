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
mod tests {
    use super::*;
    use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE, SLOT_SIZE};

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn checkpoint_recovery_confirmation_native_discards_old_proof_and_retains_unrecovered_owner() {
        let fixture = shared_completed_checkpoint_fixture(|_| {
            Arc::new(crate::io::PlatformIO::new().unwrap())
        });
        let target = fixture.proof.target_identity();
        let source = fixture.proof.source_database();
        let runtime = fixture
            .database
            .get_mv_store()
            .as_ref()
            .unwrap()
            .shared_runtime
            .get()
            .unwrap()
            .clone();
        let io = fixture.io.clone();
        drop(fixture.ids);
        drop(fixture.guard);
        drop(fixture.proof);
        drop(fixture.pause);
        drop(fixture.connection);
        drop(fixture.database);
        let files = [
            fixture.path.clone(),
            fixture.namespace.join("logical"),
            fixture.namespace.join("physical-wal"),
            fixture.namespace.join("ids-a"),
            fixture.namespace.join("ids-b"),
            fixture.namespace.join("ids-selector"),
        ];
        let before = files.each_ref().map(|path| std::fs::read(path).unwrap());
        let mut opening = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| opening.poll()).unwrap();
        let record = selected.manifest.record();
        let mut physical = selected
            .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
            .unwrap();
        let physical = io.block(|| physical.poll()).unwrap();
        let mut loading = physical.begin_id_inspection().unwrap();
        let inspected = io.block(|| loading.poll()).unwrap();
        let maximum = inspected.ids.max_owner_epoch();
        let pages =
            runtime.files.main.size().unwrap() / u64::from(inspected.parts.header.page_size.get());
        let mut confirming = inspected.begin_confirmation().unwrap();
        let confirmed = io.block(|| confirming.poll()).unwrap();
        assert_eq!(confirmed.proof().target_identity(), target);
        assert_eq!(confirmed.proof().source_identity(), None);
        assert_eq!(confirmed.proof().source_database(), source);
        assert_eq!(confirmed.proof().materialized_floor(), -(pages as i64));
        assert!(confirmed.proof().matches_lease(confirmed.guard()));
        assert_eq!(confirmed.max_owner_epoch(), maximum);
        assert!(confirmed.guard().requires_recovery());
        assert!(confirmed.guard().published().is_err());
        assert!(matches!(
            runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        assert!(matches!(
            runtime.files.authority.try_acquire(),
            Err(LimboError::Busy)
        ));
        let manifest = std::fs::read(fixture.namespace.join("manifest")).unwrap();
        for slot in 0..2 {
            assert_eq!(
                ManifestRecord::decode(&manifest[slot * SLOT_SIZE..slot * SLOT_SIZE + RECORD_SIZE])
                    .unwrap(),
                Some(record)
            );
        }
        assert_eq!(
            files.each_ref().map(|path| std::fs::read(path).unwrap()),
            before
        );
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn checkpoint_recovery_confirmation_native_rejects_same_record_from_foreign_file() {
        let fixture = shared_completed_checkpoint_fixture(|_| {
            Arc::new(crate::io::PlatformIO::new().unwrap())
        });
        let runtime = fixture
            .database
            .get_mv_store()
            .as_ref()
            .unwrap()
            .shared_runtime
            .get()
            .unwrap()
            .clone();
        let io = fixture.io.clone();
        drop(fixture.ids);
        drop(fixture.guard);
        drop(fixture.proof);
        drop(fixture.pause);
        drop(fixture.connection);
        drop(fixture.database);
        let original = std::fs::read(fixture.namespace.join("manifest")).unwrap();
        let mut opening = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| opening.poll()).unwrap();
        let mut physical = selected
            .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
            .unwrap();
        let physical = io.block(|| physical.poll()).unwrap();
        let mut loading = physical.begin_id_inspection().unwrap();
        let inspected = io.block(|| loading.poll()).unwrap();
        let (_retained, evidence, guard) = suspend_inspected(inspected).unwrap();
        let fake_io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let fake = fake_io
            .open_file("substitute-manifest", crate::OpenFlags::Create, false)
            .unwrap();
        fake_io
            .wait_for_completion(
                fake.pwrite(
                    0,
                    Arc::new(crate::Buffer::new(original.clone())),
                    Completion::new_write(|_| {}),
                )
                .unwrap(),
            )
            .unwrap();
        let mut load = ManifestLoad::new(fake, guard, runtime.database_id).unwrap();
        let loaded = fake_io.block(|| Ok(load.poll()?)).unwrap();
        let error = match loaded.mirror_recovered_completion(evidence) {
            Err(error) => error,
            Ok(_) => panic!("substitute manifest file authorized recovered completion"),
        };
        assert!(
            error.to_string().contains("changed owner, files or target"),
            "{error}"
        );
        assert_eq!(
            std::fs::read(fixture.namespace.join("manifest")).unwrap(),
            original
        );
        assert!(matches!(
            runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        let guard = runtime.files.authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        assert!(guard.published().is_err());
    }
}
