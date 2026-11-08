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

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryCheckpointConfirmationState {
    inspected: Option<InspectedRecoveryIds>,
}

#[allow(dead_code)]
impl InspectedRecoveryIds {
    pub(in crate::mvcc) fn begin_confirmation(self) -> Result<RecoveryCheckpointConfirmationState> {
        Ok(RecoveryCheckpointConfirmationState {
            inspected: Some(self),
        })
    }
}

#[allow(dead_code)]
impl RecoveryCheckpointConfirmationState {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredCheckpointConfirmation> {
        self.inspected.take();
        Err(LimboError::InvalidArgument(
            "recovered checkpoint confirmation is not implemented".into(),
        )
        .into())
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
}
