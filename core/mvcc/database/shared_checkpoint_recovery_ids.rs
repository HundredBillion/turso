use super::shared_checkpoint_recovery_physical::InspectedRecoveryBase;
use super::*;
use crate::mvcc::checkpoint_gate::CheckpointPause;
use crate::mvcc::commit_manifest::ManifestRecord;
use crate::mvcc::database::shared_bootstrap::PhysicalBaseParts;
use crate::mvcc::shared_ids::io_protocol::checkpoint_recovery::UnrecoveredIds;

#[allow(dead_code)]
pub(in crate::mvcc) struct InspectedRecoveryIds {
    pub runtime: Arc<SharedMvccRuntime>,
    pub parts: PhysicalBaseParts,
    pub record: ManifestRecord,
    pub pause: CheckpointPause,
    pub ids: UnrecoveredIds,
}

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryIdInspection;

impl InspectedRecoveryBase {
    #[allow(dead_code)]
    pub(in crate::mvcc) fn begin_id_inspection(self) -> Result<RecoveryIdInspection> {
        Err(LimboError::InvalidArgument(
            "completed checkpoint ID inspection is not implemented".into(),
        ))
    }
}

#[allow(dead_code)]
impl RecoveryIdInspection {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<InspectedRecoveryIds> {
        Err(LimboError::InvalidArgument(
            "completed checkpoint ID inspection is not implemented".into(),
        )
        .into())
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn checkpoint_recovery_ids_native_inspects_completed_target_without_mapped_recovery() {
        let fixture = shared_completed_checkpoint_fixture(|_| {
            Arc::new(crate::io::PlatformIO::new().unwrap())
        });
        let target = fixture.proof.target_identity();
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
        let paths =
            ["manifest", "ids-a", "ids-b", "ids-selector"].map(|name| fixture.namespace.join(name));
        let before = paths.each_ref().map(|path| std::fs::read(path).unwrap());
        let mut opening = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| opening.poll()).unwrap();
        let mut physical = selected
            .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
            .unwrap();
        let parts = io.block(|| physical.poll()).unwrap();
        let mut loading = parts.begin_id_inspection().unwrap();
        let inspected = io.block(|| loading.poll()).unwrap();
        assert_eq!(inspected.ids.identity(), target);
        assert!(inspected.ids.source_database().is_some());
        let bytes = runtime.files.main.size().unwrap();
        let pages = bytes / u64::from(inspected.parts.header.page_size.get());
        assert!(inspected.ids.logical_floor() <= -(pages as i64));
        assert!(inspected.ids.guard().requires_recovery());
        assert!(inspected.ids.guard().published().is_err());
        assert!(matches!(
            runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        assert_eq!(
            paths.each_ref().map(|path| std::fs::read(path).unwrap()),
            before
        );
    }
}
