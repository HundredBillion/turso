use super::shared_checkpoint_recovery::CheckpointRecoverySelection;
use super::*;
use crate::mvcc::database::shared_bootstrap::PhysicalBaseParts;

#[allow(dead_code)]
pub(in crate::mvcc) struct InspectedRecoveryBase {
    pub parts: PhysicalBaseParts,
    pub selection: CheckpointRecoverySelection,
}

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryPhysicalRead;

impl CheckpointRecoverySelection {
    #[allow(dead_code)]
    pub(in crate::mvcc) fn begin_physical_read(
        self,
        _options: crate::OpenOptions,
    ) -> Result<RecoveryPhysicalRead> {
        Err(LimboError::InvalidArgument(
            "checkpoint recovery physical inspection is not implemented".into(),
        ))
    }
}

#[allow(dead_code)]
impl RecoveryPhysicalRead {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<InspectedRecoveryBase> {
        Err(LimboError::InvalidArgument(
            "checkpoint recovery physical inspection is not implemented".into(),
        )
        .into())
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn checkpoint_recovery_physical_native_inspects_without_recovering_authority() {
        let fixture =
            shared_prepared_checkpoint_fixture(|_| Arc::new(crate::io::PlatformIO::new().unwrap()));
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
        drop(fixture.prepared);
        drop(fixture.token);
        drop(fixture.pause);
        drop(fixture.materialized);
        drop(fixture.connection);
        drop(fixture.database);
        let main = std::fs::read(&fixture.path).unwrap();
        let manifest = std::fs::read(fixture.namespace.join("manifest")).unwrap();
        let mut opening = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| opening.poll()).unwrap();
        let expected = selected.manifest.record().prefix().committed_timestamp;
        assert!(selected.manifest.record().checkpoint_intent().is_some());
        assert!(selected.manifest.guard().unwrap().requires_recovery());
        let mut reading = selected
            .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
            .unwrap();
        let inspected = io.block(|| reading.poll()).unwrap();
        assert_eq!(inspected.parts.persistent_timestamp, expected);
        assert!(inspected
            .parts
            .schema
            .get_table(crate::namespace::NamespaceId::ROOT, "preserved")
            .is_some());
        assert!(inspected
            .selection
            .manifest
            .guard()
            .unwrap()
            .requires_recovery());
        assert!(inspected
            .selection
            .manifest
            .guard()
            .unwrap()
            .published()
            .is_err());
        assert!(matches!(
            runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        assert!(reading.poll().is_err());
        assert_eq!(std::fs::read(&fixture.path).unwrap(), main);
        assert_eq!(
            std::fs::read(fixture.namespace.join("manifest")).unwrap(),
            manifest
        );
    }
}
