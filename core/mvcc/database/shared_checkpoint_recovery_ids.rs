#[path = "shared_checkpoint_recovery_confirmation.rs"]
mod shared_checkpoint_recovery_confirmation;
pub(in crate::mvcc) use shared_checkpoint_recovery_confirmation::RecoveryCompletionEvidence;

use super::shared_checkpoint_recovery_physical::InspectedRecoveryBase;
use super::*;
use crate::mvcc::checkpoint_gate::CheckpointPause;
use crate::mvcc::commit_manifest::ManifestRecord;
use crate::mvcc::database::shared_bootstrap::PhysicalBaseParts;
use crate::mvcc::shared_ids::io_protocol::checkpoint_recovery::{
    RecoveryTargetIdLoad, UnrecoveredIds,
};

#[allow(dead_code)]
pub(in crate::mvcc) struct InspectedRecoveryIds {
    runtime: Arc<SharedMvccRuntime>,
    parts: PhysicalBaseParts,
    record: ManifestRecord,
    pause: CheckpointPause,
    ids: UnrecoveredIds,
}

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryIdInspection {
    runtime: Arc<SharedMvccRuntime>,
    parts: Option<PhysicalBaseParts>,
    record: ManifestRecord,
    pause: Option<CheckpointPause>,
    load: Option<RecoveryTargetIdLoad>,
}

impl InspectedRecoveryBase {
    #[allow(dead_code)]
    pub(in crate::mvcc) fn begin_id_inspection(self) -> Result<RecoveryIdInspection> {
        let (parts, selection) = self.into_parts();
        let record = selection.manifest.record();
        let prefix = record.prefix();
        if record.requires_recovery()
            || prefix.end_offset != 0
            || prefix.running_crc != 0
            || record.log_salt() != 0
            || parts.persistent_timestamp != prefix.committed_timestamp
        {
            return Err(LimboError::InvalidArgument(
                "ID inspection requires the selected completed checkpoint".into(),
            ));
        }
        let bytes = selection.runtime.files.main.size()?;
        let physical_floor = i64::try_from(bytes / u64::from(parts.header.page_size.get()))
            .ok()
            .and_then(i64::checked_neg)
            .filter(|floor| *floor != i64::MIN)
            .ok_or(LimboError::DatabaseFull)?;
        let identity = RecoveryIdentity {
            database: DatabaseGeneration {
                generation: record.log_generation(),
                checkpoint_timestamp: parts.persistent_timestamp,
            },
            prefix,
        };
        let load = selection.runtime.ids.inspect_completed_target(
            selection.manifest.into_guard()?,
            identity,
            physical_floor,
        )?;
        Ok(RecoveryIdInspection {
            runtime: selection.runtime,
            parts: Some(parts),
            record,
            pause: Some(selection.pause),
            load: Some(load),
        })
    }
}

#[allow(dead_code)]
impl RecoveryIdInspection {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<InspectedRecoveryIds> {
        let result = self.poll_inner();
        if result.is_err() {
            self.load = None;
            self.parts = None;
            self.pause = None;
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<InspectedRecoveryIds> {
        let load = self.load.as_mut().ok_or_else(|| {
            LimboError::InvalidArgument("recovery ID inspection has ended".into())
        })?;
        let ids = return_if_io!(load.poll());
        self.load = None;
        Ok(IOResult::Done(InspectedRecoveryIds {
            runtime: self.runtime.clone(),
            parts: self.parts.take().unwrap(),
            record: self.record,
            pause: self.pause.take().unwrap(),
            ids,
        }))
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
