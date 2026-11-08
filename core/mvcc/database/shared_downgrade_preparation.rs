use super::*;
use crate::mvcc::commit_manifest::downgrade_publication::MirroredDowngradeIntent;
use crate::mvcc::mode_transition::OfflinePrerequisite;

pub(in crate::mvcc) struct SharedDowngradePreparation;

impl InstalledLocalCheckpoint {
    pub(in crate::mvcc) fn begin_downgrade(
        self,
        _connection: Arc<Connection>,
        _prerequisite: OfflinePrerequisite,
    ) -> Result<SharedDowngradePreparation> {
        Err(LimboError::InvalidArgument(
            "installed checkpoint downgrade preparation is not implemented".into(),
        ))
    }
}
impl SharedDowngradePreparation {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<MirroredDowngradeIntent> {
        Err(LimboError::InvalidArgument("downgrade preparation is not implemented".into()).into())
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;
    use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE, SLOT_SIZE};
    use crate::{ConnectOpts, Database, EngineDialect, OpenOptions};

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_prepare_native_consumes_genuine_installed_checkpoint() {
        let fixture =
            shared_completed_checkpoint_fixture(|_| Arc::new(crate::io::SyscallIO::new().unwrap()));
        let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap().clone();
        let mut finalizer = fixture.ids.finalize(fixture.guard, fixture.proof).unwrap();
        let finalized = fixture.io.block(|| Ok(finalizer.poll()?)).unwrap();
        let target = finalized.proof().target_identity();
        let published = PublishedSharedCheckpoint {
            finalized,
            pause: fixture.pause,
        };
        let mut reading = published
            .begin_physical_read(store.clone(), fixture.connection.clone())
            .unwrap();
        let verified = fixture.io.block(|| reading.poll()).unwrap();
        let mut installing = verified
            .begin_local_install(store, fixture.connection.clone())
            .unwrap();
        let installed = fixture.io.block(|| installing.poll()).unwrap();
        assert!(runtime.replay.lock().is_none());
        let before_main = std::fs::read(&fixture.path).unwrap();
        let wal_path = std::path::PathBuf::from(format!("{}-wal", fixture.path.display()));
        let before_wal = std::fs::read(&wal_path).unwrap();
        assert!(before_wal.is_empty());
        let ids = ["ids-a", "ids-b", "ids-selector"]
            .map(|name| std::fs::read(fixture.namespace.join(name)).unwrap());
        let mut preparing = installed
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .expect("genuine installed checkpoint must prepare downgrade intent");
        let proof = fixture.io.block(|| preparing.poll()).unwrap();
        assert_eq!(proof.record().prefix(), target.prefix);
        assert_eq!(proof.record().log_generation(), target.database.generation);
        assert!(proof.record().downgrade_intent().is_some());
        let bytes = std::fs::read(fixture.namespace.join("manifest")).unwrap();
        for offset in [0, SLOT_SIZE] {
            assert_eq!(
                ManifestRecord::decode(&bytes[offset..offset + RECORD_SIZE]).unwrap(),
                Some(proof.record())
            );
        }
        assert!(runtime.replay.lock().is_none());
        assert!(matches!(
            runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        assert_eq!(std::fs::read(&fixture.path).unwrap(), before_main);
        assert_eq!(std::fs::read(&wal_path).unwrap(), before_wal);
        assert_eq!(
            ["ids-a", "ids-b", "ids-selector"]
                .map(|name| std::fs::read(fixture.namespace.join(name)).unwrap()),
            ids
        );
        drop(proof);
        drop(preparing);
        let options =
            OpenOptions::new(Arc::new(EngineDialect)).with_shared_mvcc_for_test(fixture.limit);
        let reopened = Database::open(
            Arc::new(crate::io::SyscallIO::new().unwrap()),
            fixture.path.to_str().unwrap(),
            options,
        );
        assert!(
            matches!(reopened, Err(LimboError::InvalidArgument(message)) if message.contains("shared manifest recovery is required"))
        );
        assert!(fixture
            .connection
            .prepare("SELECT count(*) FROM preserved")
            .is_err());
        let _ = ConnectOpts::new();
    }
}
