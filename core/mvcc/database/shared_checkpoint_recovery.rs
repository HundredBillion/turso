use super::*;
use crate::mvcc::checkpoint_gate::CheckpointPause;

#[allow(dead_code)]
pub(in crate::mvcc) struct CheckpointRecoverySelection {
    pub manifest: LoadedManifest,
    pub pause: CheckpointPause,
}

#[allow(dead_code)]
pub(in crate::mvcc) struct CheckpointRecoveryOpen {
    load: Option<ManifestLoad>,
    pause: Option<CheckpointPause>,
}

impl SharedMvccRuntime {
    #[allow(dead_code)]
    pub(in crate::mvcc) fn begin_checkpoint_recovery(
        self: &Arc<Self>,
    ) -> Result<CheckpointRecoveryOpen> {
        Err(LimboError::InvalidArgument(
            "checkpoint recovery selection is not implemented".into(),
        ))
    }
}

#[allow(dead_code)]
impl CheckpointRecoveryOpen {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<CheckpointRecoverySelection> {
        Err(
            LimboError::InvalidArgument("checkpoint recovery selection is not implemented".into())
                .into(),
        )
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn checkpoint_recovery_entry_native_reselects_intent_without_database_open() {
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
        let before = std::fs::read(fixture.namespace.join("manifest")).unwrap();
        drop(fixture.prepared);
        drop(fixture.token);
        drop(fixture.pause);
        let mut ordinary = runtime.begin_open().unwrap();
        let failure = fixture.io.block(|| ordinary.poll_manifest()).unwrap_err();
        assert!(
            matches!(failure, LimboError::InvalidArgument(message) if message.contains("recovery is required"))
        );
        drop(ordinary);
        for _ in 0..2 {
            let mut recovery = runtime.begin_checkpoint_recovery().unwrap();
            let selection = fixture.io.block(|| recovery.poll()).unwrap();
            assert!(selection.manifest.record().checkpoint_intent().is_some());
            assert!(selection
                .manifest
                .guard()
                .unwrap()
                .live_registrations()
                .unwrap()
                .is_empty());
            assert!(matches!(
                runtime.files.authority.try_acquire(),
                Err(LimboError::Busy)
            ));
            assert!(matches!(
                runtime.gate.try_enter_admission(),
                Err(LimboError::Busy)
            ));
            assert!(recovery.poll().is_err());
            assert_eq!(
                std::fs::read(fixture.namespace.join("manifest")).unwrap(),
                before
            );
            drop(selection);
            assert!(runtime.gate.try_enter_admission().is_ok());
            runtime
                .files
                .authority
                .try_acquire()
                .unwrap()
                .finish()
                .unwrap();
        }
    }
}
