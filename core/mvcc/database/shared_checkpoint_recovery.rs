use super::*;
use crate::mvcc::checkpoint_gate::CheckpointPause;

#[allow(dead_code)]
pub(in crate::mvcc) struct CheckpointRecoverySelection {
    pub runtime: Arc<SharedMvccRuntime>,
    pub manifest: LoadedManifest,
    pub pause: CheckpointPause,
}

#[allow(dead_code)]
pub(in crate::mvcc) struct CheckpointRecoveryOpen {
    runtime: Arc<SharedMvccRuntime>,
    load: Option<ManifestLoad>,
    pause: Option<CheckpointPause>,
}

impl SharedMvccRuntime {
    #[allow(dead_code)]
    pub(in crate::mvcc) fn begin_checkpoint_recovery(
        self: &Arc<Self>,
    ) -> Result<CheckpointRecoveryOpen> {
        let pause = self.gate.try_pause_admission()?;
        let guard = self.files.authority.try_acquire()?;
        guard.ensure_no_snapshot_locks()?;
        let load = ManifestLoad::new(self.files.manifest.clone(), guard, self.database_id)?;
        Ok(CheckpointRecoveryOpen {
            runtime: self.clone(),
            load: Some(load),
            pause: Some(pause),
        })
    }
}

#[allow(dead_code)]
impl CheckpointRecoveryOpen {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<CheckpointRecoverySelection> {
        let result = self.poll_inner();
        if result.is_err() {
            self.load = None;
            self.pause = None;
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<CheckpointRecoverySelection> {
        let load = self.load.as_mut().ok_or_else(|| {
            LimboError::InvalidArgument("checkpoint recovery selection has ended".into())
        })?;
        let manifest = return_if_io!(load.poll());
        self.load = None;
        if manifest.record().downgrade_intent().is_some() {
            return Err(LimboError::InvalidArgument(
                "checkpoint recovery cannot resume a downgrade".into(),
            )
            .into());
        }
        manifest.guard()?.ensure_no_snapshot_locks()?;
        Ok(IOResult::Done(CheckpointRecoverySelection {
            runtime: self.runtime.clone(),
            manifest,
            pause: self.pause.take().unwrap(),
        }))
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
            selection
                .manifest
                .guard()
                .unwrap()
                .ensure_no_snapshot_locks()
                .unwrap();
            assert!(selection.manifest.guard().unwrap().requires_recovery());
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
            drop(runtime.files.authority.try_acquire().unwrap());
        }
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn checkpoint_recovery_entry_native_busy_preserves_reader_and_releases_pause() {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let database = crate::Database::open(
            io.clone(),
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        let connection = database.connect(crate::ConnectOpts::new()).unwrap();
        let store = database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap();
        connection
            .execute("BEGIN; SELECT value FROM preserved")
            .unwrap();
        let before = std::fs::read(runtime.files.namespace.join("manifest")).unwrap();
        assert!(matches!(
            runtime.begin_checkpoint_recovery(),
            Err(LimboError::Busy)
        ));
        assert!(runtime.gate.try_enter_admission().is_ok());
        let mut values = Vec::new();
        connection
            .prepare("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                values.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(values, ["durable payload"]);
        assert_eq!(
            std::fs::read(runtime.files.namespace.join("manifest")).unwrap(),
            before
        );
        connection.execute("ROLLBACK").unwrap();
        let mut recovery = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| recovery.poll()).unwrap();
        assert!(selected.manifest.record().checkpoint_intent().is_none());
        assert_eq!(
            std::fs::read(runtime.files.namespace.join("manifest")).unwrap(),
            before
        );
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn checkpoint_recovery_entry_native_invalid_manifest_releases_ownership_for_retry() {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let database = crate::Database::open(
            io.clone(),
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        let store = database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap();
        let manifest_path = runtime.files.namespace.join("manifest");
        let original = std::fs::read(&manifest_path).unwrap();
        let mut selection = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| selection.poll()).unwrap();
        let downgrade = selected
            .manifest
            .record()
            .begin_downgrade(runtime.files.physical_wal.file_id().unwrap().unwrap())
            .unwrap()
            .encode();
        drop(selected);
        for (bytes, downgrade) in [(b"bad".as_slice(), false), (downgrade.as_slice(), true)] {
            let guard = runtime.files.authority.try_acquire().unwrap();
            std::fs::write(&manifest_path, bytes).unwrap();
            drop(guard);
            let mut recovery = runtime.begin_checkpoint_recovery().unwrap();
            let error = match io.block(|| recovery.poll()) {
                Err(error) => error,
                Ok(_) => panic!("invalid recovery manifest was accepted"),
            };
            if downgrade {
                assert!(
                    matches!(error, LimboError::InvalidArgument(message) if message.contains("cannot resume a downgrade"))
                );
            } else {
                assert!(matches!(error, LimboError::Corrupt(_)));
            }
            assert!(recovery.poll().is_err());
            assert!(runtime.gate.try_enter_admission().is_ok());
            let guard = runtime.files.authority.try_acquire().unwrap();
            assert!(guard.requires_recovery());
            assert_eq!(std::fs::read(&manifest_path).unwrap(), bytes);
            std::fs::write(&manifest_path, &original).unwrap();
            drop(guard);
            let mut retry = runtime.begin_checkpoint_recovery().unwrap();
            let selected = io.block(|| retry.poll()).unwrap();
            assert!(!selected.manifest.record().requires_recovery());
            assert!(selected.manifest.guard().unwrap().requires_recovery());
        }
    }
}
