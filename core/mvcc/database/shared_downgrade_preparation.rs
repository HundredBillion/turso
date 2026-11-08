use super::*;
use crate::mvcc::commit_manifest::downgrade_publication::{
    DowngradeIntentPublication, MirroredDowngradeIntent,
};
use crate::mvcc::mode_transition::{downgrade_files::DowngradeFiles, OfflinePrerequisite};

// Only consuming the installed checkpoint can produce this publication capability.
// Keep the admission pause last so failure never admits work before ownership retires.
pub(in crate::mvcc) struct InstalledDowngradeSource {
    proof: CompletedCheckpointProof,
    connection: Arc<Connection>,
    runtime: Arc<SharedMvccRuntime>,
    schema: Arc<Schema>,
    replay: Arc<SharedReplayContext>,
    files: DowngradeFiles,
    _pause: CheckpointPause,
}
impl InstalledDowngradeSource {
    pub(in crate::mvcc) fn validate(&self, loaded: &LoadedManifest) -> Result<()> {
        let guard = loaded.guard()?;
        let target = self.proof.target_identity();
        let record = loaded.record();
        let store = self
            .connection
            .db
            .get_mv_store()
            .as_ref()
            .cloned()
            .ok_or_else(|| LimboError::InvalidArgument("downgrade lost installed store".into()))?;
        if !self.runtime.owns_authority(guard)
            || !self.proof.matches_lease(guard)
            || guard.published()? != target.prefix
            || !guard.incarnation_ready()?
            || record.requires_recovery()
            || record.database_id() != self.runtime.database_id
            || record.prefix() != target.prefix
            || record.log_generation() != target.database.generation
            || record.log_salt() != 0
            || self.runtime.replay.lock().is_some()
            || !self.replay.matches_oracle_identity(target)
            || !Arc::ptr_eq(&self.schema, &self.connection.db.schema.lock())
            || !Arc::ptr_eq(&self.schema, &self.connection.schema.read())
            || store
                .shared_runtime
                .get()
                .is_none_or(|runtime| !Arc::ptr_eq(runtime, &self.runtime))
            || store.last_committed_tx_ts.load(Ordering::Acquire)
                != target.prefix.committed_timestamp
            || store.durable_txid_max.load(Ordering::Acquire)
                != target.database.checkpoint_timestamp
        {
            return Err(LimboError::InvalidArgument(
                "downgrade no longer matches installed checkpoint".into(),
            ));
        }
        if !guard.io_retired() || !guard.live_registrations()?.is_empty() || !store.txs.is_empty() {
            return Err(LimboError::Busy);
        }
        let logical: Arc<dyn File> = self.runtime.logical.clone();
        let storage = self.runtime.storage.shared_commit_snapshot(&logical)?;
        if storage.offset != 0 || storage.running_crc != 0 || storage.header.is_some() {
            return Err(LimboError::InvalidArgument(
                "downgrade append state changed".into(),
            ));
        }
        self.files.revalidate()
    }
    pub(in crate::mvcc) fn wal_id(&self) -> crate::io::FileId {
        self.files.wal_id()
    }
}

pub(in crate::mvcc) struct SharedDowngradePreparation {
    load: Option<ManifestLoad>,
    source: Option<InstalledDowngradeSource>,
    publication: Option<DowngradeIntentPublication>,
    terminated: bool,
}
impl InstalledLocalCheckpoint {
    pub(in crate::mvcc) fn begin_downgrade(
        self,
        connection: Arc<Connection>,
        _: OfflinePrerequisite,
    ) -> Result<SharedDowngradePreparation> {
        let runtime = self.validate_handoff(&connection)?;
        runtime.certify_completed_pager(&connection, &self.proof, &self.guard)?;
        let files =
            DowngradeFiles::open(std::path::Path::new(&connection.db.path), &runtime.files)?;
        let load = ManifestLoad::new(files.manifest(), self.guard, runtime.database_id)?;
        Ok(SharedDowngradePreparation {
            load: Some(load),
            source: Some(InstalledDowngradeSource {
                proof: self.proof,
                connection,
                runtime,
                schema: self.schema,
                replay: self.replay,
                files,
                _pause: self.pause,
            }),
            publication: None,
            terminated: false,
        })
    }
}
impl SharedDowngradePreparation {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<MirroredDowngradeIntent> {
        if self.terminated {
            return Err(LimboError::InvalidArgument("downgrade preparation ended".into()).into());
        }
        let result = self.poll_inner();
        if result.is_err() {
            self.terminated = true;
            self.load = None;
            self.publication = None;
            self.source = None;
        }
        result
    }
    fn poll_inner(&mut self) -> IOResultOr<MirroredDowngradeIntent> {
        if let Some(load) = self.load.as_mut() {
            let loaded = return_if_io!(load.poll());
            // The transition uses synchronous native aliases; drop callback ownership
            // before checking retirement and handing the same lease to publication.
            self.load = None;
            self.publication = Some(DowngradeIntentPublication::from_installed(
                loaded,
                self.source.take().unwrap(),
            )?);
        }
        match self.publication.as_mut().unwrap().poll()? {
            IOResult::IO(waiter) => Ok(IOResult::IO(waiter)),
            IOResult::Done(proof) => {
                self.terminated = true;
                self.publication = None;
                Ok(IOResult::Done(proof))
            }
        }
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;
    use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE, SLOT_SIZE};
    use crate::{Database, EngineDialect, OpenOptions};

    struct Fixture {
        installed: Option<InstalledLocalCheckpoint>,
        runtime: Arc<SharedMvccRuntime>,
        connection: Arc<Connection>,
        io: Arc<dyn crate::IO>,
        path: PathBuf,
        namespace: PathBuf,
        limit: u64,
        _directory: tempfile::TempDir,
    }
    fn installed_fixture() -> Fixture {
        installed_fixture_with_io(false)
    }
    fn installed_fixture_with_io(platform: bool) -> Fixture {
        let fixture = shared_completed_checkpoint_fixture(|_| {
            if platform {
                Arc::new(crate::io::PlatformIO::new().unwrap())
            } else {
                Arc::new(crate::io::SyscallIO::new().unwrap())
            }
        });
        let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap().clone();
        let mut finalizer = fixture.ids.finalize(fixture.guard, fixture.proof).unwrap();
        let finalized = fixture.io.block(|| Ok(finalizer.poll()?)).unwrap();
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
        Fixture {
            installed: Some(installed),
            runtime,
            connection: fixture.connection,
            io: fixture.io,
            path: fixture.path,
            namespace: fixture.namespace,
            limit: fixture.limit,
            _directory: fixture.directory,
        }
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_prepare_native_consumes_genuine_installed_checkpoint() {
        use std::os::unix::fs::MetadataExt;
        let mut fixture = installed_fixture();
        let installed = fixture.installed.take().unwrap();
        let target = installed.proof.target_identity();
        let runtime = &fixture.runtime;
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
        let metadata = std::fs::metadata(&wal_path).unwrap();
        assert_eq!(
            proof.record().downgrade_intent().unwrap().basename_wal_id,
            crate::io::FileId {
                dev: metadata.dev(),
                ino: metadata.ino()
            }
        );
        assert!(matches!(
            fixture.io.open_file(
                fixture.path.to_str().unwrap(),
                crate::OpenFlags::None,
                false
            ),
            Err(LimboError::LockingError(_))
        ));
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
    }
    fn unchanged_files(fixture: &Fixture) -> Vec<(PathBuf, Vec<u8>)> {
        std::iter::once(fixture.path.clone())
            .chain(
                [
                    "manifest",
                    "logical",
                    "physical-wal",
                    "ids-a",
                    "ids-b",
                    "ids-selector",
                ]
                .map(|name| fixture.namespace.join(name)),
            )
            .map(|path| {
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect()
    }
    fn assert_unchanged(files: &[(PathBuf, Vec<u8>)]) {
        for (path, bytes) in files {
            assert_eq!(&std::fs::read(path).unwrap(), bytes, "{}", path.display());
        }
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_prepare_native_rejects_sidecars_without_writes() {
        for case in [
            "missing",
            "nonempty",
            "alias",
            "ordinary-log",
            "ordinary-coordination",
        ] {
            let mut fixture = installed_fixture();
            let wal = PathBuf::from(format!("{}-wal", fixture.path.display()));
            match case {
                "missing" => std::fs::remove_file(&wal).unwrap(),
                "nonempty" => std::fs::write(&wal, b"not empty").unwrap(),
                "alias" => {
                    std::fs::remove_file(&wal).unwrap();
                    std::fs::hard_link(&fixture.path, &wal).unwrap();
                }
                "ordinary-log" => {
                    std::fs::write(fixture.path.with_extension("db-log"), b"not a valid log")
                        .unwrap()
                }
                "ordinary-coordination" => std::fs::write(
                    format!("{}-tshm", fixture.path.display()),
                    b"owned by someone else",
                )
                .unwrap(),
                _ => unreachable!(),
            }
            let before = unchanged_files(&fixture);
            let wal_before = std::fs::read(&wal).ok();
            let result = fixture.installed.take().unwrap().begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            );
            assert!(result.is_err(), "accepted {case}");
            if case == "missing" {
                assert!(result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("existing empty basename WAL"));
            }
            assert_unchanged(&before);
            assert_eq!(std::fs::read(&wal).ok(), wal_before);
            assert!(fixture.runtime.replay.lock().is_none());
        }
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_prepare_native_revalidates_retained_wal_before_publish() {
        let mut fixture = installed_fixture();
        let mut preparation = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap();
        let wal = PathBuf::from(format!("{}-wal", fixture.path.display()));
        let old = fixture.path.with_extension("retained-wal");
        std::fs::rename(&wal, &old).unwrap();
        std::fs::write(&wal, []).unwrap();
        let before = unchanged_files(&fixture);
        let error = fixture
            .io
            .block(|| preparation.poll())
            .err()
            .expect("replaced WAL accepted");
        assert!(error.to_string().contains("identity changed"), "{error}");
        assert_unchanged(&before);
        assert_eq!(std::fs::read(&wal).unwrap(), Vec::<u8>::new());
        assert!(fixture.runtime.replay.lock().is_none());
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_prepare_native_rejects_foreign_connection_before_writes() {
        let mut fixture = installed_fixture();
        let foreign = crate::Database::open(
            fixture.io.clone(),
            ":memory:",
            crate::OpenOptions::new(Arc::new(EngineDialect)),
        )
        .unwrap()
        .connect(crate::ConnectOpts::new())
        .unwrap();
        let before = unchanged_files(&fixture);
        let error = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(foreign, OfflinePrerequisite::AllOlderWorkersStopped)
            .err()
            .expect("foreign connection accepted");
        assert!(error.to_string().contains("original connection"));
        assert_unchanged(&before);
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_prepare_native_accepts_retired_platform_runtime() {
        let mut fixture = installed_fixture_with_io(true);
        let before = unchanged_files(&fixture);
        let mut preparing = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap();
        let proof = fixture.io.block(|| preparing.poll()).unwrap();
        assert!(proof.record().downgrade_intent().is_some());
        for (path, bytes) in before {
            if path.file_name().unwrap() != "manifest" {
                assert_eq!(std::fs::read(path).unwrap(), bytes);
            }
        }
        assert!(fixture.runtime.replay.lock().is_none());
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_prepare_native_drop_before_publication_preserves_files() {
        let mut fixture = installed_fixture();
        let before = unchanged_files(&fixture);
        let preparing = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap();
        assert!(matches!(
            fixture.runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        drop(preparing);
        assert_unchanged(&before);
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(fixture.runtime.gate.try_enter_admission().is_ok());
        let guard = fixture.runtime.files.authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_prepare_native_rejects_manifest_backend_substitution() {
        let mut fixture = installed_fixture();
        let mut preparing = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap();
        let loaded = fixture
            .io
            .block(|| Ok(preparing.load.as_mut().unwrap().poll()?))
            .unwrap();
        preparing.load = None;
        let bytes = std::fs::read(fixture.namespace.join("manifest")).unwrap();
        let memory = crate::io::MemoryIO::new();
        let file = memory
            .open_file("substitute", crate::OpenFlags::Create, false)
            .unwrap();
        let buffer = Arc::new(crate::Buffer::new_temporary(bytes.len()));
        buffer.as_mut_slice().copy_from_slice(&bytes);
        drop(
            file.pwrite(0, buffer, Completion::new_write(|_| {}))
                .unwrap(),
        );
        let mut substitute = ManifestLoad::new(
            file,
            loaded.into_guard().unwrap(),
            fixture.runtime.database_id,
        )
        .unwrap();
        let loaded = memory.block(|| Ok(substitute.poll()?)).unwrap();
        drop(substitute);
        let before = unchanged_files(&fixture);
        let result =
            DowngradeIntentPublication::from_installed(loaded, preparing.source.take().unwrap());
        assert!(
            result.is_err(),
            "same bytes from another manifest backend must not certify offline publication"
        );
        assert_unchanged(&before);
    }
}
