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
    physical_layout: InstalledPhysicalLayout,
    _pause: CheckpointPause,
}
impl InstalledDowngradeSource {
    pub(in crate::mvcc) fn validate(&self, loaded: &LoadedManifest) -> Result<()> {
        if !loaded.belongs_to_file(&self.files.manifest()) {
            return Err(LimboError::InvalidArgument(
                "downgrade manifest is not the retained synchronous alias".into(),
            ));
        }
        if loaded.record().requires_recovery() {
            return Err(LimboError::InvalidArgument(
                "downgrade source is not a normal checkpoint".into(),
            ));
        }
        self.validate_metadata(loaded.guard()?, loaded.record())?;
        self.files.revalidate()
    }
    fn validate_metadata(
        &self,
        guard: &AuthorityGuard,
        record: crate::mvcc::commit_manifest::ManifestRecord,
    ) -> Result<()> {
        let target = self.proof.target_identity();
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
        Ok(())
    }
    pub(in crate::mvcc) fn validate_marker(
        &self,
        guard: &AuthorityGuard,
        record: crate::mvcc::commit_manifest::ManifestRecord,
    ) -> Result<()> {
        self.validate_metadata(guard, record)?;
        if record
            .downgrade_intent()
            .is_none_or(|intent| intent.basename_wal_id != self.files.wal_id())
        {
            return Err(LimboError::InvalidArgument(
                "downgrade lost its terminal WAL identity".into(),
            ));
        }
        self.files.revalidate_marker()?;
        self.files.require_mirrored(record)
    }
    pub(in crate::mvcc) fn files(&self) -> &DowngradeFiles {
        &self.files
    }
    pub(in crate::mvcc) fn validate_physical_layout(
        &self,
        header: &DatabaseHeader,
        bytes: u64,
    ) -> Result<()> {
        self.physical_layout.validate(header, bytes)
    }
    pub(in crate::mvcc) fn physical_floor(&self) -> i64 {
        self.proof.materialized_floor()
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
                physical_layout: self.physical_layout,
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
        installed_fixture_with_tail(platform, false)
    }
    fn installed_fixture_with_tail(platform: bool, extra_tail: bool) -> Fixture {
        let fixture =
            crate::mvcc::database::shared_runtime::shared_completed_checkpoint_fixture_with_tail(
                |_| {
                    if platform {
                        Arc::new(crate::io::PlatformIO::new().unwrap())
                    } else {
                        Arc::new(crate::io::SyscallIO::new().unwrap())
                    }
                },
                extra_tail,
            );
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
        for case in ["nonempty", "alias", "ordinary-log", "ordinary-coordination"] {
            let mut fixture = installed_fixture();
            let wal = PathBuf::from(format!("{}-wal", fixture.path.display()));
            match case {
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
        let file =
            crate::IO::open_file(&memory, "substitute", crate::OpenFlags::Create, false).unwrap();
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
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_marker_native_uses_real_wal_transaction_on_same_inode() {
        use std::os::unix::fs::MetadataExt;
        let mut fixture = installed_fixture();
        let original_inode = std::fs::metadata(&fixture.path).unwrap().ino();
        let mut preparing = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap();
        let intent = fixture.io.block(|| preparing.poll()).unwrap();
        let manifest = std::fs::read(fixture.namespace.join("manifest")).unwrap();
        let marker = crate::mvcc::mode_transition::downgrade_marker::commit_marker(intent)
            .expect("genuine terminal intent must commit the ordinary WAL marker");
        let main = std::fs::read(&fixture.path).unwrap();
        assert_eq!(&main[18..20], &[2, 2]);
        assert_eq!(
            std::fs::metadata(&fixture.path).unwrap().ino(),
            original_inode
        );
        assert_eq!(
            std::fs::read(fixture.namespace.join("manifest")).unwrap(),
            manifest
        );
        assert!(fixture.namespace.join("ids-selector").exists());
        assert!(std::fs::read(format!("{}-wal", fixture.path.display()))
            .unwrap()
            .is_empty());
        assert!(matches!(
            fixture.io.open_file(
                fixture.path.to_str().unwrap(),
                crate::OpenFlags::None,
                false
            ),
            Err(LimboError::LockingError(_))
        ));
        assert!(fixture.runtime.replay.lock().is_none());
        drop(marker);
        let db = Database::open(
            fixture.io.clone(),
            fixture.path.to_str().unwrap(),
            OpenOptions::new(Arc::new(EngineDialect)),
        )
        .unwrap();
        let connection = db.connect(crate::ConnectOpts::new()).unwrap();
        let mut rows = Vec::new();
        connection
            .prepare("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                rows.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, ["materialized shared row"]);
        let mut integrity = Vec::new();
        connection
            .prepare("PRAGMA integrity_check")
            .unwrap()
            .run_with_row_callback(|row| {
                integrity.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(integrity, ["ok"]);
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_marker_native_rejects_changed_wal_without_recreation() {
        for missing in [true, false] {
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
            let intent = fixture.io.block(|| preparing.poll()).unwrap();
            let wal = PathBuf::from(format!("{}-wal", fixture.path.display()));
            std::fs::rename(&wal, fixture.path.with_extension("retained-wal")).unwrap();
            if !missing {
                std::fs::write(&wal, b"replacement must survive").unwrap();
            }
            let before = unchanged_files(&fixture);
            let wal_before = std::fs::read(&wal).ok();
            assert!(crate::mvcc::mode_transition::downgrade_marker::commit_marker(intent).is_err());
            assert_unchanged(&before);
            assert_eq!(std::fs::read(&wal).ok(), wal_before);
        }
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_marker_native_rejects_changed_installed_extent() {
        use std::os::unix::fs::FileExt;
        for append in [true, false] {
            let mut fixture = installed_fixture();
            let layout = fixture.installed.as_ref().unwrap().physical_layout;
            assert!(layout.pages > 1);
            let mut preparing = fixture
                .installed
                .take()
                .unwrap()
                .begin_downgrade(
                    fixture.connection.clone(),
                    OfflinePrerequisite::AllOlderWorkersStopped,
                )
                .unwrap();
            let intent = fixture.io.block(|| preparing.poll()).unwrap();
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&fixture.path)
                .unwrap();
            if append {
                file.set_len(layout.bytes + u64::from(layout.page_size))
                    .unwrap();
            } else {
                file.write_all_at(&(layout.pages - 1).to_be_bytes(), 28)
                    .unwrap();
            }
            let before = unchanged_files(&fixture);
            let wal = PathBuf::from(format!("{}-wal", fixture.path.display()));
            let wal_before = std::fs::read(&wal).unwrap();
            let result = crate::mvcc::mode_transition::downgrade_marker::commit_marker(intent);
            assert!(
                result.is_err(),
                "changed installed extent must reject before marker publication"
            );
            assert_unchanged(&before);
            assert_eq!(std::fs::read(&wal).unwrap(), wal_before);
        }
    }
    #[test]
    #[ignore = "native subprocess lock-probe helper"]
    fn shared_downgrade_lock_child() {
        assert_ne!(
            std::env::var("TURSO_DOWNGRADE_LOCK_FAIL").as_deref(),
            Ok("1"),
            "deliberate child failure control"
        );
        let path = std::env::var("TURSO_DOWNGRADE_LOCK_PATH").unwrap();
        let io = crate::io::SyscallIO::new().unwrap();
        let result = crate::IO::open_file(&io, &path, crate::OpenFlags::None, false);
        assert!(
            matches!(result, Err(LimboError::LockingError(_))),
            "ordinary owner must still exclude the child after old descriptors close"
        );
    }
    fn assert_child_excluded(path: &std::path::Path) {
        assert_child_excluded_with_failure(path, false);
    }
    fn assert_child_excluded_with_failure(path: &std::path::Path, fail: bool) {
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                &format!(
                    "{}::shared_downgrade_lock_child",
                    module_path!()
                        .strip_prefix("turso_core::")
                        .unwrap_or(module_path!())
                ),
                "--nocapture",
            ])
            .env("TURSO_DOWNGRADE_LOCK_PATH", path)
            .env("TURSO_DOWNGRADE_LOCK_FAIL", if fail { "1" } else { "0" })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("lock probe exceeded its deadline");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains("test result: ok. 1 passed; 0 failed;"),
            "lock child must execute exactly one test: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            output.status.success(),
            "child lock probe failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_marker_native_old_handle_close_preserves_ordinary_lock() {
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
        let intent = fixture.io.block(|| preparing.poll()).unwrap();
        let marker = crate::mvcc::mode_transition::downgrade_marker::commit_marker(intent).unwrap();
        drop(marker);
        drop(preparing);
        let db = Database::open(
            fixture.io.clone(),
            fixture.path.to_str().unwrap(),
            OpenOptions::new(Arc::new(EngineDialect)),
        )
        .unwrap();
        let connection = db.connect(crate::ConnectOpts::new()).unwrap();
        connection.execute("SELECT value FROM preserved").unwrap();
        assert_child_excluded(&fixture.path);
        let old_main = Arc::downgrade(&fixture.runtime.files.main);
        let old_runtime = Arc::downgrade(&fixture.runtime);
        let old_database = Arc::downgrade(&fixture.connection.db);
        drop(fixture.connection);
        drop(fixture.runtime);
        assert!(
            old_database.upgrade().is_none(),
            "old shared database did not retire"
        );
        assert!(
            old_runtime.upgrade().is_none(),
            "old shared runtime did not retire"
        );
        assert!(
            old_main.upgrade().is_none(),
            "old native main handle did not retire"
        );
        assert_child_excluded(&fixture.path);
        connection.execute("SELECT value FROM preserved").unwrap();
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_creation_native_missing_wal_is_created_before_intent() {
        use std::os::unix::fs::MetadataExt;
        let mut fixture = installed_fixture();
        let wal = PathBuf::from(format!("{}-wal", fixture.path.display()));
        std::fs::remove_file(&wal).unwrap();
        let before = unchanged_files(&fixture);
        let mut preparing = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .expect("genuine installed checkpoint must durably create its missing basename WAL");
        assert_unchanged(&before);
        let metadata = std::fs::symlink_metadata(&wal).unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 0);
        assert_eq!(metadata.mode() & 0o777, 0o600);
        let expected = crate::io::FileId {
            dev: metadata.dev(),
            ino: metadata.ino(),
        };
        let intent = fixture.io.block(|| preparing.poll()).unwrap();
        assert_eq!(
            intent.record().downgrade_intent().unwrap().basename_wal_id,
            expected
        );
        assert_eq!(std::fs::metadata(&wal).unwrap().ino(), expected.ino);
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_creation_native_invalid_sidecars_do_not_create_wal() {
        for coordination in [false, true] {
            let mut fixture = installed_fixture();
            let wal = PathBuf::from(format!("{}-wal", fixture.path.display()));
            std::fs::remove_file(&wal).unwrap();
            let invalid = if coordination {
                PathBuf::from(format!("{}-tshm", fixture.path.display()))
            } else {
                fixture.path.with_extension("db-log")
            };
            std::fs::write(&invalid, b"unrelated content").unwrap();
            let before = unchanged_files(&fixture);
            assert!(fixture
                .installed
                .take()
                .unwrap()
                .begin_downgrade(
                    fixture.connection.clone(),
                    OfflinePrerequisite::AllOlderWorkersStopped,
                )
                .is_err());
            assert!(!wal.exists());
            assert_eq!(std::fs::read(&invalid).unwrap(), b"unrelated content");
            assert_unchanged(&before);
        }
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_resume_native_terminal_reopens_without_live_source() {
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
        let intent = fixture.io.block(|| preparing.poll()).unwrap();
        let namespace_bytes: Vec<_> = [
            "manifest",
            "logical",
            "physical-wal",
            "ids-a",
            "ids-b",
            "ids-selector",
        ]
        .into_iter()
        .map(|name| {
            (
                fixture.namespace.join(name),
                std::fs::read(fixture.namespace.join(name)).unwrap(),
            )
        })
        .collect();
        drop(intent);
        drop(preparing);
        drop(fixture.connection);
        drop(fixture.runtime);
        let completed = crate::mvcc::mode_transition::downgrade_resume::resume_marker(
            &fixture.path,
            fixture.limit,
            OfflinePrerequisite::AllOlderWorkersStopped,
        )
        .expect("fresh terminal downgrade must resume without a live installed source");
        assert_eq!(
            completed.recovered_mirror_syncs(),
            2,
            "fresh recovery must establish both terminal durability barriers"
        );
        assert_eq!(&std::fs::read(&fixture.path).unwrap()[18..20], &[2, 2]);
        assert_unchanged(&namespace_bytes);
        drop(completed);
        let ordinary = crate::Database::open(
            fixture.io.clone(),
            fixture.path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
        )
        .unwrap();
        let connection = ordinary.connect(crate::ConnectOpts::new()).unwrap();
        let mut count = 0;
        connection
            .prepare("PRAGMA integrity_check")
            .unwrap()
            .run_with_row_callback(|row| {
                assert_eq!(row.get::<String>(0)?, "ok");
                count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(count, 1);
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_resume_native_rejects_unproven_files_without_writes() {
        for case in ["missing-wal", "replaced-wal", "invalid-ids"] {
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
            let intent = fixture.io.block(|| preparing.poll()).unwrap();
            drop(intent);
            drop(preparing);
            let wal = PathBuf::from(format!("{}-wal", fixture.path.display()));
            if case == "invalid-ids" {
                std::fs::write(fixture.namespace.join("ids-selector"), []).unwrap();
            } else {
                std::fs::rename(&wal, fixture.path.with_extension("retained-wal")).unwrap();
                if case == "replaced-wal" {
                    std::fs::write(&wal, []).unwrap();
                }
            }
            let before = unchanged_files(&fixture);
            let wal_before = std::fs::read(&wal).ok();
            drop(fixture.connection);
            drop(fixture.runtime);
            assert!(
                crate::mvcc::mode_transition::downgrade_resume::resume_marker(
                    &fixture.path,
                    fixture.limit,
                    OfflinePrerequisite::AllOlderWorkersStopped,
                )
                .is_err(),
                "accepted {case}"
            );
            assert_unchanged(&before);
            assert_eq!(std::fs::read(&wal).ok(), wal_before);
        }
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_resume_native_restores_single_terminal_copy() {
        use std::os::unix::fs::FileExt;
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
        let intent = fixture.io.block(|| preparing.poll()).unwrap();
        let expected = intent.record();
        drop(intent);
        drop(preparing);
        let manifest = fixture.namespace.join("manifest");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&manifest)
            .unwrap();
        file.write_all_at(&[0; RECORD_SIZE], 0).unwrap();
        file.sync_all().unwrap();
        drop(file);
        drop(fixture.connection);
        drop(fixture.runtime);
        let completed = crate::mvcc::mode_transition::downgrade_resume::resume_marker(
            &fixture.path,
            fixture.limit,
            OfflinePrerequisite::AllOlderWorkersStopped,
        )
        .unwrap();
        let bytes = std::fs::read(manifest).unwrap();
        for offset in [0, SLOT_SIZE] {
            assert_eq!(
                ManifestRecord::decode(&bytes[offset..offset + RECORD_SIZE]).unwrap(),
                Some(expected)
            );
        }
        assert_eq!(&std::fs::read(&fixture.path).unwrap()[18..20], &[2, 2]);
        drop(completed);
    }
    fn write_header_only_log(fixture: &Fixture) {
        let source_path = fixture._directory.path().join("header-source.db");
        let source = Database::open(
            fixture.io.clone(),
            source_path.to_str().unwrap(),
            OpenOptions::new(Arc::new(EngineDialect)),
        )
        .unwrap();
        let source_connection = source.connect(crate::ConnectOpts::new()).unwrap();
        source_connection
            .execute("PRAGMA journal_mode=mvcc")
            .unwrap();
        source_connection
            .execute("CREATE TABLE header_source(v)")
            .unwrap();
        let log = std::fs::read(source_path.with_extension("db-log")).unwrap();
        let header = &log[..crate::mvcc::persistent_storage::logical_log::LOG_HDR_SIZE];
        crate::mvcc::persistent_storage::logical_log::LogHeader::validate_header_only_file(header)
            .unwrap();
        std::fs::write(fixture.path.with_extension("db-log"), header).unwrap();
        drop(source_connection);
        drop(source);
    }
    #[test]
    #[ignore = "native downgrade process-death helper"]
    fn shared_downgrade_crash_child() {
        let mut fixture = installed_fixture();
        if std::env::var("TURSO_DOWNGRADE_CRASH_PHASE")
            .unwrap()
            .starts_with("cleanup-")
        {
            write_header_only_log(&fixture);
        }
        let location = std::env::var("TURSO_DOWNGRADE_CRASH_LOCATION").unwrap();
        std::fs::write(
            &location,
            format!("{}\n{}\n", fixture.path.display(), fixture.limit),
        )
        .unwrap();
        let mut preparing = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap();
        let intent = fixture.io.block(|| preparing.poll()).unwrap();
        crate::mvcc::mode_transition::downgrade_marker::commit_marker(intent)
            .unwrap()
            .cleanup()
            .unwrap();
        panic!("requested crash phase was not reached");
    }
    fn killed_downgrade(phase: &str) -> (tempfile::TempDir, PathBuf, u64) {
        use std::os::unix::process::ExitStatusExt;
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};
        struct KillChild(std::process::Child);
        impl Drop for KillChild {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let ready = directory.path().join("ready");
        let location = directory.path().join("location");
        let mut child = KillChild(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    &format!(
                        "{}::shared_downgrade_crash_child",
                        module_path!()
                            .strip_prefix("turso_core::")
                            .unwrap_or(module_path!())
                    ),
                    "--nocapture",
                ])
                .env("TMPDIR", directory.path())
                .env("TURSO_DOWNGRADE_CRASH_PHASE", phase)
                .env("TURSO_DOWNGRADE_CRASH_READY", &ready)
                .env("TURSO_DOWNGRADE_CRASH_LOCATION", &location)
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        while !ready.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before {phase}"
            );
            assert!(Instant::now() < deadline, "child did not reach {phase}");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(std::fs::read_to_string(&ready).unwrap(), phase);
        child.0.kill().unwrap();
        assert_eq!(child.0.wait().unwrap().signal(), Some(9));
        let location = std::fs::read_to_string(location).unwrap();
        let mut lines = location.lines();
        let path = PathBuf::from(lines.next().unwrap());
        let limit = lines.next().unwrap().parse().unwrap();
        (directory, path, limit)
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem and subprocess signals"]
    fn shared_downgrade_resume_native_sigkill_marker_phases_preserve_data() {
        for phase in ["intent", "commit", "checkpoint"] {
            let (_directory, path, limit) = killed_downgrade(phase);
            let completed = crate::mvcc::mode_transition::downgrade_resume::resume_marker(
                &path,
                limit,
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap_or_else(|error| panic!("resume after {phase}: {error}"));
            assert_eq!(&std::fs::read(&path).unwrap()[18..20], &[2, 2]);
            drop(completed);
            let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
            let db = Database::open(
                io,
                path.to_str().unwrap(),
                OpenOptions::new(Arc::new(EngineDialect)),
            )
            .unwrap();
            let connection = db.connect(crate::ConnectOpts::new()).unwrap();
            let mut rows = Vec::new();
            connection
                .prepare("SELECT value FROM preserved WHERE id=1")
                .unwrap()
                .run_with_row_callback(|row| {
                    rows.push(row.get::<String>(0)?);
                    Ok(())
                })
                .unwrap();
            assert_eq!(rows, ["materialized shared row"]);
            let mut integrity = Vec::new();
            connection
                .prepare("PRAGMA integrity_check")
                .unwrap()
                .run_with_row_callback(|row| {
                    integrity.push(row.get::<String>(0)?);
                    Ok(())
                })
                .unwrap();
            assert_eq!(integrity, ["ok"]);
        }
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_cleanup_native_removes_namespace_and_header_only_log() {
        let mut fixture = installed_fixture();
        write_header_only_log(&fixture);
        let mut preparing = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap();
        let intent = fixture.io.block(|| preparing.poll()).unwrap();
        let completed =
            crate::mvcc::mode_transition::downgrade_marker::commit_marker(intent).unwrap();
        completed
            .cleanup()
            .expect("durable marker must clean namespace and retained header-only ordinary log");
        assert!(!fixture.namespace.exists());
        assert!(!fixture.path.with_extension("db-log").exists());
        let db = Database::open(
            fixture.io.clone(),
            fixture.path.to_str().unwrap(),
            OpenOptions::new(Arc::new(EngineDialect)),
        )
        .unwrap();
        let connection = db.connect(crate::ConnectOpts::new()).unwrap();
        let mut rows = Vec::new();
        connection
            .prepare("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                rows.push(row.get::<String>(0)?);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, ["materialized shared row"]);
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_extent_native_accepts_genuine_installed_unused_tail() {
        let mut fixture = installed_fixture_with_tail(false, true);
        let header = fixture
            .connection
            .get_pager()
            .io
            .block(|| fixture.connection.get_pager().with_header(|header| *header))
            .unwrap();
        let bytes = std::fs::metadata(&fixture.path).unwrap().len();
        assert!(bytes > u64::from(header.page_size.get()) * u64::from(header.database_size.get()));
        assert!(
            fixture
                .installed
                .as_ref()
                .unwrap()
                .proof
                .materialized_floor()
                <= -i64::try_from(bytes / u64::from(header.page_size.get())).unwrap()
        );
        let mut preparing = fixture
            .installed
            .take()
            .unwrap()
            .begin_downgrade(
                fixture.connection.clone(),
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap();
        let intent = fixture.io.block(|| preparing.poll()).unwrap();
        drop(intent);
        drop(preparing);
        drop(fixture.connection);
        drop(fixture.runtime);
        let completed = crate::mvcc::mode_transition::downgrade_resume::resume_marker(
            &fixture.path,
            fixture.limit,
            OfflinePrerequisite::AllOlderWorkersStopped,
        )
        .expect("fresh downgrade must accept the unchanged genuine installed physical extent");
        drop(completed);
        let db = Database::open(
            fixture.io.clone(),
            fixture.path.to_str().unwrap(),
            OpenOptions::new(Arc::new(EngineDialect)),
        )
        .unwrap();
        let connection = db.connect(crate::ConnectOpts::new()).unwrap();
        let mut rows = Vec::new();
        connection
            .prepare("PRAGMA integrity_check")
            .unwrap()
            .run_with_row_callback(|row| {
                rows.push(row.get::<String>(0)?);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, ["ok"]);
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_downgrade_cleanup_native_rejects_unknown_or_replaced_files_before_unlink() {
        for replacement in [false, true] {
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
            let intent = fixture.io.block(|| preparing.poll()).unwrap();
            let completed =
                crate::mvcc::mode_transition::downgrade_marker::commit_marker(intent).unwrap();
            let changed = if replacement {
                let path = fixture.namespace.join("ids-a");
                std::fs::rename(&path, fixture._directory.path().join("original-ids-a")).unwrap();
                std::fs::write(&path, b"unrelated replacement").unwrap();
                path
            } else {
                let path = fixture.namespace.join("unexpected");
                std::fs::write(&path, b"unrelated addition").unwrap();
                path
            };
            let before = unchanged_files(&fixture);
            let changed_bytes = std::fs::read(&changed).unwrap();
            let error = completed
                .cleanup()
                .err()
                .expect("unsafe cleanup must reject");
            assert!(error
                .to_string()
                .contains("database is ordinary WAL; downgrade cleanup remains incomplete"));
            assert_unchanged(&before);
            assert_eq!(std::fs::read(changed).unwrap(), changed_bytes);
            assert_eq!(
                std::fs::read_dir(&fixture.namespace).unwrap().count(),
                if replacement { 8 } else { 9 }
            );
        }
    }
    #[test]
    #[ignore = "requires native Linux ext-family filesystem and subprocess signals"]
    fn shared_downgrade_partial_native_sigkill_cleanup_resumes_without_recreating_files() {
        for phase in [
            "cleanup-start",
            "cleanup-log",
            "cleanup-logical",
            "cleanup-authority",
            "cleanup-ids-a",
            "cleanup-manifest",
        ] {
            let (_directory, path, _limit) = killed_downgrade(phase);
            let main = std::fs::read(&path).unwrap();
            assert_eq!(&main[18..20], &[2, 2]);
            crate::mvcc::mode_transition::downgrade_cleanup::resume_cleanup(
                &path,
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .expect(
                "marker2 partial cleanup must resume without recreating missing namespace files",
            );
            assert_eq!(std::fs::read(&path).unwrap(), main);
            assert!(!path.with_extension("db-log").exists());
            let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
            let db = Database::open(
                io,
                path.to_str().unwrap(),
                OpenOptions::new(Arc::new(EngineDialect)),
            )
            .unwrap();
            let connection = db.connect(crate::ConnectOpts::new()).unwrap();
            let mut rows = Vec::new();
            connection
                .prepare("SELECT value FROM preserved WHERE id=1")
                .unwrap()
                .run_with_row_callback(|row| {
                    rows.push(row.get::<String>(0)?);
                    Ok(())
                })
                .unwrap();
            assert_eq!(rows, ["materialized shared row"]);
        }
    }
    #[test]
    #[ignore = "requires native Linux subprocess support"]
    fn shared_downgrade_marker_native_child_failure_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unused-control.db");
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_child_excluded_with_failure(&path, true);
        }));
        assert!(
            rejected.is_err(),
            "supervisor accepted deliberately failing child"
        );
    }
}
