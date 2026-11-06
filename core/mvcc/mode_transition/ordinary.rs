use super::{fs_error, identity, OfflineBackend, OfflinePrerequisite, SHARED_MVCC_VERSION};
use crate::io::FileSyncType;
use crate::storage::database::DatabaseFile;
use crate::sync::Arc;
use crate::util::IOExt;
use crate::{Connection, LimboError, OpenFlags, Result, IO};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub(super) struct OrdinaryOfflineBackend {
    path: PathBuf,
    connection: Arc<Connection>,
    main: Arc<dyn crate::io::File>,
    retained: Vec<std::fs::File>,
    materialized: Option<(u64, i64)>,
    #[cfg(test)]
    marker_hook: Option<Box<dyn FnMut(MarkerPoint)>>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum MarkerPoint {
    Committed,
    Checkpointed,
}

pub(super) fn resume_prepared_marker(
    path: &Path,
    history_limit: u64,
    prerequisite: OfflinePrerequisite,
) -> Result<()> {
    let proof = super::recovery::PreparedMarkerRecovery::open(path, history_limit, prerequisite)?;
    let db = crate::Database::open_offline_marker_recovery(proof.clone())?;
    if db
        .shared_wal
        .read()
        .runtime
        .frame_cache
        .lock()
        .keys()
        .any(|page| *page != 1)
    {
        return Err(LimboError::Corrupt(
            "prepared marker recovery contains unrelated WAL pages".into(),
        ));
    }
    let connection = db.connect(crate::ConnectOpts::new())?;
    connection.execute("PRAGMA synchronous=FULL")?;
    let pager = connection.get_pager();
    let physical_floor = pager
        .io
        .block(|| pager.with_header(|header| -i64::from(header.database_size.get().max(1))))?;
    if proof.logical_floor() > physical_floor {
        return Err(LimboError::Corrupt(
            "prepared ID floor does not cover materialized roots".into(),
        ));
    }
    let marker = pager
        .io
        .block(|| pager.with_header(|header| (header.read_version.0, header.write_version.0)))?;
    if !matches!(marker, (255, 255) | (254, 254)) {
        return Err(LimboError::Corrupt(
            "invalid recovered offline marker".into(),
        ));
    }
    let mut timestamp = None;
    connection
        .prepare("SELECT v FROM __turso_internal_mvcc_meta WHERE k='persistent_tx_ts_max'")?
        .run_with_row_callback(|row| {
            let value = row.get::<i64>(0)?;
            if value < 0
                || value as u64 >= crate::mvcc::shared_authority::MAX_TIMESTAMP
                || timestamp.is_some()
            {
                return Err(LimboError::Corrupt(
                    "invalid recovered offline timestamp".into(),
                ));
            }
            timestamp = Some(value as u64);
            Ok(())
        })?;
    if timestamp != Some(proof.timestamp()) {
        return Err(LimboError::Corrupt(
            "prepared namespace differs from materialized database".into(),
        ));
    }
    if marker.0 == 255 {
        connection.execute("BEGIN IMMEDIATE")?;
        pager.io.block(|| {
            pager.with_header_mut(|header| {
                header.read_version =
                    crate::storage::sqlite3_ondisk::RawVersion(SHARED_MVCC_VERSION);
                header.write_version =
                    crate::storage::sqlite3_ondisk::RawVersion(SHARED_MVCC_VERSION);
            })
        })?;
        connection.execute("COMMIT")?;
    }
    connection.checkpoint(crate::CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })?;
    connection.close()?;
    proof.revalidate()?;
    std::fs::File::open(proof.path())
        .map_err(fs_error)?
        .sync_all()
        .map_err(fs_error)?;
    if super::validate_shared_open(proof.path())? != proof.prefix() {
        return Err(LimboError::Corrupt(
            "resumed marker changed the prepared manifest".into(),
        ));
    }
    Ok(())
}

impl OrdinaryOfflineBackend {
    pub(super) fn create_staged(path: &Path) -> Result<Self> {
        super::require_linux_platform()?;
        let parent = super::canonical_parent(path)?;
        let directory = std::fs::File::open(&parent).map_err(fs_error)?;
        if !crate::mvcc::shared_files::filesystem_supported(
            crate::mvcc::shared_files::filesystem_magic(&directory)?,
        ) {
            return Err(LimboError::InvalidArgument(
                "fresh shared databases require native ext-family storage".into(),
            ));
        }
        let path =
            parent.join(path.file_name().ok_or_else(|| {
                LimboError::InvalidArgument("staging path needs a filename".into())
            })?);
        for sidecar in [
            PathBuf::from(format!("{}-wal", path_string(&path)?)),
            path.with_extension("db-log"),
            PathBuf::from(format!("{}-tshm", path_string(&path)?)),
        ] {
            match std::fs::symlink_metadata(sidecar) {
                Ok(_) => {
                    return Err(LimboError::InvalidArgument(
                        "fresh staging sidecar already exists".into(),
                    ))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(fs_error(error)),
            }
        }
        let native = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .map_err(fs_error)?;
        lock_exclusive(&native)?;
        let id = identity(&native)?;
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new()?);
        let main = io.open_file(path_string(&path)?, OpenFlags::NoLock, false)?;
        if main.file_id()? != Some(id) {
            return Err(LimboError::InvalidArgument(
                "staging identity changed".into(),
            ));
        }
        let connection = open_connection(&path, io, main)?;
        connection.execute("PRAGMA journal_mode=mvcc")?;
        connection.close()?;
        drop(connection);
        Self::open_retained(&path, native)
    }

    pub(super) fn open(path: &Path, _: OfflinePrerequisite) -> Result<Self> {
        super::require_linux_platform()?;
        let native = open_native(path)?;
        Self::open_retained(path, native)
    }

    fn open_retained(path: &Path, native: std::fs::File) -> Result<Self> {
        lock_exclusive(&native)?;
        let database_id = identity(&native)?;
        let path = path.canonicalize().map_err(fs_error)?;
        super::require_path_identity(&path, database_id)?;
        let marker = super::read_marker(&native)?;
        if marker != 255 {
            return Err(LimboError::InvalidArgument(
                "offline backend initially requires ordinary MVCC mode".into(),
            ));
        }
        super::require_marker(&native, marker)?;
        let mut retained = vec![native];
        let mut identities = vec![database_id];
        let wal_path = PathBuf::from(format!("{}-wal", path_string(&path)?));
        let log_path = path.with_extension("db-log");
        let tshm_path = PathBuf::from(format!("{}-tshm", path_string(&path)?));
        if tshm_path.try_exists().map_err(fs_error)? {
            return Err(LimboError::InvalidArgument(
                "offline upgrade does not support multiprocess WAL coordination".into(),
            ));
        }
        for sidecar in [&wal_path, &log_path] {
            match std::fs::symlink_metadata(sidecar) {
                Ok(_) => {
                    let file = open_native(sidecar)?;
                    let id = identity(&file)?;
                    if id.dev != database_id.dev || identities.contains(&id) {
                        return Err(LimboError::InvalidArgument("ordinary sidecars must have distinct identities on the database device".into()));
                    }
                    identities.push(id);
                    retained.push(file);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(fs_error(error)),
            }
        }
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new()?);
        let main = io.open_file(path_string(&path)?, OpenFlags::NoLock, false)?;
        if main.file_id()? != Some(database_id) {
            return Err(LimboError::InvalidArgument(
                "offline database identity changed".into(),
            ));
        }
        let connection = open_connection(&path, io, main.clone())?;
        Ok(Self {
            path,
            connection,
            main,
            retained,
            materialized: None,
            #[cfg(test)]
            marker_hook: None,
        })
    }
}

impl OfflineBackend for OrdinaryOfflineBackend {
    fn path(&self) -> &Path {
        &self.path
    }

    fn recover_materialize_sync(&mut self) -> Result<u64> {
        if let Some((timestamp, _)) = self.materialized {
            return Ok(timestamp);
        }
        let store = self.connection.mv_store().as_ref().cloned();
        let logical_floor = match store.as_ref() {
            Some(store) => store.recovered_logical_id_floor()?,
            None => -1,
        };
        self.connection
            .checkpoint(crate::CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })?;
        let pager = self.connection.get_pager();
        let physical_floor = pager
            .io
            .block(|| pager.with_header(|header| -i64::from(header.database_size.get().max(1))))?;
        let mut timestamp = None;
        if store.is_some() {
            self.connection
                .prepare("SELECT v FROM __turso_internal_mvcc_meta WHERE k='persistent_tx_ts_max'")?
                .run_with_row_callback(|row| {
                    let value = row.get::<i64>(0)?;
                    if value < 0
                        || value as u64 >= crate::mvcc::shared_authority::MAX_TIMESTAMP
                        || timestamp.is_some()
                    {
                        return Err(LimboError::Corrupt(
                            "invalid materialized MVCC timestamp".into(),
                        ));
                    }
                    timestamp = Some(value as u64);
                    Ok(())
                })?;
            if timestamp.is_none() {
                return Err(LimboError::Corrupt(
                    "missing materialized MVCC timestamp".into(),
                ));
            }
        }
        let completion = self
            .main
            .sync(crate::Completion::new_sync(|_| {}), FileSyncType::Fsync)?;
        pager.io.wait_for_completion(completion)?;
        // The final pager transaction is the only durable version change.
        self.connection.db.mv_store.store(None);
        drop(store);
        let timestamp = timestamp.unwrap_or(0);
        self.materialized = Some((timestamp, logical_floor.min(physical_floor)));
        Ok(timestamp)
    }

    fn materialized_logical_floor(&mut self) -> Result<i64> {
        self.materialized.map(|(_, floor)| floor).ok_or_else(|| {
            LimboError::InvalidArgument("offline database has not been materialized".into())
        })
    }

    fn commit_marker_checkpoint_sync(&mut self, marker: u8) -> Result<()> {
        if marker != SHARED_MVCC_VERSION || self.materialized.is_none() {
            return Err(LimboError::InvalidArgument(
                "ordinary backend only publishes a materialized shared upgrade".into(),
            ));
        }
        self.connection.execute("BEGIN IMMEDIATE")?;
        let pager = self.connection.get_pager();
        let result = (|| {
            pager.io.block(|| {
                pager.with_header_mut(|header| {
                    header.read_version = crate::storage::sqlite3_ondisk::RawVersion(marker);
                    header.write_version = crate::storage::sqlite3_ondisk::RawVersion(marker);
                })
            })?;
            self.connection.execute("COMMIT")?;
            #[cfg(test)]
            if let Some(hook) = self.marker_hook.as_mut() {
                hook(MarkerPoint::Committed);
            }
            self.connection
                .checkpoint(crate::CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                })?;
            #[cfg(test)]
            if let Some(hook) = self.marker_hook.as_mut() {
                hook(MarkerPoint::Checkpointed);
            }
            let completion = self
                .main
                .sync(crate::Completion::new_sync(|_| {}), FileSyncType::Fsync)?;
            pager.io.wait_for_completion(completion)
        })();
        if result.is_err() && !self.connection.get_auto_commit() {
            let _ = self.connection.execute("ROLLBACK");
        }
        result
    }
}

fn open_connection(
    path: &Path,
    io: Arc<dyn IO>,
    main: Arc<dyn crate::io::File>,
) -> Result<Arc<Connection>> {
    let options = crate::OpenOptions::new(Arc::new(crate::dialect::EngineDialect))
        .flags(OpenFlags::NoLock)
        .storage(Arc::new(DatabaseFile::new(main)));
    let mut state = crate::database::OpenDbAsyncState::new();
    let db = io.block(|| {
        crate::Database::do_open_async(&mut state, io.clone(), path_string(path)?, &options)
    })?;
    let connection = db.connect(crate::ConnectOpts::new())?;
    connection.execute("PRAGMA synchronous=FULL")?;
    Ok(connection)
}

fn path_string(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| LimboError::InvalidArgument("offline path must be UTF-8".into()))
}

pub(super) fn open_native(path: &Path) -> Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(fs_error)?;
    if !file.metadata().map_err(fs_error)?.is_file()
        || !crate::mvcc::shared_files::filesystem_supported(
            crate::mvcc::shared_files::filesystem_magic(&file)?,
        )
    {
        return Err(LimboError::InvalidArgument(
            "offline files require regular native ext-family storage".into(),
        ));
    }
    Ok(file)
}

#[cfg(target_os = "linux")]
pub(super) fn lock_exclusive(file: &std::fs::File) -> Result<()> {
    use std::os::fd::AsRawFd;
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &mut lock) } != 0 {
        return Err(fs_error(std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(super) fn lock_exclusive(_: &std::fs::File) -> Result<()> {
    super::require_linux_platform()
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::Arc;

    fn create_mvcc(path: &Path) {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let db =
            crate::Database::open_file(io, path.to_str().unwrap(), Arc::new(crate::EngineDialect))
                .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA journal_mode=mvcc; CREATE TABLE kept(id INTEGER PRIMARY KEY, value TEXT); CREATE INDEX kept_value ON kept(value); INSERT INTO kept VALUES(1,'retained')").unwrap();
        conn.close().unwrap();
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn offline_backend_rejects_main_log_alias_before_creating() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("fresh.db-log");
        assert!(OrdinaryOfflineBackend::create_staged(&staging).is_err());
        assert!(
            !staging.exists(),
            "self-aliasing staging database was created"
        );
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn offline_backend_initializes_and_publishes_fresh_database_without_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging.db");
        let destination = dir.path().join("published.db");
        let backend = OrdinaryOfflineBackend::create_staged(&staging).unwrap();
        super::super::OfflineModeChange::new(
            backend,
            OfflinePrerequisite::AllOlderWorkersStopped,
            64 * 1024 * 1024,
        )
        .publish_new(&destination)
        .unwrap();
        assert!(!staging.exists());
        super::super::validate_shared_open(&destination).unwrap();
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let db = crate::Database::open(
            io,
            destination.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(64 * 1024 * 1024),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.prepare("SELECT count(*) FROM sqlite_schema")
            .unwrap()
            .run_with_row_callback(|_| Ok(()))
            .unwrap();
        drop(conn);
        drop(db);
        let before = std::fs::read(&destination).unwrap();
        let other_stage = dir.path().join("other.db");
        let backend = OrdinaryOfflineBackend::create_staged(&other_stage).unwrap();
        assert!(super::super::OfflineModeChange::new(
            backend,
            OfflinePrerequisite::AllOlderWorkersStopped,
            64 * 1024 * 1024
        )
        .publish_new(&destination)
        .is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), before);
        assert!(OrdinaryOfflineBackend::create_staged(&destination).is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), before);
    }

    #[test]
    #[ignore = "subprocess fault-injection entry point"]
    fn offline_marker_crash_child() {
        let Some(path) = std::env::var_os("TURSO_OFFLINE_MARKER_CRASH_PATH") else {
            return;
        };
        let point = if std::env::var("TURSO_OFFLINE_MARKER_CRASH_POINT").unwrap() == "committed" {
            MarkerPoint::Committed
        } else {
            MarkerPoint::Checkpointed
        };
        let mut backend = OrdinaryOfflineBackend::open(
            Path::new(&path),
            OfflinePrerequisite::AllOlderWorkersStopped,
        )
        .unwrap();
        backend.marker_hook = Some(Box::new(move |observed| {
            if observed == point {
                std::process::exit(73);
            }
        }));
        let mut transition = super::super::OfflineModeChange::new(
            backend,
            OfflinePrerequisite::AllOlderWorkersStopped,
            64 * 1024 * 1024,
        );
        transition.upgrade().unwrap();
        panic!("marker crash point was not reached");
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn offline_backend_resumes_committed_marker_without_recreating_namespace() {
        for point in ["committed", "checkpointed"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("interrupted.db");
            create_mvcc(&path);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "mvcc::mode_transition::ordinary::tests::offline_marker_crash_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("TURSO_OFFLINE_MARKER_CRASH_PATH", &path)
                .env("TURSO_OFFLINE_MARKER_CRASH_POINT", point)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let file = std::fs::File::open(&path).unwrap();
            let id = super::super::identity(&file).unwrap();
            let namespace = super::super::namespace_for(&path, id).unwrap();
            let names = ["manifest", "ids-a", "ids-b", "ids-selector"];
            let before = names.map(|name| std::fs::read(namespace.join(name)).unwrap());
            resume_prepared_marker(
                &path,
                64 * 1024 * 1024,
                OfflinePrerequisite::AllOlderWorkersStopped,
            )
            .unwrap();
            assert_eq!(
                names.map(|name| std::fs::read(namespace.join(name)).unwrap()),
                before
            );
            super::super::validate_shared_open(&path).unwrap();
            let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
            let db = crate::Database::open(
                io,
                path.to_str().unwrap(),
                crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                    .with_shared_mvcc_for_test(64 * 1024 * 1024),
            )
            .unwrap();
            let conn = db.connect(crate::ConnectOpts::new()).unwrap();
            let mut values = Vec::new();
            conn.prepare("SELECT value FROM kept INDEXED BY kept_value WHERE id=1")
                .unwrap()
                .run_with_row_callback(|row| {
                    values.push(row.get::<&str>(0)?.to_owned());
                    Ok(())
                })
                .unwrap();
            assert_eq!(values, ["retained"]);
        }
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn offline_backend_retains_exclusion_until_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("locked.db");
        create_mvcc(&path);
        let backend =
            OrdinaryOfflineBackend::open(&path, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(
            OrdinaryOfflineBackend::open(&path, OfflinePrerequisite::AllOlderWorkersStopped)
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(backend);
        OrdinaryOfflineBackend::open(&path, OfflinePrerequisite::AllOlderWorkersStopped).unwrap();
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn offline_backend_rejects_sidecar_aliases_before_mutation() {
        for (logical, symlink) in [(false, false), (false, true), (true, false), (true, true)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("alias.db");
            create_mvcc(&path);
            let sidecar = if logical {
                path.with_extension("db-log")
            } else {
                PathBuf::from(format!("{}-wal", path.display()))
            };
            if sidecar.exists() {
                std::fs::remove_file(&sidecar).unwrap();
            }
            if symlink {
                std::os::unix::fs::symlink(&path, &sidecar).unwrap();
            } else {
                std::fs::hard_link(&path, &sidecar).unwrap();
            }
            let before = std::fs::read(&path).unwrap();
            assert!(OrdinaryOfflineBackend::open(
                &path,
                OfflinePrerequisite::AllOlderWorkersStopped
            )
            .is_err());
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn offline_backend_upgrades_real_mvcc_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordinary-mvcc.db");
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let db = crate::Database::open_file(
            io.clone(),
            path.to_str().unwrap(),
            Arc::new(crate::EngineDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA journal_mode=mvcc; CREATE TABLE kept(id INTEGER PRIMARY KEY, value TEXT); CREATE INDEX kept_value ON kept(value); INSERT INTO kept VALUES(1,'retained')").unwrap();
        conn.close().unwrap();
        drop(conn);
        drop(db);
        let backend =
            OrdinaryOfflineBackend::open(&path, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap();
        let mut transition = super::super::OfflineModeChange::new(
            backend,
            OfflinePrerequisite::AllOlderWorkersStopped,
            64 * 1024 * 1024,
        );
        transition.upgrade().unwrap();
        drop(transition);
        super::super::validate_shared_open(&path).unwrap();
        let db = crate::Database::open(
            io,
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(64 * 1024 * 1024),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        let mut rows = Vec::new();
        conn.prepare("SELECT value FROM kept INDEXED BY kept_value WHERE value='retained'")
            .unwrap()
            .run_with_row_callback(|row| {
                rows.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, ["retained"]);
    }
}
