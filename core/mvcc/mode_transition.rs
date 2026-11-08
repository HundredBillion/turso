pub(super) mod downgrade_files;
pub(super) mod downgrade_marker;
pub(super) mod downgrade_resume;
pub(super) mod operation;
mod ordinary;
pub(super) mod recovery;
#[cfg(test)]
mod recovery_integration_tests;
mod staging_cleanup;

use super::commit_manifest::{select_latest, ManifestRecord, RECORD_SIZE, SLOT_SIZE};
use super::shared_authority::PublishedPrefix;
use super::shared_ids::{DatabaseGeneration, InitialIds, MIN_HISTORY_LIMIT};
use crate::io::FileId;
use crate::{LimboError, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub(super) const SHARED_MVCC_VERSION: u8 = 254;

/// The backend owns exclusive database/protocol leases and stopped transaction admission.
/// Its methods retain those leases until all submitted I/O has actually retired.
pub(super) trait OfflineBackend {
    fn path(&self) -> &Path;
    fn recover_materialize_sync(&mut self) -> Result<u64>;
    /// Lowest consumed logical ID, including retired IDs, after materialization.
    fn materialized_logical_floor(&mut self) -> Result<i64>;
    fn commit_marker_checkpoint_sync(&mut self, marker: u8) -> Result<()>;
}

/// This is an operator prerequisite, not a fact that database locking can establish.
pub(super) enum OfflinePrerequisite {
    AllOlderWorkersStopped,
}

pub(super) struct OfflineModeChange<B> {
    backend: B,
    history_limit: u64,
}

impl<B: OfflineBackend> OfflineModeChange<B> {
    pub(super) fn new(backend: B, _: OfflinePrerequisite, history_limit: u64) -> Self {
        Self {
            backend,
            history_limit,
        }
    }

    pub(super) fn publish_new(mut self, destination: &Path) -> Result<()> {
        match fs::symlink_metadata(destination) {
            Ok(_) => {
                return Err(LimboError::InvalidArgument(
                    "new database destination already exists".into(),
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(fs_error(error)),
        }
        if canonical_parent(self.backend.path())? != canonical_parent(destination)? {
            return Err(LimboError::InvalidArgument(
                "new database staging must use the destination directory".into(),
            ));
        }
        self.upgrade()?;
        let cleanup = staging_cleanup::StagingCleanup::capture(self.backend.path(), destination)?;
        StagedDatabase::validate(self.backend.path(), destination)?.publish()?;
        drop(self.backend);
        cleanup.finish().map_err(|error| {
            LimboError::InvalidArgument(format!(
                "destination was published but staging cleanup failed: {error}"
            ))
        })
    }

    pub(super) fn upgrade(&mut self) -> Result<()> {
        if self.history_limit < MIN_HISTORY_LIMIT {
            return Err(LimboError::InvalidArgument(
                "shared MVCC history limit must be at least 32 KiB".into(),
            ));
        }
        require_linux_platform()?;
        let file = File::open(self.backend.path()).map_err(fs_error)?;
        let old_marker = read_marker(&file)?;
        if !matches!(old_marker, 1 | 2 | 255) {
            return Err(LimboError::InvalidArgument(
                "offline upgrade requires an ordinary database".into(),
            ));
        }
        require_marker(&file, old_marker)?;
        let timestamp = self.backend.recover_materialize_sync()?;
        let id = identity(&file)?;
        require_path_identity(self.backend.path(), id)?;
        let namespace = namespace_for(self.backend.path(), id)?;
        let logical_floor = self.backend.materialized_logical_floor()?;
        prepare_namespace(&namespace, id, timestamp, logical_floor, self.history_limit)?;
        self.backend
            .commit_marker_checkpoint_sync(SHARED_MVCC_VERSION)?;
        require_marker(&file, SHARED_MVCC_VERSION)?;
        validate_shared_open(self.backend.path())?;
        Ok(())
    }

    pub(super) fn downgrade(&mut self) -> Result<()> {
        require_linux_platform()?;
        let prefix = validate_shared_open(self.backend.path())?;
        let file = File::open(self.backend.path()).map_err(fs_error)?;
        let namespace = namespace_for(self.backend.path(), identity(&file)?)?;
        let timestamp = self.backend.recover_materialize_sync()?;
        if timestamp < prefix.committed_timestamp {
            return Err(LimboError::Corrupt(
                "offline downgrade did not materialize the acknowledged prefix".into(),
            ));
        }
        require_path_identity(self.backend.path(), identity(&file)?)?;
        self.backend.commit_marker_checkpoint_sync(2)?;
        require_marker(&file, 2)?;
        remove_namespace(&namespace)?;
        Ok(())
    }
}

/// Existing shared-mode opens must never create recovery metadata or upgrade the header.
pub(super) fn validate_shared_open(path: &Path) -> Result<PublishedPrefix> {
    let file = File::open(path).map_err(fs_error)?;
    require_marker(&file, SHARED_MVCC_VERSION)?;
    let namespace = namespace_for(path, identity(&file)?)?;
    for name in [
        "authority",
        "logical",
        "physical-wal",
        "physical-tshm",
        "ids-a",
        "ids-b",
        "ids-selector",
    ] {
        if !File::open(namespace.join(name))
            .map_err(fs_error)?
            .metadata()
            .map_err(fs_error)?
            .is_file()
        {
            return Err(LimboError::Corrupt(
                "shared MVCC sidecar is not a regular file".into(),
            ));
        }
    }
    read_manifest(&namespace, identity(&file)?)
}

pub(super) struct StagedDatabase {
    path: PathBuf,
    file: File,
    namespace: PathBuf,
    destination: PathBuf,
}

impl StagedDatabase {
    pub(super) fn validate(path: &Path, destination: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(fs_error)?;
        require_marker(&file, SHARED_MVCC_VERSION)?;
        require_empty_basename_wal(path)?;
        let namespace = namespace_for(destination, identity(&file)?)?;
        require_materialized(&namespace, identity(&file)?)?;
        Ok(Self {
            path: path.canonicalize().map_err(fs_error)?,
            file,
            namespace,
            destination: canonical_parent(destination)?.join(destination.file_name().ok_or_else(
                || LimboError::InvalidArgument("database destination needs a filename".into()),
            )?),
        })
    }

    pub(super) fn publish(self) -> Result<()> {
        self.publish_with(|_| Ok(()))
    }

    fn publish_with(self, mut observe: impl FnMut(PublicationPoint) -> Result<()>) -> Result<()> {
        require_marker(&self.file, SHARED_MVCC_VERSION)?;
        require_empty_basename_wal(&self.path)?;
        let id = identity(&self.file)?;
        if identity(&File::open(&self.path).map_err(fs_error)?)? != id {
            return Err(LimboError::InvalidArgument(
                "staged database pathname changed".into(),
            ));
        }
        require_materialized(&self.namespace, id)?;
        for name in [
            "manifest",
            "logical",
            "authority",
            "physical-wal",
            "physical-tshm",
            "ids-a",
            "ids-b",
            "ids-selector",
        ] {
            File::open(self.namespace.join(name))
                .map_err(fs_error)?
                .sync_all()
                .map_err(fs_error)?;
        }
        self.file.sync_all().map_err(fs_error)?;
        sync_directory(&self.namespace)?;
        sync_directory(self.namespace.parent().unwrap())?;
        observe(PublicationPoint::Prepared)?;
        link_open_inode(&self.file, &self.destination)?;
        observe(PublicationPoint::Linked)?;
        sync_directory(self.namespace.parent().unwrap())?;
        observe(PublicationPoint::Published)?;
        require_path_identity(&self.path, id)?;
        fs::remove_file(&self.path).map_err(fs_error)?;
        sync_directory(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
        observe(PublicationPoint::StagingRemoved)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicationPoint {
    Prepared,
    Linked,
    Published,
    StagingRemoved,
}

#[cfg(target_os = "linux")]
fn link_open_inode(file: &File, destination: &Path) -> Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(format!("/proc/self/fd/{}", file.as_raw_fd())).unwrap();
    let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| LimboError::InvalidArgument("database pathname contains NUL".into()))?;
    // The descriptor, not a replaceable staging pathname, identifies the published inode.
    let result = unsafe {
        libc::linkat(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::AT_SYMLINK_FOLLOW,
        )
    };
    if result != 0 {
        return Err(fs_error(std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn link_open_inode(_: &File, _: &Path) -> Result<()> {
    Err(LimboError::InvalidArgument(
        "atomic shared MVCC publication requires Linux procfs".into(),
    ))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .map_err(fs_error)?
        .sync_all()
        .map_err(fs_error)
}

fn require_linux_platform() -> Result<()> {
    if !cfg!(all(target_os = "linux", target_pointer_width = "64")) {
        return Err(LimboError::InvalidArgument(
            "offline shared MVCC transitions require 64-bit Linux".into(),
        ));
    }
    Ok(())
}

fn prepare_namespace(
    namespace: &Path,
    id: FileId,
    timestamp: u64,
    logical_floor: i64,
    history_limit: u64,
) -> Result<()> {
    if history_limit < MIN_HISTORY_LIMIT {
        return Err(LimboError::InvalidArgument(
            "shared MVCC history limit must be at least 32 KiB".into(),
        ));
    }
    let ids = InitialIds::new(
        id,
        DatabaseGeneration {
            generation: 1,
            checkpoint_timestamp: timestamp,
        },
        logical_floor,
    )?;
    let record = ManifestRecord::new(
        1,
        id,
        1,
        0,
        PublishedPrefix {
            committed_timestamp: timestamp,
            end_offset: 0,
            running_crc: 0,
        },
    )?;
    // An old marker and the offline lease make an interrupted preparation disposable.
    if namespace.exists() {
        remove_namespace(namespace)?;
    }
    fs::create_dir(namespace).map_err(fs_error)?;
    let mut bytes = [0; SLOT_SIZE * 2];
    bytes[..RECORD_SIZE].copy_from_slice(&record.encode());
    for (name, data) in [
        ("manifest", bytes.as_slice()),
        ("logical", &[]),
        ("physical-wal", &[]),
        ("ids-a", ids.bank_a.as_slice()),
        ("ids-b", &[]),
        ("ids-selector", ids.selector.as_slice()),
    ] {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(namespace.join(name))
            .map_err(fs_error)?;
        file.write_all(data).map_err(fs_error)?;
        file.sync_all().map_err(fs_error)?;
    }
    initialize_authority(namespace, id, history_limit)?;
    initialize_physical_coordination(namespace)?;
    for name in ["authority", "physical-tshm"] {
        File::open(namespace.join(name))
            .map_err(fs_error)?
            .sync_all()
            .map_err(fs_error)?;
    }
    sync_directory(namespace)?;
    sync_directory(namespace.parent().unwrap())
}

fn initialize_authority(namespace: &Path, id: FileId, history_limit: u64) -> Result<()> {
    let io: crate::sync::Arc<dyn crate::IO> = crate::sync::Arc::new(crate::io::PlatformIO::new()?);
    let authority_path = namespace.join("authority");
    let authority = super::shared_authority::SharedMvccAuthority::open(
        &io,
        authority_path
            .to_str()
            .ok_or_else(|| LimboError::InvalidArgument("MVCC namespace must be UTF-8".into()))?,
        id,
        history_limit,
    )?;
    drop(authority);
    Ok(())
}

fn initialize_physical_coordination(namespace: &Path) -> Result<()> {
    let io: crate::sync::Arc<dyn crate::IO> = crate::sync::Arc::new(crate::io::PlatformIO::new()?);
    let coordination =
        crate::storage::shared_wal_coordination::MappedSharedWalCoordination::create_or_open(
            &io,
            &namespace.join("physical-tshm"),
            64,
        )?;
    drop(coordination);
    Ok(())
}

fn remove_namespace(namespace: &Path) -> Result<()> {
    if !fs::symlink_metadata(namespace).map_err(fs_error)?.is_dir() {
        return Err(LimboError::InvalidArgument(
            "MVCC namespace must be an owned directory".into(),
        ));
    }
    let names = [
        "manifest",
        "logical",
        "authority",
        "physical-wal",
        "physical-tshm",
        "ids-a",
        "ids-b",
        "ids-selector",
    ];
    for entry in fs::read_dir(namespace).map_err(fs_error)? {
        let entry = entry.map_err(fs_error)?;
        if !names.iter().any(|name| entry.file_name() == *name) {
            return Err(LimboError::InvalidArgument(
                "MVCC namespace contains unowned files".into(),
            ));
        }
    }
    for name in names {
        match fs::remove_file(namespace.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(fs_error(error)),
        }
    }
    fs::remove_dir(namespace).map_err(fs_error)?;
    sync_directory(namespace.parent().unwrap())
}

fn require_path_identity(path: &Path, id: FileId) -> Result<()> {
    if identity(&File::open(path).map_err(fs_error)?)? != id {
        return Err(LimboError::InvalidArgument(
            "database pathname changed during offline transition".into(),
        ));
    }
    Ok(())
}

fn read_marker(file: &File) -> Result<u8> {
    let mut file = file.try_clone().map_err(fs_error)?;
    file.seek(SeekFrom::Start(18)).map_err(fs_error)?;
    let mut versions = [0; 2];
    file.read_exact(&mut versions).map_err(fs_error)?;
    if versions[0] != versions[1] {
        return Err(LimboError::Corrupt(
            "mismatched database mode versions".into(),
        ));
    }
    Ok(versions[0])
}

pub(super) fn namespace_for(path: &Path, id: FileId) -> Result<PathBuf> {
    Ok(canonical_parent(path)?.join(format!(".turso-mvcc-{:016x}-{:016x}", id.dev, id.ino)))
}

fn canonical_parent(path: &Path) -> Result<PathBuf> {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(fs_error)
}

fn identity(file: &File) -> Result<FileId> {
    let metadata = file.metadata().map_err(fs_error)?;
    Ok(FileId {
        dev: metadata.dev(),
        ino: metadata.ino(),
    })
}

pub(super) fn require_marker(file: &File, marker: u8) -> Result<()> {
    let mut file = file.try_clone().map_err(fs_error)?;
    file.seek(SeekFrom::Start(0)).map_err(fs_error)?;
    let mut header = [0; 100];
    file.read_exact(&mut header).map_err(fs_error)?;
    if &header[..16] != b"SQLite format 3\0" || header[18..20] != [marker, marker] {
        return Err(LimboError::InvalidArgument(
            "database mode marker does not match the requested transition".into(),
        ));
    }
    let page_size = match u16::from_be_bytes([header[16], header[17]]) {
        1 => 65536,
        size => u64::from(size),
    };
    let length = file.metadata().map_err(fs_error)?.len();
    if !(512..=65536).contains(&page_size)
        || !page_size.is_power_of_two()
        || length < page_size
        || length % page_size != 0
    {
        return Err(LimboError::Corrupt(
            "mode transition requires a complete database image".into(),
        ));
    }
    Ok(())
}

fn require_empty_basename_wal(path: &Path) -> Result<()> {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    match fs::metadata(PathBuf::from(wal)) {
        Ok(metadata) if metadata.len() == 0 => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(fs_error(error)),
        Ok(_) => Err(LimboError::InvalidArgument(
            "offline staging has uncheckpointed physical WAL frames".into(),
        )),
    }
}

fn require_materialized(namespace: &Path, id: FileId) -> Result<()> {
    if fs::metadata(namespace.join("physical-wal"))
        .map_err(fs_error)?
        .len()
        != 0
    {
        return Err(LimboError::InvalidArgument(
            "new database publication requires a completed physical WAL checkpoint".into(),
        ));
    }
    if read_manifest(namespace, id)?.end_offset != 0
        || fs::metadata(namespace.join("logical"))
            .map_err(fs_error)?
            .len()
            != 0
    {
        return Err(LimboError::InvalidArgument(
            "new database publication requires fully materialized logical history".into(),
        ));
    }
    Ok(())
}

fn read_manifest(namespace: &Path, id: FileId) -> Result<PublishedPrefix> {
    let mut file = File::open(namespace.join("manifest")).map_err(fs_error)?;
    if file.metadata().map_err(fs_error)?.len() != (SLOT_SIZE * 2) as u64 {
        return Err(LimboError::Corrupt("invalid mode manifest length".into()));
    }
    let mut bytes = [0; SLOT_SIZE * 2];
    file.read_exact(&mut bytes).map_err(fs_error)?;
    let first = ManifestRecord::decode(&bytes[..RECORD_SIZE])?;
    let second = ManifestRecord::decode(&bytes[SLOT_SIZE..SLOT_SIZE + RECORD_SIZE])?;
    Ok(select_latest([first, second], id)?.1.prefix())
}

fn fs_error(error: std::io::Error) -> LimboError {
    crate::error::io_error(error, "MVCC offline mode transition")
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::util::IOExt;

    struct PagerBackend {
        path: PathBuf,
        conn: crate::sync::Arc<crate::Connection>,
        calls: Vec<&'static str>,
        fail_materialize: bool,
        fail_marker: bool,
        materialized: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl PagerBackend {
        fn create(path: &Path) -> Self {
            let io: crate::sync::Arc<dyn crate::IO> =
                crate::sync::Arc::new(crate::io::PlatformIO::new().unwrap());
            let db = crate::Database::open_file(
                io,
                path.to_str().unwrap(),
                crate::sync::Arc::new(crate::EngineDialect),
            )
            .unwrap();
            let conn = db.connect(crate::ConnectOpts::new()).unwrap();
            conn.execute("CREATE TABLE preserved(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO preserved VALUES(1,'durable payload')").unwrap();
            Self {
                path: path.to_owned(),
                conn,
                calls: vec![],
                fail_materialize: false,
                fail_marker: false,
                materialized: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }
        }
    }

    impl OfflineBackend for PagerBackend {
        fn path(&self) -> &Path {
            &self.path
        }
        fn recover_materialize_sync(&mut self) -> Result<u64> {
            self.materialized
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.calls.push("materialize");
            if self.fail_materialize {
                return Err(LimboError::Busy);
            }
            self.conn.checkpoint(crate::CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })?;
            Ok(0)
        }
        fn materialized_logical_floor(&mut self) -> Result<i64> {
            let pager = self.conn.pager.load().clone();
            pager
                .io
                .block(|| pager.with_header(|header| -i64::from(header.database_size.get().max(1))))
        }
        fn commit_marker_checkpoint_sync(&mut self, marker: u8) -> Result<()> {
            self.calls.push("marker");
            if self.fail_marker {
                return Err(LimboError::Busy);
            }
            self.conn.execute("BEGIN IMMEDIATE")?;
            let pager = self.conn.pager.load().clone();
            pager.io.block(|| {
                pager.with_header_mut(|header| {
                    header.read_version = crate::storage::sqlite3_ondisk::RawVersion(marker);
                    header.write_version = crate::storage::sqlite3_ondisk::RawVersion(marker);
                })
            })?;
            self.conn.execute("COMMIT")?;
            self.conn.checkpoint(crate::CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })?;
            Ok(())
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_initializes_id_sidecars_before_publishing_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upgraded.db");
        let backend = PagerBackend::create(&path);
        let mut transition = OfflineModeChange::new(
            backend,
            OfflinePrerequisite::AllOlderWorkersStopped,
            64 * 1024,
        );
        transition.upgrade().unwrap();
        let file = File::open(&path).unwrap();
        let namespace = namespace_for(&path, identity(&file).unwrap()).unwrap();
        for name in ["ids-a", "ids-b", "ids-selector"] {
            assert!(
                namespace.join(name).is_file(),
                "required ID sidecar missing: {name}"
            );
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_offline_upgrade_and_downgrade_preserve_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.db");
        let backend = PagerBackend::create(&path);
        let mut transition =
            OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, 65536);
        transition.upgrade().unwrap();
        validate_shared_open(&path).unwrap();
        assert_eq!(transition.backend.calls, ["materialize", "marker"]);
        transition.backend.fail_materialize = true;
        assert!(transition.downgrade().is_err());
        validate_shared_open(&path).unwrap();
        transition.backend.fail_materialize = false;
        transition.downgrade().unwrap();
        assert!(validate_shared_open(&path).is_err());
        require_marker(&File::open(&path).unwrap(), 2).unwrap();
        transition.backend.conn.close().unwrap();
        drop(transition);
        let io: crate::sync::Arc<dyn crate::IO> =
            crate::sync::Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = crate::Database::open_file(
            io,
            path.to_str().unwrap(),
            crate::sync::Arc::new(crate::EngineDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        let mut query = conn
            .query("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .unwrap();
        let mut rows = vec![];
        query
            .run_with_row_callback(|row| {
                rows.push(row.get_value(0).to_text().unwrap().to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, ["durable payload"]);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_failed_marker_keeps_old_mode_and_can_retry_offline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.db");
        let mut backend = PagerBackend::create(&path);
        backend.fail_marker = true;
        let mut transition =
            OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, 65536);
        assert!(transition.upgrade().is_err());
        require_marker(&File::open(&path).unwrap(), 2).unwrap();
        assert!(validate_shared_open(&path).is_err());
        transition.backend.fail_marker = false;
        transition.upgrade().unwrap();
        transition.backend.fail_marker = true;
        assert!(transition.downgrade().is_err());
        validate_shared_open(&path).unwrap();
        transition.backend.fail_marker = false;
        transition.downgrade().unwrap();
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_new_creation_keeps_final_name_absent_until_complete() {
        let dir = tempfile::tempdir().unwrap();
        let staged = dir.path().join("private.db");
        let destination = dir.path().join("published.db");
        let backend = PagerBackend::create(&staged);
        assert!(!destination.exists());
        OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, 65536)
            .publish_new(&destination)
            .unwrap();
        validate_shared_open(&destination).unwrap();
        assert!(!staged.exists());
    }

    #[test]
    fn mode_transition_invalid_limit_rejects_before_backend_work() {
        for limit in [0, MIN_HISTORY_LIMIT - 1] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("existing.db");
            let backend = PagerBackend::create(&path);
            let touched = backend.materialized.clone();
            let mut transition =
                OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, limit);
            let error = transition.upgrade().unwrap_err();
            assert!(!touched.load(std::sync::atomic::Ordering::SeqCst));
            assert!(error.to_string().contains("history limit"));
        }
    }

    #[test]
    fn mode_transition_invalid_id_floor_preserves_existing_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let namespace = dir.path().join("existing");
        fs::create_dir(&namespace).unwrap();
        fs::write(namespace.join("logical"), b"existing bytes").unwrap();
        let result = prepare_namespace(
            &namespace,
            FileId { dev: 1, ino: 2 },
            0,
            0,
            MIN_HISTORY_LIMIT,
        );
        assert!(result.is_err());
        assert_eq!(
            fs::read(namespace.join("logical")).unwrap(),
            b"existing bytes"
        );
        assert_eq!(fs::read_dir(&namespace).unwrap().count(), 1);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn mode_transition_unsupported_platform_rejects_before_backend_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.db");
        let backend = PagerBackend::create(&path);
        let touched = backend.materialized.clone();
        let mut transition =
            OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, 65536);
        assert!(transition.upgrade().is_err());
        assert!(!touched.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn mode_transition_new_creation_rejects_visible_destination_before_backend_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("visible.db");
        let mut backend = PagerBackend::create(&path);
        backend.recover_materialize_sync().unwrap();
        let touched = backend.materialized.clone();
        touched.store(false, std::sync::atomic::Ordering::SeqCst);
        let before = fs::read(&path).unwrap();
        assert!(OfflineModeChange::new(
            backend,
            OfflinePrerequisite::AllOlderWorkersStopped,
            65536
        )
        .publish_new(&path)
        .is_err());
        assert!(!touched.load(std::sync::atomic::Ordering::SeqCst));
        require_marker(&File::open(&path).unwrap(), 2).unwrap();
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn mode_transition_existing_ordinary_open_never_upgrades() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordinary.db");
        let mut backend = PagerBackend::create(&path);
        backend.recover_materialize_sync().unwrap();
        let before = fs::read(&path).unwrap();
        let count = fs::read_dir(dir.path()).unwrap().count();
        assert!(validate_shared_open(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), count);
        backend.conn.execute("SELECT * FROM preserved").unwrap();
    }

    pub(in crate::mvcc) fn fixture(dir: &Path) -> (PathBuf, PathBuf) {
        let path = dir.join("staged.db");
        let destination = dir.join("published.db");
        let io: crate::sync::Arc<dyn crate::IO> =
            crate::sync::Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = crate::Database::open_file(
            io.clone(),
            path.to_str().unwrap(),
            crate::sync::Arc::new(crate::EngineDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE preserved(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO preserved VALUES(1,'durable payload')").unwrap();
        conn.execute("BEGIN IMMEDIATE").unwrap();
        let pager = conn.pager.load().clone();
        io.block(|| {
            pager.with_header_mut(|header| {
                header.read_version =
                    crate::storage::sqlite3_ondisk::RawVersion(SHARED_MVCC_VERSION);
                header.write_version =
                    crate::storage::sqlite3_ondisk::RawVersion(SHARED_MVCC_VERSION);
            })
        })
        .unwrap();
        conn.execute("COMMIT").unwrap();
        conn.checkpoint(crate::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
        conn.close().unwrap();
        drop(conn);
        drop(pager);
        drop(db);
        let file = File::open(&path).unwrap();
        let id = identity(&file).unwrap();
        let namespace = namespace_for(&destination, id).unwrap();
        fs::create_dir(&namespace).unwrap();
        let record = ManifestRecord::new(1, id, 1, 0, PublishedPrefix::default()).unwrap();
        let mut manifest = vec![0; SLOT_SIZE * 2];
        manifest[..RECORD_SIZE].copy_from_slice(&record.encode());
        fs::write(namespace.join("manifest"), manifest).unwrap();
        fs::write(namespace.join("logical"), []).unwrap();
        #[cfg(target_os = "linux")]
        initialize_authority(&namespace, id, 65536).unwrap();
        #[cfg(not(target_os = "linux"))]
        fs::write(namespace.join("authority"), [0; 4096]).unwrap();
        fs::write(namespace.join("physical-wal"), []).unwrap();
        initialize_physical_coordination(&namespace).unwrap();
        let floor = -i64::try_from(file.metadata().unwrap().len() / 512)
            .unwrap()
            .max(1);
        let ids = InitialIds::new(
            id,
            DatabaseGeneration {
                generation: 1,
                checkpoint_timestamp: 0,
            },
            floor,
        )
        .unwrap();
        fs::write(namespace.join("ids-a"), ids.bank_a).unwrap();
        fs::write(namespace.join("ids-b"), []).unwrap();
        fs::write(namespace.join("ids-selector"), ids.selector).unwrap();
        (path, destination)
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_marked_open_requires_every_published_sidecar() {
        for name in [
            "authority",
            "logical",
            "manifest",
            "physical-wal",
            "physical-tshm",
            "ids-a",
            "ids-b",
            "ids-selector",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (staged, destination) = fixture(dir.path());
            StagedDatabase::validate(&staged, &destination)
                .unwrap()
                .publish()
                .unwrap();
            let namespace = namespace_for(
                &destination,
                identity(&File::open(&destination).unwrap()).unwrap(),
            )
            .unwrap();
            fs::remove_file(namespace.join(name)).unwrap();
            assert!(
                validate_shared_open(&destination).is_err(),
                "accepted absent {name}"
            );
            assert!(!namespace.join(name).exists());
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_aliases_and_missing_manifests_cannot_initialize_state() {
        let dir = tempfile::tempdir().unwrap();
        let (staged, destination) = fixture(dir.path());
        StagedDatabase::validate(&staged, &destination)
            .unwrap()
            .publish()
            .unwrap();
        let alias = dir.path().join("alias.db");
        fs::hard_link(&destination, &alias).unwrap();
        validate_shared_open(&alias).unwrap();
        let elsewhere = dir.path().join("elsewhere");
        fs::create_dir(&elsewhere).unwrap();
        let other = elsewhere.join("alias.db");
        fs::hard_link(&destination, &other).unwrap();
        assert!(validate_shared_open(&other).is_err());
        assert_eq!(fs::read_dir(&elsewhere).unwrap().count(), 1);
        let namespace = namespace_for(
            &destination,
            identity(&File::open(&destination).unwrap()).unwrap(),
        )
        .unwrap();
        fs::remove_file(namespace.join("manifest")).unwrap();
        assert!(validate_shared_open(&destination).is_err());
        assert!(!namespace.join("manifest").exists());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_process_death_keeps_final_image_complete() {
        for point in [
            PublicationPoint::Prepared,
            PublicationPoint::Linked,
            PublicationPoint::Published,
            PublicationPoint::StagingRemoved,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (staged, destination) = fixture(dir.path());
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "mvcc::mode_transition::tests::mode_transition_process_child",
                    "--nocapture",
                ])
                .env("TURSO_MODE_TEST_DIR", dir.path())
                .env("TURSO_MODE_TEST_POINT", format!("{point:?}"))
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            let status = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                if std::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("publication child timed out");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(libc::SIGKILL));
            if point == PublicationPoint::Prepared {
                assert!(!destination.exists());
                assert!(staged.exists());
            } else {
                validate_shared_open(&destination).unwrap();
                let file = File::open(&destination).unwrap();
                require_marker(&file, SHARED_MVCC_VERSION).unwrap();
                assert!(file.metadata().unwrap().len() >= 8192);
            }
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_process_child() {
        let Some(dir) = std::env::var_os("TURSO_MODE_TEST_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        let point = std::env::var("TURSO_MODE_TEST_POINT").unwrap();
        StagedDatabase::validate(&dir.join("staged.db"), &dir.join("published.db"))
            .unwrap()
            .publish_with(|current| {
                if format!("{current:?}") == point {
                    unsafe {
                        libc::raise(libc::SIGKILL);
                    }
                }
                Ok(())
            })
            .unwrap();
        panic!("requested kill point was not reached");
    }

    #[test]
    #[ignore = "requires TURSO_OLD_CLI pointing to a provenance-verified older executable"]
    #[cfg(target_os = "linux")]
    fn mode_transition_old_creator_race_never_replaces_its_database() {
        let old = std::env::var_os("TURSO_OLD_CLI").expect("TURSO_OLD_CLI required");
        let dir = tempfile::tempdir().unwrap();
        let (staged, destination) = fixture(dir.path());
        let image = StagedDatabase::validate(&staged, &destination).unwrap();
        let mut competitor = None;
        let result = image.publish_with(|point| {
            if point == PublicationPoint::Prepared {
                let output = std::process::Command::new(&old)
                    .arg("-q")
                    .arg(&destination)
                    .arg("CREATE TABLE legacy(v); INSERT INTO legacy VALUES('race winner');")
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                competitor = Some(fs::read(&destination).unwrap());
            }
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(fs::read(&destination).unwrap(), competitor.unwrap());
        assert!(staged.exists());
        let output = std::process::Command::new(&old)
            .args(["-q", "--readonly"])
            .arg(&destination)
            .arg("SELECT * FROM legacy;")
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("race winner"));
    }

    #[test]
    #[ignore = "requires TURSO_OLD_CLI pointing to a provenance-verified older executable"]
    fn mode_transition_old_binary_rejects_pager_produced_marker() {
        let old = std::env::var_os("TURSO_OLD_CLI").expect("TURSO_OLD_CLI required");
        let dir = tempfile::tempdir().unwrap();
        let (destination, _) = fixture(dir.path());
        let bytes = fs::read(&destination).unwrap();
        for flags in [
            vec![],
            vec!["--readonly"],
            vec!["--experimental-multiprocess-wal"],
        ] {
            let output = std::process::Command::new(&old)
                .arg("-q")
                .args(flags)
                .arg(&destination)
                .arg("SELECT * FROM preserved;")
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains("Invalid read_version: 254"));
            assert_eq!(fs::read(&destination).unwrap(), bytes);
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_staging_path_replacement_cannot_publish_another_inode() {
        let dir = tempfile::tempdir().unwrap();
        let (staged, destination) = fixture(dir.path());
        let id = identity(&File::open(&staged).unwrap()).unwrap();
        let image = StagedDatabase::validate(&staged, &destination).unwrap();
        let result = image.publish_with(|point| {
            if point == PublicationPoint::Prepared {
                fs::rename(&staged, dir.path().join("saved-original.db")).unwrap();
                fs::write(&staged, b"wrong inode").unwrap();
            }
            Ok(())
        });
        assert!(result.is_err());
        if destination.exists() {
            assert_eq!(identity(&File::open(&destination).unwrap()).unwrap(), id);
            validate_shared_open(&destination).unwrap();
        }
        assert_eq!(fs::read(&staged).unwrap(), b"wrong inode");
    }

    #[test]
    fn mode_transition_publication_rejects_nonempty_physical_wal() {
        let dir = tempfile::tempdir().unwrap();
        let (staged, destination) = fixture(dir.path());
        let namespace = namespace_for(
            &destination,
            identity(&File::open(&staged).unwrap()).unwrap(),
        )
        .unwrap();
        fs::write(namespace.join("physical-wal"), b"uncheckpointed frames").unwrap();
        assert!(StagedDatabase::validate(&staged, &destination).is_err());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_live_wal_only_user_row_requires_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let staged = dir.path().join("staged.db");
        let destination = dir.path().join("published.db");
        let backend = PagerBackend::create(&staged);
        let mut transition =
            OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, 65536);
        transition.upgrade().unwrap();
        transition
            .backend
            .conn
            .execute("INSERT INTO preserved VALUES(2,'WAL-only row')")
            .unwrap();
        let mut wal = staged.as_os_str().to_owned();
        wal.push("-wal");
        assert!(fs::metadata(PathBuf::from(wal)).unwrap().len() > 0);
        assert!(StagedDatabase::validate(&staged, &destination).is_err());
        transition.backend.recover_materialize_sync().unwrap();
        StagedDatabase::validate(&staged, &destination).unwrap();
    }

    #[test]
    fn mode_transition_publication_requires_materialized_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let (staged, destination) = fixture(dir.path());
        let id = identity(&File::open(&staged).unwrap()).unwrap();
        let namespace = namespace_for(&destination, id).unwrap();
        let prefix = PublishedPrefix {
            committed_timestamp: 1,
            end_offset: 512,
            running_crc: 9,
        };
        let record = ManifestRecord::new(1, id, 1, 0, prefix).unwrap();
        let mut bytes = vec![0; SLOT_SIZE * 2];
        bytes[..RECORD_SIZE].copy_from_slice(&record.encode());
        fs::write(namespace.join("manifest"), bytes).unwrap();
        assert!(StagedDatabase::validate(&staged, &destination).is_err());
        assert!(!destination.exists());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn mode_transition_new_file_publish_preserves_inode_and_never_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let (staged, destination) = fixture(dir.path());
        let before = identity(&File::open(&staged).unwrap()).unwrap();
        StagedDatabase::validate(&staged, &destination)
            .unwrap()
            .publish()
            .unwrap();
        assert!(!staged.exists());
        assert_eq!(
            identity(&File::open(&destination).unwrap()).unwrap(),
            before
        );
        require_marker(&File::open(&destination).unwrap(), SHARED_MVCC_VERSION).unwrap();
        let (second, _) = fixture(dir.path());
        let original = fs::read(&destination).unwrap();
        assert!(StagedDatabase::validate(&second, &destination)
            .unwrap()
            .publish()
            .is_err());
        assert_eq!(fs::read(&destination).unwrap(), original);
        assert!(second.exists());
    }
}

#[cfg(all(test, target_os = "linux", not(shuttle)))]
pub(crate) fn prepare_test_namespace(
    path: &Path,
    timestamp: u64,
    history_limit: u64,
) -> Result<()> {
    let file = File::open(path).map_err(fs_error)?;
    require_marker(&file, SHARED_MVCC_VERSION)?;
    let id = identity(&file)?;
    let namespace = namespace_for(path, id)?;
    let floor = -i64::try_from(file.metadata().map_err(fs_error)?.len() / 512)
        .map_err(|_| LimboError::DatabaseFull)?
        .max(1);
    prepare_namespace(&namespace, id, timestamp, floor, history_limit)
}
