use super::downgrade_files::{validate_ordinary_log, RetainedFile};
use super::ordinary::lock_exclusive;
use super::{fs_error, identity, namespace_for, require_marker, OfflinePrerequisite};
use crate::io::{File as IoFile, FileId};
use crate::mvcc::commit_manifest::{select_latest, ManifestRecord, RECORD_SIZE, SLOT_SIZE};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::Arc;
use crate::{LimboError, OpenFlags, Result, IO};
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const NAMES: [&str; 8] = [
    "manifest",
    "logical",
    "physical-wal",
    "physical-tshm",
    "authority",
    "ids-a",
    "ids-b",
    "ids-selector",
];

/// A read-only metadata view and unlink capability, never marker-write authority.
/// The native main lease remains owned until the actual view and aliases retire.
pub(crate) struct PreparedDowngradeCleanup {
    main: RetainedFile,
    wal: RetainedFile,
    main_io: Arc<dyn IoFile>,
    wal_io: Arc<dyn IoFile>,
    namespace: PathBuf,
    namespace_file: Option<File>,
    files: Vec<RetainedFile>,
    record: Option<ManifestRecord>,
    log_path: PathBuf,
    log: Option<RetainedFile>,
    header: DatabaseHeader,
    bytes: u64,
}
impl PreparedDowngradeCleanup {
    fn open(path: &Path) -> Result<Arc<Self>> {
        super::require_linux_platform()?;
        let mut main = RetainedFile::open(path.to_path_buf())?;
        lock_exclusive(&main.file)?;
        main.path = path.canonicalize().map_err(fs_error)?;
        main.revalidate()?;
        require_marker(&main.file, 2)?;
        let namespace = namespace_for(&main.path, main.id)?;
        let namespace_file = match std::fs::symlink_metadata(&namespace) {
            Ok(metadata) if metadata.is_dir() => Some(
                std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
                    .open(&namespace)
                    .map_err(fs_error)?,
            ),
            Ok(_) => return Err(invalid("cleanup namespace is not a native directory")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(fs_error(error)),
        };
        if let Some(file) = &namespace_file {
            if identity(file)?.dev != main.id.dev {
                return Err(invalid("partial cleanup namespace is on another device"));
            }
        }
        let mut files = Vec::new();
        if namespace_file.is_some() {
            for entry in std::fs::read_dir(&namespace).map_err(fs_error)? {
                let entry = entry.map_err(fs_error)?;
                if !NAMES.iter().any(|name| entry.file_name() == *name) {
                    return Err(invalid("unknown file in partial downgrade namespace"));
                }
                files.push(RetainedFile::open(entry.path())?);
            }
        }
        let wal = RetainedFile::open(PathBuf::from(format!("{}-wal", main.path.display())))?;
        let log_path = main.path.with_extension("db-log");
        let log = match std::fs::symlink_metadata(&log_path) {
            Ok(_) => Some(RetainedFile::open(log_path.clone())?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(fs_error(error)),
        };
        let mut identities = vec![main.id];
        for file in std::iter::once(&wal).chain(log.iter()).chain(files.iter()) {
            if file.id.dev != main.id.dev || identities.contains(&file.id) {
                return Err(invalid(
                    "partial cleanup files must have distinct native identities",
                ));
            }
            identities.push(file.id);
        }
        let record = files
            .iter()
            .find(|file| file.path.file_name().is_some_and(|name| name == "manifest"))
            .map(|file| read_terminal(file, main.id, wal.id))
            .transpose()?;
        if record.is_none() && (!files.is_empty() || log.is_some()) {
            return Err(invalid(
                "cleanup without terminal manifest requires empty namespace and no ordinary log",
            ));
        }
        let mut raw = [0; DatabaseHeader::SIZE];
        main.file.read_exact_at(&mut raw, 0).map_err(fs_error)?;
        let header: DatabaseHeader = bytemuck::pod_read_unaligned(&raw);
        let bytes = main.file.metadata().map_err(fs_error)?.len();
        validate_layout(&header, bytes)?;
        let io = crate::io::SyscallIO::new()?;
        let flags = OpenFlags::NoLock | OpenFlags::ReadOnly;
        let main_io = io.open_file(
            main.path
                .to_str()
                .ok_or_else(|| invalid("cleanup path must be UTF-8"))?,
            flags,
            false,
        )?;
        let wal_io = io.open_file(
            wal.path
                .to_str()
                .ok_or_else(|| invalid("cleanup WAL path must be UTF-8"))?,
            flags,
            false,
        )?;
        let proof = Arc::new(Self {
            main,
            wal,
            main_io,
            wal_io,
            namespace,
            namespace_file,
            files,
            record,
            log_path,
            log,
            header,
            bytes,
        });
        proof.revalidate()?;
        Ok(proof)
    }
    #[cfg(test)]
    pub(in crate::mvcc) fn open_for_test(path: &Path) -> Result<Arc<Self>> {
        Self::open(path)
    }
    pub(crate) fn path(&self) -> &Path {
        &self.main.path
    }
    pub(crate) fn database_id(&self) -> FileId {
        self.main.id
    }
    pub(crate) fn wal_id(&self) -> FileId {
        self.wal.id
    }
    pub(crate) fn main_file(&self) -> Arc<dyn IoFile> {
        self.main_io.clone()
    }
    pub(crate) fn wal_file(&self) -> Arc<dyn IoFile> {
        self.wal_io.clone()
    }
    pub(crate) fn revalidate(&self) -> Result<()> {
        self.main.revalidate()?;
        self.wal.revalidate()?;
        require_marker(&self.main.file, 2)?;
        if self.main_io.file_id()? != Some(self.main.id)
            || self.wal_io.file_id()? != Some(self.wal.id)
            || self.wal.file.metadata().map_err(fs_error)?.len() != 0
        {
            return Err(invalid(
                "cleanup requires unchanged main and empty retained WAL",
            ));
        }
        let mut raw = [0; DatabaseHeader::SIZE];
        self.main
            .file
            .read_exact_at(&mut raw, 0)
            .map_err(fs_error)?;
        let header: DatabaseHeader = bytemuck::pod_read_unaligned(&raw);
        let bytes = self.main.file.metadata().map_err(fs_error)?.len();
        validate_layout(&header, bytes)?;
        if header.page_size != self.header.page_size
            || header.reserved_space != self.header.reserved_space
            || header.database_size != self.header.database_size
            || bytes != self.bytes
        {
            return Err(invalid("partial cleanup physical layout changed"));
        }
        require_absent(&PathBuf::from(format!("{}-tshm", self.main.path.display())))?;
        if let Some(directory) = &self.namespace_file {
            let metadata = std::fs::symlink_metadata(&self.namespace).map_err(fs_error)?;
            if !metadata.is_dir()
                || identity(directory)?
                    != (FileId {
                        dev: metadata.dev(),
                        ino: metadata.ino(),
                    })
            {
                return Err(invalid("partial cleanup namespace identity changed"));
            }
            let expected: std::collections::HashSet<_> =
                self.files.iter().map(|file| file.path.clone()).collect();
            let actual: std::collections::HashSet<_> = std::fs::read_dir(&self.namespace)
                .map_err(fs_error)?
                .map(|entry| entry.map(|entry| entry.path()).map_err(fs_error))
                .collect::<Result<_>>()?;
            if expected != actual {
                return Err(invalid("partial cleanup namespace entries changed"));
            }
        } else {
            require_absent(&self.namespace)?;
        }
        for file in &self.files {
            file.revalidate()?;
            if matches!(
                file.path.file_name().and_then(|s| s.to_str()),
                Some("logical" | "physical-wal")
            ) && file.file.metadata().map_err(fs_error)?.len() != 0
            {
                return Err(invalid("partial cleanup shared logs contain history"));
            }
            if file.path.file_name().is_some_and(|name| name == "manifest")
                && Some(read_terminal(file, self.main.id, self.wal.id)?) != self.record
            {
                return Err(invalid("partial cleanup terminal record changed"));
            }
        }
        if let Some(log) = &self.log {
            log.revalidate()?;
            validate_ordinary_log(log)?;
        } else {
            require_absent(&self.log_path)?;
        }
        Ok(())
    }
    fn unlink(&self) -> Result<()> {
        self.revalidate()?;
        self.main.file.sync_all().map_err(fs_error)?;
        let parent = self
            .main
            .path
            .parent()
            .ok_or_else(|| invalid("missing cleanup parent"))?;
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
            .open(parent)
            .map_err(fs_error)?;
        if let Some(log) = &self.log {
            log.revalidate()?;
            std::fs::remove_file(&log.path).map_err(fs_error)?;
            directory.sync_all().map_err(fs_error)?;
        }
        if let Some(namespace) = &self.namespace_file {
            for file in &self.files {
                if file.path.file_name().is_some_and(|name| name == "manifest") {
                    continue;
                }
                file.revalidate()?;
                std::fs::remove_file(&file.path).map_err(fs_error)?;
            }
            namespace.sync_all().map_err(fs_error)?;
            if let Some(manifest) = self
                .files
                .iter()
                .find(|file| file.path.file_name().is_some_and(|name| name == "manifest"))
            {
                manifest.revalidate()?;
                std::fs::remove_file(&manifest.path).map_err(fs_error)?;
                namespace.sync_all().map_err(fs_error)?;
            }
            std::fs::remove_dir(&self.namespace).map_err(fs_error)?;
            directory.sync_all().map_err(fs_error)?;
        }
        Ok(())
    }
}
fn validate_layout(header: &DatabaseHeader, bytes: u64) -> Result<()> {
    let size = header.page_size.get();
    if header.magic != *b"SQLite format 3\0"
        || header.read_version.0 != 2
        || header.write_version.0 != 2
        || crate::PageSize::new(size).is_none()
        || !header
            .page_size
            .has_valid_reserved_space(header.reserved_space)
        || header.database_size.get() == 0
        || bytes % u64::from(size) != 0
        || bytes / u64::from(size) < u64::from(header.database_size.get())
    {
        return Err(invalid(
            "partial cleanup requires valid materialized ordinary WAL layout",
        ));
    }
    Ok(())
}
fn read_terminal(file: &RetainedFile, main: FileId, wal: FileId) -> Result<ManifestRecord> {
    if file.file.metadata().map_err(fs_error)?.len() != (2 * SLOT_SIZE) as u64 {
        return Err(invalid("partial cleanup manifest length changed"));
    }
    let mut records = [None; 2];
    for (index, record) in records.iter_mut().enumerate() {
        let mut bytes = [0; RECORD_SIZE];
        file.file
            .read_exact_at(&mut bytes, (index * SLOT_SIZE) as u64)
            .map_err(fs_error)?;
        *record = ManifestRecord::decode(&bytes)?;
    }
    let record = select_latest(records, main)?.1;
    if record
        .downgrade_intent()
        .is_none_or(|intent| intent.basename_wal_id != wal)
        || record.log_generation() == 0
        || record.log_salt() != 0
        || record.prefix().end_offset != 0
        || record.prefix().running_crc != 0
    {
        return Err(invalid(
            "partial cleanup requires the original terminal database/WAL identities",
        ));
    }
    Ok(record)
}
fn require_absent(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(fs_error(error)),
        Ok(_) => Err(invalid("unexpected partial cleanup path")),
    }
}
pub(in crate::mvcc) fn resume_cleanup(path: &Path, _: OfflinePrerequisite) -> Result<()> {
    let proof = PreparedDowngradeCleanup::open(path)?;
    if let Some(record) = proof.record {
        let db = crate::Database::open_offline_downgrade_cleanup(proof.clone())?;
        let connection = db.connect(crate::ConnectOpts::new())?;
        let mut timestamp = None;
        connection
            .prepare("SELECT v FROM __turso_internal_mvcc_meta WHERE k='persistent_tx_ts_max'")?
            .run_with_row_callback(|row| {
                let value = row.get::<i64>(0)?;
                if value < 0 || timestamp.replace(value as u64).is_some() {
                    return Err(invalid("invalid cleanup physical timestamp"));
                }
                Ok(())
            })?;
        if timestamp != Some(record.prefix().committed_timestamp) {
            return Err(invalid(
                "cleanup target differs from materialized timestamp",
            ));
        }
        connection.close()?;
        drop(connection);
        drop(db);
    }
    proof.unlink().map_err(|error| {
        invalid(&format!(
            "database is ordinary WAL; downgrade cleanup remains incomplete: {error}"
        ))
    })
}
fn invalid(message: &str) -> LimboError {
    LimboError::InvalidArgument(message.into())
}
