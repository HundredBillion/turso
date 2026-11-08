use super::ordinary::{lock_exclusive, open_native};
use super::{fs_error, identity, namespace_for, require_marker, SHARED_MVCC_VERSION};
use crate::io::{File as IoFile, FileId};
use crate::mvcc::persistent_storage::logical_log::{LogHeader, LOG_HDR_SIZE};
use crate::mvcc::shared_files::SharedMvccFiles;
use crate::sync::Arc;
use crate::{LimboError, OpenFlags, Result, IO};
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

struct RetainedFile {
    path: PathBuf,
    file: File,
    id: FileId,
}
impl RetainedFile {
    fn open(path: PathBuf) -> Result<Self> {
        let file = open_native(&path)?;
        let id = identity(&file)?;
        let retained = Self { path, file, id };
        retained.revalidate()?;
        Ok(retained)
    }
    fn revalidate(&self) -> Result<()> {
        let metadata = std::fs::symlink_metadata(&self.path).map_err(fs_error)?;
        if !metadata.is_file()
            || identity(&self.file)? != self.id
            || (FileId {
                dev: metadata.dev(),
                ino: metadata.ino(),
            }) != self.id
        {
            return Err(invalid("downgrade file identity changed"));
        }
        Ok(())
    }
}

/// Native aliases used only while the caller retains completed-checkpoint ownership.
/// No path is created, truncated, or removed by this preparation slice.
pub(in crate::mvcc) struct DowngradeFiles {
    main: RetainedFile,
    wal: RetainedFile,
    files: Vec<RetainedFile>,
    namespace: PathBuf,
    namespace_file: File,
    log_path: PathBuf,
    log: Option<RetainedFile>,
    manifest: Arc<dyn IoFile>,
    main_io: Arc<dyn IoFile>,
    wal_io: Arc<dyn IoFile>,
}
impl DowngradeFiles {
    pub(in crate::mvcc) fn open(path: &Path, expected: &SharedMvccFiles) -> Result<Self> {
        super::require_linux_platform()?;
        let main = RetainedFile::open(path.to_path_buf())?;
        lock_exclusive(&main.file)?;
        let canonical = path.canonicalize().map_err(fs_error)?;
        let namespace = namespace_for(&canonical, main.id)?;
        if expected.main.file_id()? != Some(main.id)
            || namespace != expected.namespace.canonicalize().map_err(fs_error)?
        {
            return Err(invalid("downgrade requires the installed native database"));
        }
        let main = RetainedFile {
            path: canonical,
            ..main
        };
        require_marker(&main.file, SHARED_MVCC_VERSION)?;
        let namespace_file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
            .open(&namespace)
            .map_err(fs_error)?;
        let text = main
            .path
            .to_str()
            .ok_or_else(|| invalid("downgrade path must be UTF-8"))?;
        require_absent(&PathBuf::from(format!("{text}-tshm")))?;
        let wal_path = PathBuf::from(format!("{text}-wal"));
        if !wal_path.try_exists().map_err(fs_error)? {
            return Err(invalid(
                "downgrade preparation requires an existing empty basename WAL",
            ));
        }
        let wal = RetainedFile::open(wal_path)?;
        let log_path = main.path.with_extension("db-log");
        let log = match std::fs::symlink_metadata(&log_path) {
            Ok(_) => Some(RetainedFile::open(log_path.clone())?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(fs_error(error)),
        };
        let identities = [
            ("manifest", expected.manifest.file_id()?),
            ("logical", expected.logical.file_id()?),
            ("physical-wal", expected.physical_wal.file_id()?),
            ("physical-tshm", expected.physical_coordination.file_id()?),
            ("authority", expected.authority.file_id()?),
            ("ids-a", expected.id_banks[0].file_id()?),
            ("ids-b", expected.id_banks[1].file_id()?),
            ("ids-selector", expected.id_selector.file_id()?),
        ];
        let mut files = Vec::new();
        for (name, expected_id) in identities {
            let file = RetainedFile::open(namespace.join(name))?;
            if Some(file.id) != expected_id {
                return Err(invalid("downgrade namespace identity changed"));
            }
            files.push(file);
        }
        let mut ids = vec![main.id];
        for file in std::iter::once(&wal).chain(log.iter()).chain(files.iter()) {
            if file.id.dev != main.id.dev || ids.contains(&file.id) {
                return Err(invalid(
                    "downgrade files must have distinct native identities",
                ));
            }
            ids.push(file.id);
        }
        let io = crate::io::SyscallIO::new()?;
        let manifest = io.open_file(
            files[0]
                .path
                .to_str()
                .ok_or_else(|| invalid("manifest path must be UTF-8"))?,
            OpenFlags::NoLock,
            false,
        )?;
        if manifest.file_id()? != Some(files[0].id) {
            return Err(invalid("downgrade manifest alias identity changed"));
        }
        let main_io = io.open_file(main.path.to_str().unwrap(), OpenFlags::NoLock, false)?;
        let wal_io = io.open_file(wal.path.to_str().unwrap(), OpenFlags::NoLock, false)?;
        if main_io.file_id()? != Some(main.id) || wal_io.file_id()? != Some(wal.id) {
            return Err(invalid("downgrade content alias identity changed"));
        }
        let retained = Self {
            main,
            wal,
            files,
            namespace,
            namespace_file,
            log_path,
            log,
            manifest,
            main_io,
            wal_io,
        };
        retained.revalidate()?;
        Ok(retained)
    }
    pub(in crate::mvcc) fn revalidate(&self) -> Result<()> {
        self.revalidate_inner(false)
    }
    pub(in crate::mvcc) fn revalidate_marker(&self) -> Result<()> {
        self.revalidate_inner(true)
    }
    fn revalidate_inner(&self, marker_stage: bool) -> Result<()> {
        self.main.revalidate()?;
        self.wal.revalidate()?;
        if marker_stage {
            let marker = super::read_marker(&self.main.file)?;
            if !matches!(marker, SHARED_MVCC_VERSION | 2) {
                return Err(invalid("downgrade marker changed unexpectedly"));
            }
            require_marker(&self.main.file, marker)?;
        } else {
            require_marker(&self.main.file, SHARED_MVCC_VERSION)?;
        }
        let metadata = std::fs::symlink_metadata(&self.namespace).map_err(fs_error)?;
        if !metadata.is_dir()
            || identity(&self.namespace_file)?
                != (FileId {
                    dev: metadata.dev(),
                    ino: metadata.ino(),
                })
        {
            return Err(invalid("downgrade namespace changed"));
        }
        for file in &self.files {
            file.revalidate()?;
        }
        if self.manifest.file_id()? != Some(self.files[0].id)
            || self.main_io.file_id()? != Some(self.main.id)
            || self.wal_io.file_id()? != Some(self.wal.id)
            || (!marker_stage && self.wal.file.metadata().map_err(fs_error)?.len() != 0)
            || self.files[1].file.metadata().map_err(fs_error)?.len() != 0
            || self.files[2].file.metadata().map_err(fs_error)?.len() != 0
        {
            return Err(invalid("downgrade requires empty ordinary and shared logs"));
        }
        require_absent(&PathBuf::from(format!("{}-tshm", self.main.path.display())))?;
        if let Some(log) = &self.log {
            log.revalidate()?;
            let length = log.file.metadata().map_err(fs_error)?.len();
            if length > LOG_HDR_SIZE as u64 {
                return Err(invalid("ordinary logical log contains frames"));
            }
            let mut bytes = vec![0; length as usize];
            log.file.read_exact_at(&mut bytes, 0).map_err(fs_error)?;
            LogHeader::validate_header_only_file(&bytes)?;
        } else {
            require_absent(&self.log_path)?;
        }
        Ok(())
    }
    pub(in crate::mvcc) fn path(&self) -> &Path {
        &self.main.path
    }
    pub(in crate::mvcc) fn main_file(&self) -> Arc<dyn IoFile> {
        self.main_io.clone()
    }
    pub(in crate::mvcc) fn wal_file(&self) -> Arc<dyn IoFile> {
        self.wal_io.clone()
    }
    pub(in crate::mvcc) fn require_mirrored(
        &self,
        record: crate::mvcc::commit_manifest::ManifestRecord,
    ) -> Result<()> {
        use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE, SLOT_SIZE};
        if self.files[0].file.metadata().map_err(fs_error)?.len() != (2 * SLOT_SIZE) as u64 {
            return Err(invalid("downgrade manifest length changed"));
        }
        for offset in [0, SLOT_SIZE] {
            let mut bytes = [0; RECORD_SIZE];
            self.files[0]
                .file
                .read_exact_at(&mut bytes, offset as u64)
                .map_err(fs_error)?;
            if ManifestRecord::decode(&bytes)? != Some(record) {
                return Err(invalid("downgrade terminal intent is not mirrored"));
            }
        }
        Ok(())
    }
    pub(in crate::mvcc) fn manifest(&self) -> Arc<dyn IoFile> {
        self.manifest.clone()
    }
    pub(in crate::mvcc) fn wal_id(&self) -> FileId {
        self.wal.id
    }
}
fn require_absent(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Err(invalid("unexpected ordinary sidecar during downgrade")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(fs_error(error)),
    }
}
fn invalid(message: &str) -> LimboError {
    LimboError::InvalidArgument(message.into())
}
