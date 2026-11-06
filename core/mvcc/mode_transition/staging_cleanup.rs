use super::{canonical_parent, fs_error, identity, sync_directory};
use crate::io::FileId;
use crate::mvcc::persistent_storage::logical_log::{LogHeader, LOG_HDR_SIZE};
use crate::{LimboError, Result};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy)]
enum Kind {
    Wal,
    Logical,
}

struct Sidecar {
    path: PathBuf,
    file: Option<File>,
    kind: Kind,
}

impl Sidecar {
    fn capture(path: PathBuf, kind: Kind) -> Result<Self> {
        let file = match fs::symlink_metadata(&path) {
            Ok(_) => Some(open_retained(&path)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(fs_error(error)),
        };
        let sidecar = Self { path, file, kind };
        sidecar.validate()?;
        Ok(sidecar)
    }

    fn validate(&self) -> Result<()> {
        let Some(file) = &self.file else {
            return require_absent(&self.path);
        };
        require_identity(&self.path, identity(file)?)?;
        let length = file.metadata().map_err(fs_error)?.len();
        match self.kind {
            Kind::Wal if length != 0 => return Err(invalid("staging WAL contains history")),
            Kind::Logical if length != 0 => {
                if length != LOG_HDR_SIZE as u64 {
                    return Err(invalid("staging logical log contains history"));
                }
                let mut header = [0; LOG_HDR_SIZE];
                file.read_exact_at(&mut header, 0).map_err(fs_error)?;
                LogHeader::validate_header_only_file(&header)?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// Captured before publication while the backend still owns the staging lease.
/// Cleanup requires operator-owned staging names: identity checks are not an
/// atomic conditional unlink against an uncooperative pathname replacement.
pub(super) struct StagingCleanup {
    staging: PathBuf,
    destination: PathBuf,
    main: File,
    sidecars: [Sidecar; 2],
}

impl StagingCleanup {
    pub(super) fn capture(staging: &Path, destination: &Path) -> Result<Self> {
        let staging = canonical_parent(staging)?.join(
            staging
                .file_name()
                .ok_or_else(|| invalid("staging filename is missing"))?,
        );
        let destination = canonical_parent(destination)?.join(
            destination
                .file_name()
                .ok_or_else(|| invalid("destination filename is missing"))?,
        );
        let main = open_retained(&staging)?;
        let sidecars = [
            Sidecar::capture(
                PathBuf::from(format!("{}-wal", staging.display())),
                Kind::Wal,
            )?,
            Sidecar::capture(staging.with_extension("db-log"), Kind::Logical)?,
        ];
        let main_id = identity(&main)?;
        for sidecar in &sidecars {
            if sidecar
                .file
                .as_ref()
                .map(identity)
                .transpose()?
                .is_some_and(|id| id == main_id || id.dev != main_id.dev)
            {
                return Err(invalid(
                    "staging sidecar aliases main or resides on another device",
                ));
            }
        }
        if let (Some(wal), Some(log)) = (&sidecars[0].file, &sidecars[1].file) {
            if identity(wal)? == identity(log)? {
                return Err(invalid("staging sidecars alias each other"));
            }
        }
        Ok(Self {
            staging,
            destination,
            main,
            sidecars,
        })
    }

    /// Called only after durable publication and full destruction of the backend.
    pub(super) fn finish(self) -> Result<()> {
        require_identity(&self.destination, identity(&self.main)?)?;
        require_absent(&self.staging)?;
        // Validate the complete set before removing its first member.
        for sidecar in &self.sidecars {
            sidecar.validate()?;
        }
        let remove: Result<()> = (|| {
            for sidecar in &self.sidecars {
                if sidecar.file.is_some() {
                    fs::remove_file(&sidecar.path).map_err(fs_error)?;
                }
            }
            Ok(())
        })();
        let sync = sync_directory(self.staging.parent().unwrap());
        remove?;
        sync
    }
}

fn open_retained(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(fs_error)?;
    require_identity(path, identity(&file)?)?;
    Ok(file)
}

fn require_identity(path: &Path, expected: FileId) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(fs_error)?;
    if !metadata.is_file()
        || (FileId {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }) != expected
    {
        return Err(invalid("staging cleanup pathname identity changed"));
    }
    Ok(())
}

fn require_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(fs_error(error)),
        Ok(_) => Err(invalid("staging cleanup found an unexpected pathname")),
    }
}

fn invalid(message: &str) -> LimboError {
    LimboError::InvalidArgument(message.into())
}

#[cfg(test)]
mod tests {
    use crate::mvcc::mode_transition::ordinary::OrdinaryOfflineBackend;
    use crate::mvcc::mode_transition::{
        validate_shared_open, OfflineModeChange, OfflinePrerequisite,
    };
    use std::path::PathBuf;

    const LIMIT: u64 = 64 * 1024 * 1024;

    fn fixture(staging: &std::path::Path) -> OrdinaryOfflineBackend {
        {
            let io: crate::sync::Arc<dyn crate::IO> =
                crate::sync::Arc::new(crate::io::SyscallIO::new().unwrap());
            let db = crate::Database::open_file(
                io,
                staging.to_str().unwrap(),
                crate::sync::Arc::new(crate::EngineDialect),
            )
            .unwrap();
            let conn = db.connect(crate::ConnectOpts::new()).unwrap();
            conn.execute("PRAGMA journal_mode=mvcc; CREATE TABLE kept(id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.close().unwrap();
        }
        OrdinaryOfflineBackend::open(staging, OfflinePrerequisite::AllOlderWorkersStopped).unwrap()
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn staging_cleanup_fresh_publication_removes_owned_ordinary_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging.db");
        let destination = dir.path().join("published.db");
        let backend = fixture(&staging);
        OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, LIMIT)
            .publish_new(&destination)
            .unwrap();
        validate_shared_open(&destination).unwrap();
        assert!(!staging.exists());
        for path in [
            PathBuf::from(format!("{}-wal", staging.display())),
            staging.with_extension("db-log"),
        ] {
            assert!(
                !path.exists(),
                "owned staging sidecar remained after publication: {}",
                path.display()
            );
        }
    }
    struct MutateAfterClose {
        backend: Option<OrdinaryOfflineBackend>,
        staging: PathBuf,
        destination: PathBuf,
        action: u8,
    }
    impl crate::mvcc::mode_transition::OfflineBackend for MutateAfterClose {
        fn path(&self) -> &std::path::Path {
            &self.staging
        }
        fn recover_materialize_sync(&mut self) -> crate::Result<u64> {
            self.backend.as_mut().unwrap().recover_materialize_sync()
        }
        fn materialized_logical_floor(&mut self) -> crate::Result<i64> {
            self.backend.as_mut().unwrap().materialized_logical_floor()
        }
        fn commit_marker_checkpoint_sync(&mut self, marker: u8) -> crate::Result<()> {
            self.backend
                .as_mut()
                .unwrap()
                .commit_marker_checkpoint_sync(marker)
        }
    }
    impl Drop for MutateAfterClose {
        fn drop(&mut self) {
            drop(self.backend.take());
            match self.action {
                0 => {
                    let log = self.staging.with_extension("db-log");
                    std::fs::rename(&log, self.staging.with_extension("saved-log")).unwrap();
                    std::fs::write(log, []).unwrap();
                }
                1 => std::fs::write(&self.staging, b"replacement main").unwrap(),
                2 => {
                    std::fs::rename(&self.destination, self.staging.with_extension("saved-main"))
                        .unwrap();
                    std::fs::write(&self.destination, b"replacement destination").unwrap();
                }
                3 => std::fs::write(format!("{}-wal", self.staging.display()), b"new history")
                    .unwrap(),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn staging_cleanup_revalidates_all_files_after_backend_drop_before_any_unlink() {
        for action in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let staging = dir.path().join("staging.db");
            let destination = dir.path().join("published.db");
            let backend = MutateAfterClose {
                backend: Some(fixture(&staging)),
                staging: staging.clone(),
                destination: destination.clone(),
                action,
            };
            let error =
                OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, LIMIT)
                    .publish_new(&destination)
                    .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("destination was published but staging cleanup failed"),
                "{error}"
            );
            // Replacing the second sidecar must preserve the first one too.
            assert!(PathBuf::from(format!("{}-wal", staging.display())).exists());
            assert!(staging.with_extension("db-log").exists());
            match action {
                0 => assert_eq!(
                    std::fs::read(staging.with_extension("db-log")).unwrap(),
                    b""
                ),
                1 => assert_eq!(std::fs::read(&staging).unwrap(), b"replacement main"),
                2 => assert_eq!(
                    std::fs::read(&destination).unwrap(),
                    b"replacement destination"
                ),
                3 => assert_eq!(
                    std::fs::read(format!("{}-wal", staging.display())).unwrap(),
                    b"new history"
                ),
                _ => unreachable!(),
            }
            if action != 2 {
                validate_shared_open(&destination).unwrap();
            }
        }
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn staging_cleanup_failed_publication_preserves_destination_and_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging.db");
        let destination = dir.path().join("published.db");
        let backend = fixture(&staging);
        std::fs::write(&destination, b"existing destination").unwrap();
        let error =
            OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, LIMIT)
                .publish_new(&destination)
                .unwrap_err();
        assert!(!error.to_string().contains("was published"));
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"existing destination"
        );
        assert!(staging.exists());
        assert!(PathBuf::from(format!("{}-wal", staging.display())).exists());
        assert!(staging.with_extension("db-log").exists());
    }
}
