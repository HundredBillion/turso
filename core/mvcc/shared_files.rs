use super::commit_manifest::SLOT_SIZE;
use super::mode_transition::{namespace_for, require_marker, SHARED_MVCC_VERSION};
use super::shared_authority::SharedMvccAuthority;
use crate::io::{File, FileId};
use crate::storage::shared_wal_coordination::MappedSharedWalCoordination;
use crate::sync::{Arc, LazyLock, Mutex, Weak};
use crate::{HashMap, LimboError, OpenFlags, Result, IO};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const EXT_FAMILY_MAGIC: i64 = 0xef53;

struct PhysicalCoordinationEntry {
    database_id: FileId,
    mapping: Weak<MappedSharedWalCoordination>,
}

static PHYSICAL_COORDINATION: LazyLock<Mutex<HashMap<FileId, PhysicalCoordinationEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::default()));

pub(super) struct SharedMvccFiles {
    pub main: Arc<dyn File>,
    pub logical: Arc<dyn File>,
    pub manifest: Arc<dyn File>,
    pub physical_wal: Arc<dyn File>,
    pub physical_coordination: Arc<MappedSharedWalCoordination>,
    pub authority: Arc<SharedMvccAuthority>,
    pub namespace: PathBuf,
    pub id_banks: [Arc<dyn File>; 2],
    pub id_selector: Arc<dyn File>,
}

impl SharedMvccFiles {
    /// The caller excludes custom durable storage and opens a read-only main handle when requested.
    /// Retained files are not a recovered snapshot; admission must validate the manifest under authority.
    pub(super) fn open_existing(
        io: Arc<dyn IO>,
        main: Arc<dyn File>,
        database_id: FileId,
        canonical_path: &Path,
        history_limit: u64,
        readonly_content: bool,
    ) -> Result<Self> {
        if !cfg!(all(target_os = "linux", target_pointer_width = "64"))
            || !io.supports_shared_wal_coordination()
        {
            return Err(LimboError::InvalidArgument(
                "shared MVCC files require a native 64-bit Linux backend".into(),
            ));
        }
        if history_limit == 0 {
            return Err(LimboError::InvalidArgument(
                "shared MVCC history limit must be positive".into(),
            ));
        }
        if !canonical_path.is_absolute()
            || canonical_path.canonicalize().map_err(fs_error)? != canonical_path
        {
            return Err(LimboError::InvalidArgument(
                "shared MVCC open requires the canonical database path".into(),
            ));
        }
        require_identity(main.file_id()?, database_id)?;
        let main_native = NativeFile::open(canonical_path, database_id.dev)?;
        require_identity(Some(main_native.id), database_id)?;
        require_marker(&main_native.file, SHARED_MVCC_VERSION)?;
        let namespace = namespace_for(canonical_path, database_id)?;
        if !std::fs::symlink_metadata(&namespace)
            .map_err(fs_error)?
            .is_dir()
        {
            return Err(LimboError::InvalidArgument(
                "shared MVCC namespace must be a real directory".into(),
            ));
        }
        let logical_native = NativeFile::open(&namespace.join("logical"), database_id.dev)?;
        let manifest_native = NativeFile::open(&namespace.join("manifest"), database_id.dev)?;
        let wal_native = NativeFile::open(&namespace.join("physical-wal"), database_id.dev)?;
        let coordination_native =
            NativeFile::open(&namespace.join("physical-tshm"), database_id.dev)?;
        let authority_native = NativeFile::open(&namespace.join("authority"), database_id.dev)?;
        let ids_native = [
            NativeFile::open(&namespace.join("ids-a"), database_id.dev)?,
            NativeFile::open(&namespace.join("ids-b"), database_id.dev)?,
            NativeFile::open(&namespace.join("ids-selector"), database_id.dev)?,
        ];
        let other_ids = [
            database_id,
            logical_native.id,
            manifest_native.id,
            wal_native.id,
            coordination_native.id,
            authority_native.id,
        ];
        for (index, file) in ids_native.iter().enumerate() {
            if other_ids.contains(&file.id)
                || ids_native[..index].iter().any(|other| other.id == file.id)
            {
                return Err(LimboError::InvalidArgument(
                    "shared ID journals must have distinct native file identities".into(),
                ));
            }
        }
        if manifest_native.file.metadata().map_err(fs_error)?.len() != (SLOT_SIZE * 2) as u64 {
            return Err(LimboError::Corrupt(
                "invalid shared MVCC manifest length".into(),
            ));
        }
        let content_flags = if readonly_content {
            OpenFlags::NoLock | OpenFlags::ReadOnly
        } else {
            OpenFlags::NoLock
        };
        let logical = open_verified(
            &io,
            &namespace.join("logical"),
            content_flags,
            logical_native.id,
        )?;
        let id_banks = [
            open_verified(
                &io,
                &namespace.join("ids-a"),
                OpenFlags::NoLock,
                ids_native[0].id,
            )?,
            open_verified(
                &io,
                &namespace.join("ids-b"),
                OpenFlags::NoLock,
                ids_native[1].id,
            )?,
        ];
        let id_selector = open_verified(
            &io,
            &namespace.join("ids-selector"),
            OpenFlags::NoLock,
            ids_native[2].id,
        )?;
        let manifest = open_verified(
            &io,
            &namespace.join("manifest"),
            OpenFlags::NoLock,
            manifest_native.id,
        )?;
        let physical_wal = open_verified(
            &io,
            &namespace.join("physical-wal"),
            content_flags,
            wal_native.id,
        )?;
        let physical_coordination = open_physical_coordination(
            &io,
            &namespace.join("physical-tshm"),
            coordination_native.id,
            database_id,
        )?;
        let authority = SharedMvccAuthority::open_existing(
            &io,
            path_string(&namespace.join("authority"))?,
            database_id,
            history_limit,
        )?;
        require_identity(authority.file_id()?, authority_native.id)?;
        Ok(Self {
            main,
            logical,
            manifest,
            physical_wal,
            physical_coordination,
            authority,
            namespace,
            id_banks,
            id_selector,
        })
    }
}

fn open_physical_coordination(
    io: &Arc<dyn IO>,
    path: &Path,
    coordination_id: FileId,
    database_id: FileId,
) -> Result<Arc<MappedSharedWalCoordination>> {
    // Reader ownership and local lock counts must belong to the same mapping.
    // Keep database instances independent so their content permissions stay separate.
    let mut mappings = PHYSICAL_COORDINATION.lock();
    if let Some(entry) = mappings.get(&coordination_id) {
        if let Some(mapping) = entry.mapping.upgrade() {
            if entry.database_id != database_id {
                return Err(LimboError::InvalidArgument(
                    "physical coordination belongs to another database".into(),
                ));
            }
            require_identity(mapping.file_id()?, coordination_id)?;
            return Ok(mapping);
        }
    }
    mappings.retain(|_, entry| entry.mapping.strong_count() != 0);
    mappings.try_reserve(1)?;
    let mapping = Arc::new(
        MappedSharedWalCoordination::open_existing(io, path, 64)?.ok_or_else(|| {
            LimboError::Corrupt("published physical coordination is missing".into())
        })?,
    );
    require_identity(mapping.file_id()?, coordination_id)?;
    mappings.insert(
        coordination_id,
        PhysicalCoordinationEntry {
            database_id,
            mapping: Arc::downgrade(&mapping),
        },
    );
    Ok(mapping)
}

fn open_verified(
    io: &Arc<dyn IO>,
    path: &Path,
    flags: OpenFlags,
    id: FileId,
) -> Result<Arc<dyn File>> {
    let file = io.open_file(path_string(path)?, flags, false)?;
    require_identity(file.file_id()?, id)?;
    Ok(file)
}

fn require_identity(actual: Option<FileId>, expected: FileId) -> Result<()> {
    if actual != Some(expected) {
        return Err(LimboError::InvalidArgument(
            "shared MVCC retained file identity does not match native storage".into(),
        ));
    }
    Ok(())
}

fn path_string(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| LimboError::InvalidArgument("shared MVCC path must be UTF-8".into()))
}

struct NativeFile {
    file: std::fs::File,
    id: FileId,
}

impl NativeFile {
    fn open(path: &Path, device: u64) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(fs_error)?;
        let metadata = file.metadata().map_err(fs_error)?;
        if !metadata.is_file() || metadata.dev() != device {
            return Err(LimboError::InvalidArgument(
                "shared MVCC files must be regular files on the database device".into(),
            ));
        }
        let magic = filesystem_magic(&file)?;
        if !filesystem_supported(magic) {
            return Err(LimboError::InvalidArgument(format!("shared MVCC filesystem magic {magic:#x} is not validated; initial support requires the ext family")));
        }
        Ok(Self {
            file,
            id: FileId {
                dev: metadata.dev(),
                ino: metadata.ino(),
            },
        })
    }
}

fn filesystem_supported(magic: i64) -> bool {
    magic == EXT_FAMILY_MAGIC
}

#[cfg(target_os = "linux")]
fn filesystem_magic(file: &std::fs::File) -> Result<i64> {
    use std::os::fd::AsRawFd;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(fs_error(std::io::Error::last_os_error()));
    }
    Ok(unsafe { stat.assume_init() }.f_type)
}

#[cfg(not(target_os = "linux"))]
fn filesystem_magic(_: &std::fs::File) -> Result<i64> {
    Err(LimboError::InvalidArgument(
        "shared MVCC files require 64-bit Linux".into(),
    ))
}

fn fs_error(error: std::io::Error) -> LimboError {
    crate::error::io_error(error, "open existing shared MVCC files")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    fn native_fixture() -> (
        tempfile::TempDir,
        PathBuf,
        Arc<dyn IO>,
        Arc<dyn File>,
        FileId,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = super::super::mode_transition::tests::fixture(dir.path());
        let path = path.canonicalize().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let main = io
            .open_file(
                path.to_str().unwrap(),
                OpenFlags::NoLock | OpenFlags::ReadOnly,
                false,
            )
            .unwrap();
        let id = main.file_id().unwrap().unwrap();
        let native = std::fs::File::open(&path).unwrap();
        eprintln!(
            "shared files native fixture filesystem_magic={:#x}",
            filesystem_magic(&native).unwrap()
        );
        (dir, path, io, main, id)
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn shared_files_retains_native_handles_and_enforces_readonly_content() {
        let (_dir, path, io, main, id) = native_fixture();
        let namespace = namespace_for(&path, id).unwrap();
        let content = [
            path.clone(),
            namespace.join("logical"),
            namespace.join("physical-wal"),
            namespace.join("manifest"),
        ];
        let before: Vec<_> = content.iter().map(|p| std::fs::read(p).unwrap()).collect();
        let files = SharedMvccFiles::open_existing(io, main, id, &path, 65536, true).unwrap();
        assert_eq!(files.namespace, namespace);
        assert_eq!(files.main.file_id().unwrap(), Some(id));
        for file in [&files.main, &files.logical, &files.physical_wal] {
            let buffer = Arc::new(crate::Buffer::new_temporary(1));
            buffer.as_mut_slice()[0] = 99;
            assert!(file
                .pwrite(0, buffer, crate::Completion::new_write(|_| {}))
                .is_err());
        }
        let after: Vec<_> = content.iter().map(|p| std::fs::read(p).unwrap()).collect();
        assert_eq!(before, after);
        assert_eq!(
            files.manifest.file_id().unwrap(),
            Some(crate::io::get_file_id(namespace.join("manifest").to_str().unwrap()).unwrap())
        );
        assert_eq!(
            files.physical_coordination.file_id().unwrap(),
            Some(
                crate::io::get_file_id(namespace.join("physical-tshm").to_str().unwrap()).unwrap()
            )
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn shared_files_reuses_only_matching_database_coordination_until_last_drop() {
        let (_dir, path, io, main, id) = native_fixture();
        let first =
            SharedMvccFiles::open_existing(io.clone(), main.clone(), id, &path, 65536, true)
                .unwrap();
        let second =
            SharedMvccFiles::open_existing(io.clone(), main.clone(), id, &path, 65536, false)
                .unwrap();
        assert!(Arc::ptr_eq(
            &first.physical_coordination,
            &second.physical_coordination
        ));
        assert!(!Arc::ptr_eq(&first.logical, &second.logical));
        let weak = Arc::downgrade(&first.physical_coordination);

        let (_other_dir, other_path, other_io, other_main, other_id) = native_fixture();
        let other_namespace = namespace_for(&other_path, other_id).unwrap();
        let other_coordination = other_namespace.join("physical-tshm");
        std::fs::remove_file(&other_coordination).unwrap();
        std::fs::hard_link(first.namespace.join("physical-tshm"), &other_coordination).unwrap();
        let before = std::fs::read(&other_coordination).unwrap();
        let error = SharedMvccFiles::open_existing(
            other_io,
            other_main,
            other_id,
            &other_path,
            65536,
            true,
        )
        .err()
        .expect("another database must not reuse a live coordination mapping");
        assert!(error
            .to_string()
            .contains("physical coordination belongs to another database"));
        assert_eq!(std::fs::read(&other_coordination).unwrap(), before);

        drop(first);
        assert!(weak.upgrade().is_some());
        drop(second);
        assert!(weak.upgrade().is_none());
        let reopened = SharedMvccFiles::open_existing(io, main, id, &path, 65536, true).unwrap();
        assert_eq!(
            reopened.physical_coordination.file_id().unwrap(),
            Some(crate::io::get_file_id(other_coordination.to_str().unwrap()).unwrap())
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn shared_files_id_journals_are_writable_for_readonly_participants() {
        let (_dir, path, io, main, id) = native_fixture();
        let namespace = namespace_for(&path, id).unwrap();
        let paths = ["ids-a", "ids-b", "ids-selector"].map(|name| namespace.join(name));
        let before = paths.each_ref().map(|path| std::fs::read(path).unwrap());
        let files =
            SharedMvccFiles::open_existing(io.clone(), main, id, &path, 65536, true).unwrap();
        let _guard = files.authority.try_acquire().unwrap();
        for (file, index) in [(&files.id_banks[0], 0), (&files.id_selector, 2)] {
            let buffer = Arc::new(crate::Buffer::new_temporary(before[index].len()));
            buffer.as_mut_slice().copy_from_slice(&before[index]);
            let completion = file
                .pwrite(0, buffer, crate::Completion::new_write(|_| {}))
                .unwrap();
            io.wait_for_completion(completion).unwrap();
            assert_eq!(
                file.file_id().unwrap(),
                Some(crate::io::get_file_id(paths[index].to_str().unwrap()).unwrap())
            );
        }
        let completion = files.id_banks[1]
            .truncate(0, crate::Completion::new_trunc(|_| {}))
            .unwrap();
        io.wait_for_completion(completion).unwrap();
        assert_eq!(
            paths.each_ref().map(|path| std::fs::read(path).unwrap()),
            before
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn shared_files_rejects_id_aliases_and_symlinks_without_mutation() {
        for (target, source, symlink) in [
            ("ids-a", "ids-b", false),
            ("ids-selector", "manifest", false),
            ("ids-b", "main", false),
            ("ids-a", "ids-b", true),
        ] {
            let (_dir, path, io, main, id) = native_fixture();
            let namespace = namespace_for(&path, id).unwrap();
            let target = namespace.join(target);
            let source = if source == "main" {
                path.clone()
            } else {
                namespace.join(source)
            };
            std::fs::remove_file(&target).unwrap();
            if symlink {
                std::os::unix::fs::symlink(&source, &target).unwrap();
            } else {
                std::fs::hard_link(&source, &target).unwrap();
            }
            let main_before = std::fs::read(&path).unwrap();
            let before: Vec<_> = std::fs::read_dir(&namespace)
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    let bytes = std::fs::read(&path).unwrap();
                    (path, bytes)
                })
                .collect();
            assert!(SharedMvccFiles::open_existing(io, main, id, &path, 65536, true).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), main_before);
            for (path, bytes) in before {
                assert_eq!(std::fs::read(path).unwrap(), bytes);
            }
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn shared_files_ordinary_database_is_not_implicitly_upgraded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordinary.db");
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let db = crate::Database::open_file(
            io.clone(),
            path.to_str().unwrap(),
            Arc::new(crate::EngineDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE preserved(v); INSERT INTO preserved VALUES(42)")
            .unwrap();
        conn.checkpoint(crate::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
        conn.close().unwrap();
        drop(conn);
        drop(db);
        let path = path.canonicalize().unwrap();
        let main = io
            .open_file(
                path.to_str().unwrap(),
                OpenFlags::NoLock | OpenFlags::ReadOnly,
                false,
            )
            .unwrap();
        let id = main.file_id().unwrap().unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(SharedMvccFiles::open_existing(io, main, id, &path, 65536, true).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(!namespace_for(&path, id).unwrap().exists());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn shared_files_missing_sidecars_fail_without_creating_or_mutating_state() {
        for missing in [
            "logical",
            "manifest",
            "physical-wal",
            "physical-tshm",
            "authority",
            "ids-a",
            "ids-b",
            "ids-selector",
        ] {
            let (_dir, path, io, main, id) = native_fixture();
            let namespace = namespace_for(&path, id).unwrap();
            std::fs::remove_file(namespace.join(missing)).unwrap();
            let before: Vec<_> = std::fs::read_dir(&namespace)
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    let bytes = std::fs::read(&path).unwrap();
                    (path, bytes)
                })
                .collect();
            assert!(SharedMvccFiles::open_existing(io, main, id, &path, 65536, false).is_err());
            assert!(!namespace.join(missing).exists());
            for (path, bytes) in before {
                assert_eq!(std::fs::read(path).unwrap(), bytes);
            }
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn shared_files_live_mapping_unlink_cannot_create_replacement_authority() {
        let (_dir, path, io, main, id) = native_fixture();
        let live = SharedMvccFiles::open_existing(io.clone(), main.clone(), id, &path, 65536, true)
            .unwrap();
        let authority_id = live.authority.file_id().unwrap();
        let authority_path = live.namespace.join("authority");
        std::fs::remove_file(&authority_path).unwrap();
        assert!(SharedMvccFiles::open_existing(io, main, id, &path, 65536, true).is_err());
        assert!(!authority_path.exists());
        assert_eq!(live.authority.file_id().unwrap(), authority_id);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn shared_files_rejects_wrong_main_identity_and_invalid_manifest_length() {
        let (_dir, path, io, main, id) = native_fixture();
        let namespace = namespace_for(&path, id).unwrap();
        let before = std::fs::read(namespace.join("authority")).unwrap();
        assert!(SharedMvccFiles::open_existing(
            io.clone(),
            main.clone(),
            FileId {
                ino: id.ino + 1,
                ..id
            },
            &path,
            65536,
            true
        )
        .is_err());
        assert_eq!(std::fs::read(namespace.join("authority")).unwrap(), before);
        std::fs::write(namespace.join("manifest"), [0; SLOT_SIZE]).unwrap();
        assert!(SharedMvccFiles::open_existing(io, main, id, &path, 65536, true).is_err());
        assert_eq!(std::fs::read(namespace.join("authority")).unwrap(), before);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn shared_files_rejects_missing_coordination_write_permission() {
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(
            unsafe { libc::geteuid() },
            0,
            "permission-denial fixture requires an unprivileged test process"
        );
        let (_dir, path, io, main, id) = native_fixture();
        let manifest = namespace_for(&path, id).unwrap().join("manifest");
        std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(0o400)).unwrap();
        let before = std::fs::read(&manifest).unwrap();
        assert!(SharedMvccFiles::open_existing(io, main, id, &path, 65536, true).is_err());
        assert_eq!(std::fs::read(&manifest).unwrap(), before);
    }

    #[test]
    fn shared_files_filesystem_policy_rejects_unknown_network_and_overlay() {
        assert!(filesystem_supported(0xef53));
        for magic in [0, 0x6969, 0x65735546, 0x794c7630, 0x01021994, -1] {
            assert!(!filesystem_supported(magic));
        }
    }

    #[test]
    fn shared_files_rejects_non_native_backend_without_creating_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uncreated.db");
        let io: Arc<dyn IO> = Arc::new(crate::MemoryIO::new());
        let main = io.open_file("memory", OpenFlags::Create, false).unwrap();
        assert!(SharedMvccFiles::open_existing(
            io,
            main,
            FileId { dev: 0, ino: 0 },
            &path,
            65536,
            false
        )
        .is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
