use super::ordinary::{lock_exclusive, open_native};
use super::{
    fs_error, identity, namespace_for, require_marker, require_path_identity, OfflinePrerequisite,
    SHARED_MVCC_VERSION,
};
use crate::io::FileId;
use crate::mvcc::checkpoint_gate::{CheckpointAdmissionGate, CheckpointPause};
use crate::mvcc::commit_manifest::{select_latest, ManifestRecord, RECORD_SIZE, SLOT_SIZE};
use crate::mvcc::persistent_storage::logical_log::{LogHeader, LOG_HDR_SIZE};
use crate::mvcc::shared_authority::{
    AuthorityGuard, PublishedPrefix, SharedMvccAuthority, MAX_TIMESTAMP,
};
use crate::mvcc::shared_ids::{DatabaseGeneration, InitialIds, MIN_HISTORY_LIMIT};
use crate::storage::shared_wal_coordination::MappedSharedWalCoordination;
use crate::sync::Arc;
use crate::{LimboError, Result, IO};
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

struct RetainedFile {
    path: PathBuf,
    file: File,
    id: FileId,
}
impl RetainedFile {
    fn open(path: PathBuf) -> Result<Self> {
        let file = open_native(&path)?;
        let id = identity(&file)?;
        Ok(Self { path, file, id })
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
            return Err(invalid("retained recovery file identity changed"));
        }
        Ok(())
    }
    fn read_exact(&self, length: usize) -> Result<Vec<u8>> {
        if self.file.metadata().map_err(fs_error)?.len() != length as u64 {
            return Err(invalid("prepared recovery file has unexpected length"));
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length)?;
        bytes.resize(length, 0);
        self.file.read_exact_at(&mut bytes, 0).map_err(fs_error)?;
        Ok(bytes)
    }
}

/// Keeps both old-format exclusion and new-format admission closed while the
/// private WAL-only opener completes an already prepared marker transaction.
pub(crate) struct PreparedMarkerRecovery {
    guard: AuthorityGuard,
    _pause: CheckpointPause,
    prefix: PublishedPrefix,
    logical_floor: i64,
    namespace: PathBuf,
    namespace_file: File,
    files: Vec<RetainedFile>,
    ordinary_log: Option<RetainedFile>,
    ordinary_log_path: PathBuf,
    ordinary_wal: RetainedFile,
    main: RetainedFile,
}

impl PreparedMarkerRecovery {
    pub(super) fn open(
        path: &Path,
        history_limit: u64,
        _: OfflinePrerequisite,
    ) -> Result<Arc<Self>> {
        super::require_linux_platform()?;
        if history_limit < MIN_HISTORY_LIMIT {
            return Err(invalid("prepared recovery history limit is too small"));
        }
        let mut main = RetainedFile::open(path.to_path_buf())?;
        lock_exclusive(&main.file)?;
        main.path = path.canonicalize().map_err(fs_error)?;
        require_path_identity(&main.path, main.id)?;
        let marker = super::read_marker(&main.file)?;
        if !matches!(marker, 255 | SHARED_MVCC_VERSION) {
            return Err(invalid(
                "prepared recovery requires an MVCC or shared marker",
            ));
        }
        require_marker(&main.file, marker)?;
        let namespace = namespace_for(&main.path, main.id)?;
        let namespace_file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
            .open(&namespace)
            .map_err(fs_error)?;
        if identity(&namespace_file)?.dev != main.id.dev {
            return Err(invalid("recovery namespace is on another device"));
        }
        let path_text = main
            .path
            .to_str()
            .ok_or_else(|| invalid("recovery path must be UTF-8"))?;
        let tshm = PathBuf::from(format!("{path_text}-tshm"));
        match std::fs::symlink_metadata(tshm) {
            Ok(_) => {
                return Err(invalid(
                    "ordinary multiprocess WAL is not supported by recovery",
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(fs_error(error)),
        }
        let ordinary_wal = RetainedFile::open(PathBuf::from(format!("{path_text}-wal")))?;
        let ordinary_log_path = main.path.with_extension("db-log");
        let ordinary_log = match std::fs::symlink_metadata(&ordinary_log_path) {
            Ok(_) => Some(RetainedFile::open(ordinary_log_path.clone())?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(fs_error(error)),
        };
        let mut files = Vec::new();
        files.try_reserve_exact(NAMES.len())?;
        for name in NAMES {
            files.push(RetainedFile::open(namespace.join(name))?);
        }
        let mut identities = vec![main.id];
        for file in std::iter::once(&ordinary_wal)
            .chain(ordinary_log.iter())
            .chain(files.iter())
        {
            if file.id.dev != main.id.dev || identities.contains(&file.id) {
                return Err(invalid(
                    "recovery files must have distinct identities on the main device",
                ));
            }
            identities.push(file.id);
        }
        let (prefix, logical_floor) =
            validate_durable_files(main.id, &files, ordinary_log.as_ref())?;
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new()?);
        let gate = CheckpointAdmissionGate::open_existing(&io, &files[4].path, files[4].id)?;
        let pause = gate.try_pause_admission()?;
        let authority = SharedMvccAuthority::open_existing(
            &io,
            files[4]
                .path
                .to_str()
                .ok_or_else(|| invalid("authority path must be UTF-8"))?,
            main.id,
            history_limit,
        )?;
        if authority.file_id()? != Some(files[4].id) {
            return Err(invalid("recovery authority identity changed"));
        }
        let mut guard = authority.try_acquire()?;
        if guard.requires_recovery() {
            guard.recover(prefix)?;
        } else if guard.published()? != prefix {
            return Err(invalid("prepared authority prefix differs from manifest"));
        }
        if !guard.live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        let physical = MappedSharedWalCoordination::open_existing(&io, &files[3].path, 64)?
            .ok_or_else(|| invalid("prepared physical coordination is missing"))?;
        if physical.file_id()? != Some(files[3].id) {
            return Err(invalid("physical coordination identity changed"));
        }
        let physical_snapshot = physical
            .try_snapshot()
            .ok_or_else(|| invalid("physical coordination publication is incomplete"))?;
        if physical_snapshot.max_frame != 0 || physical_snapshot.nbackfills != 0 {
            return Err(invalid("prepared physical coordination contains history"));
        }
        drop(physical);
        let token = Arc::new(Self {
            guard,
            _pause: pause,
            prefix,
            logical_floor,
            namespace,
            namespace_file,
            files,
            ordinary_log,
            ordinary_log_path,
            ordinary_wal,
            main,
        });
        token.revalidate()?;
        Ok(token)
    }
    pub(crate) fn path(&self) -> &Path {
        &self.main.path
    }
    pub(crate) fn database_id(&self) -> FileId {
        self.main.id
    }
    pub(crate) fn ordinary_wal_id(&self) -> FileId {
        self.ordinary_wal.id
    }
    pub(super) fn prefix(&self) -> PublishedPrefix {
        self.prefix
    }
    pub(crate) fn logical_floor(&self) -> i64 {
        self.logical_floor
    }
    pub(crate) fn timestamp(&self) -> u64 {
        self.prefix.committed_timestamp
    }
    pub(crate) fn revalidate(&self) -> Result<()> {
        self.main.revalidate()?;
        self.ordinary_wal.revalidate()?;
        let tshm = PathBuf::from(format!("{}-tshm", self.main.path.display()));
        match std::fs::symlink_metadata(tshm) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(fs_error(error)),
            Ok(_) => {
                return Err(invalid(
                    "ordinary multiprocess WAL appeared during recovery",
                ))
            }
        }
        let metadata = std::fs::symlink_metadata(&self.namespace).map_err(fs_error)?;
        if !metadata.is_dir()
            || (FileId {
                dev: metadata.dev(),
                ino: metadata.ino(),
            }) != identity(&self.namespace_file)?
        {
            return Err(invalid("prepared namespace identity changed"));
        }
        for file in &self.files {
            file.revalidate()?;
        }
        if let Some(file) = &self.ordinary_log {
            file.revalidate()?;
        } else {
            match std::fs::symlink_metadata(&self.ordinary_log_path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(fs_error(error)),
                Ok(_) => return Err(invalid("ordinary logical log appeared during recovery")),
            }
        }
        let marker = super::read_marker(&self.main.file)?;
        if !matches!(marker, 255 | SHARED_MVCC_VERSION) {
            return Err(invalid("recovery mode marker changed"));
        }
        require_marker(&self.main.file, marker)?;
        if validate_durable_files(self.main.id, &self.files, self.ordinary_log.as_ref())?
            != (self.prefix, self.logical_floor)
        {
            return Err(invalid("prepared namespace prefix changed"));
        }
        if !self.guard.live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        Ok(())
    }
}

fn validate_durable_files(
    database_id: FileId,
    files: &[RetainedFile],
    ordinary_log: Option<&RetainedFile>,
) -> Result<(PublishedPrefix, i64)> {
    let bytes = files[0].read_exact(2 * SLOT_SIZE)?;
    let first = ManifestRecord::decode(&bytes[..RECORD_SIZE])?;
    let second = ManifestRecord::decode(&bytes[SLOT_SIZE..SLOT_SIZE + RECORD_SIZE])?;
    let record = select_latest([first, second], database_id)?.1;
    let prefix = record.prefix();
    if record.sequence() != 1
        || record.log_generation() != 1
        || record.log_salt() != 0
        || prefix.end_offset != 0
        || prefix.running_crc != 0
        || prefix.committed_timestamp >= MAX_TIMESTAMP
    {
        return Err(invalid("recovery requires the initial empty manifest"));
    }
    let mut expected = [0; SLOT_SIZE * 2];
    expected[..RECORD_SIZE]
        .copy_from_slice(&ManifestRecord::new(1, database_id, 1, 0, prefix)?.encode());
    if bytes != expected {
        return Err(invalid("prepared manifest bytes changed"));
    }
    files[1].read_exact(0)?;
    files[2].read_exact(0)?;
    let bank_a = files[5].read_exact(8192)?;
    let bank_b = files[6].read_exact(0)?;
    let selector = files[7].read_exact(8192)?;
    let logical_floor = InitialIds::validate(
        database_id,
        DatabaseGeneration {
            generation: 1,
            checkpoint_timestamp: prefix.committed_timestamp,
        },
        &bank_a,
        &bank_b,
        &selector,
    )?;
    if let Some(log) = ordinary_log {
        let length = log.file.metadata().map_err(fs_error)?.len();
        if length != 0 && length != LOG_HDR_SIZE as u64 {
            return Err(invalid("ordinary logical history remains"));
        }
        LogHeader::validate_header_only_file(&log.read_exact(length as usize)?)?;
    }
    Ok((prefix, logical_floor))
}

fn invalid(message: &str) -> LimboError {
    LimboError::InvalidArgument(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::mode_transition::ordinary::{
        lock_exclusive, open_native, OrdinaryOfflineBackend,
    };
    use crate::mvcc::mode_transition::{
        identity, namespace_for, prepare_namespace, OfflineBackend,
    };

    const LIMIT: u64 = 64 * 1024 * 1024;

    fn prepared() -> (tempfile::TempDir, std::path::PathBuf, u64) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prepared.db");
        {
            let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
            let db = crate::Database::open_file(
                io,
                path.to_str().unwrap(),
                Arc::new(crate::EngineDialect),
            )
            .unwrap();
            let conn = db.connect(crate::ConnectOpts::new()).unwrap();
            conn.execute("PRAGMA journal_mode=mvcc; CREATE TABLE kept(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO kept VALUES(1,'durable')").unwrap();
            conn.close().unwrap();
        }
        let mut backend =
            OrdinaryOfflineBackend::open(&path, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap();
        let timestamp = backend.recover_materialize_sync().unwrap();
        let floor = backend.materialized_logical_floor().unwrap();
        let id = identity(&open_native(&path).unwrap()).unwrap();
        prepare_namespace(
            &namespace_for(&path, id).unwrap(),
            id,
            timestamp,
            floor,
            LIMIT,
        )
        .unwrap();
        drop(backend);
        (dir, path, timestamp)
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn marker_recovery_token_retains_main_exclusion_and_initial_proofs() {
        let (_dir, path, timestamp) = prepared();
        let token =
            PreparedMarkerRecovery::open(&path, LIMIT, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap();
        assert_eq!(token.timestamp(), timestamp);
        token.revalidate().unwrap();
        let competitor = open_native(&path).unwrap();
        assert!(lock_exclusive(&competitor).is_err());
        drop(token);
        lock_exclusive(&competitor).unwrap();
    }
    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn marker_recovery_token_rejects_changed_durable_state() {
        for name in [
            "manifest",
            "logical",
            "physical-wal",
            "ids-a",
            "ids-b",
            "ids-selector",
        ] {
            let (_dir, path, _) = prepared();
            let id = identity(&open_native(&path).unwrap()).unwrap();
            let file = namespace_for(&path, id).unwrap().join(name);
            use std::io::Write;
            std::fs::OpenOptions::new()
                .append(true)
                .open(&file)
                .unwrap()
                .write_all(&[1])
                .unwrap();
            let before = std::fs::read(&file).unwrap();
            assert!(
                PreparedMarkerRecovery::open(
                    &path,
                    LIMIT,
                    OfflinePrerequisite::AllOlderWorkersStopped
                )
                .is_err(),
                "{name}"
            );
            assert_eq!(std::fs::read(&file).unwrap(), before);
        }
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn marker_recovery_token_rejects_interrupted_physical_snapshot() {
        let (_dir, path, _) = prepared();
        let id = identity(&open_native(&path).unwrap()).unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let physical = MappedSharedWalCoordination::open_existing(
            &io,
            &namespace_for(&path, id).unwrap().join("physical-tshm"),
            64,
        )
        .unwrap()
        .unwrap();
        physical.interrupt_snapshot_for_test();
        drop(physical);
        let error =
            PreparedMarkerRecovery::open(&path, LIMIT, OfflinePrerequisite::AllOlderWorkersStopped)
                .err()
                .unwrap();
        assert!(
            error.to_string().contains("publication is incomplete"),
            "{error}"
        );
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn marker_recovery_token_revalidation_rejects_replacement_and_new_tshm() {
        let (_dir, path, _) = prepared();
        let token =
            PreparedMarkerRecovery::open(&path, LIMIT, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap();
        let tshm = PathBuf::from(format!("{}-tshm", path.display()));
        std::fs::write(&tshm, []).unwrap();
        assert!(token.revalidate().is_err());
        std::fs::remove_file(tshm).unwrap();
        token.revalidate().unwrap();
        let manifest = token.namespace.join("manifest");
        std::fs::rename(&manifest, token.namespace.join("old-manifest")).unwrap();
        std::fs::write(
            &manifest,
            std::fs::read(token.namespace.join("old-manifest")).unwrap(),
        )
        .unwrap();
        assert!(token.revalidate().is_err());
    }
    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn marker_recovery_token_rejects_live_registration_and_log_history() {
        let (_dir, path, timestamp) = prepared();
        let id = identity(&open_native(&path).unwrap()).unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let authority = SharedMvccAuthority::open_existing(
            &io,
            namespace_for(&path, id)
                .unwrap()
                .join("authority")
                .to_str()
                .unwrap(),
            id,
            LIMIT,
        )
        .unwrap();
        let mut guard = authority.try_acquire().unwrap();
        if guard.requires_recovery() {
            guard
                .recover(PublishedPrefix {
                    committed_timestamp: timestamp,
                    end_offset: 0,
                    running_crc: 0,
                })
                .unwrap();
        }
        let registration = guard.register_snapshot().unwrap();
        guard.finish().unwrap();
        assert!(matches!(
            PreparedMarkerRecovery::open(&path, LIMIT, OfflinePrerequisite::AllOlderWorkersStopped),
            Err(LimboError::Busy)
        ));
        drop(registration);
        drop(
            PreparedMarkerRecovery::open(&path, LIMIT, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap(),
        );
        std::fs::write(path.with_extension("db-log"), [1u8; 57]).unwrap();
        assert!(PreparedMarkerRecovery::open(
            &path,
            LIMIT,
            OfflinePrerequisite::AllOlderWorkersStopped
        )
        .is_err());
    }
}
