use super::{downgrade_files::DowngradeFiles, downgrade_marker, OfflinePrerequisite};
use crate::mvcc::checkpoint_gate::{CheckpointAdmissionGate, CheckpointPause};
use crate::mvcc::commit_manifest::{ManifestLoad, ManifestRecord, RECORD_SIZE, SLOT_SIZE};
use crate::mvcc::guarded_file::GuardedFile;
use crate::mvcc::shared_authority::{AuthorityGuard, SharedMvccAuthority};
use crate::mvcc::shared_ids::io_protocol::checkpoint_recovery::SuspendedRecoveryIds;
use crate::mvcc::shared_ids::io_protocol::{RecoveryIdentity, SharedIds};
use crate::mvcc::shared_ids::{DatabaseGeneration, MIN_HISTORY_LIMIT};
use crate::storage::shared_wal_coordination::MappedSharedWalCoordination;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::Arc;
use crate::{IOExt, LimboError, OpenFlags, Result, IO};
use std::os::unix::fs::FileExt;
use std::path::Path;

pub(super) struct RecoveredDowngradeSource {
    pub(super) files: DowngradeFiles,
    pub(super) record: ManifestRecord,
    guard: AuthorityGuard,
    header: DatabaseHeader,
    bytes: u64,
    pub(super) logical_floor: i64,
    _ids: SuspendedRecoveryIds,
    _pause: CheckpointPause,
    #[cfg(test)]
    mirror_syncs: std::sync::atomic::AtomicUsize,
}
impl RecoveredDowngradeSource {
    fn open(path: &Path, history_limit: u64) -> Result<Self> {
        if history_limit < MIN_HISTORY_LIMIT {
            return Err(invalid("downgrade recovery history limit is too small"));
        }
        let files = DowngradeFiles::open_terminal(path)?;
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new()?);
        let authority_file = &files.files[4];
        let gate =
            CheckpointAdmissionGate::open_existing(&io, &authority_file.path, authority_file.id)?;
        let pause = gate.try_pause_admission()?;
        let authority = SharedMvccAuthority::open_existing(
            &io,
            authority_file
                .path
                .to_str()
                .ok_or_else(|| invalid("authority path must be UTF-8"))?,
            files.main.id,
            history_limit,
        )?;
        if authority.file_id()? != Some(authority_file.id) {
            return Err(invalid("downgrade authority identity changed"));
        }
        let guard = authority.try_acquire()?;
        guard.ensure_no_snapshot_locks()?;
        let mut loading = ManifestLoad::new(files.manifest(), guard, files.main.id)?;
        let loaded = io.block(|| Ok(loading.poll()?))?;
        drop(loading);
        let record = loaded.record();
        if record
            .downgrade_intent()
            .is_none_or(|intent| intent.basename_wal_id != files.wal_id())
            || record.log_generation() == 0
            || record.log_salt() != 0
            || record.prefix().end_offset != 0
            || record.prefix().running_crc != 0
        {
            return Err(invalid(
                "downgrade recovery requires the recorded terminal WAL",
            ));
        }
        let guard = loaded.into_guard()?;
        let physical = MappedSharedWalCoordination::open_existing(&io, &files.files[3].path, 64)?
            .ok_or_else(|| invalid("downgrade physical coordination is missing"))?;
        if physical.file_id()? != Some(files.files[3].id) {
            return Err(invalid("downgrade physical coordination identity changed"));
        }
        let snapshot = physical
            .try_snapshot()
            .ok_or_else(|| invalid("incomplete physical publication"))?;
        if snapshot.max_frame != 0 || snapshot.nbackfills != 0 {
            return Err(invalid("downgrade shared physical history is not empty"));
        }
        drop(physical);
        let mut raw = [0u8; DatabaseHeader::SIZE];
        files
            .main
            .file
            .read_exact_at(&mut raw, 0)
            .map_err(super::fs_error)?;
        let header: DatabaseHeader = bytemuck::pod_read_unaligned(&raw);
        let bytes = files.main.file.metadata().map_err(super::fs_error)?.len();
        let page_size = header.page_size.get();
        let pages = header.database_size.get();
        if header.magic != *b"SQLite format 3\0"
            || pages == 0
            || !(512..=65536).contains(&page_size)
            || !page_size.is_power_of_two()
            || header.reserved_space as u32 >= page_size
            || u64::from(pages) * u64::from(page_size) != bytes
        {
            return Err(invalid("terminal downgrade physical extent is invalid"));
        }
        let open_id_file = |index: usize| -> Result<_> {
            let retained = &files.files[index];
            let file = io.open_file(
                retained
                    .path
                    .to_str()
                    .ok_or_else(|| invalid("ID path must be UTF-8"))?,
                OpenFlags::NoLock,
                false,
            )?;
            if file.file_id()? != Some(retained.id) {
                return Err(invalid("downgrade ID alias identity changed"));
            }
            Ok(GuardedFile::new(file, &authority))
        };
        let ids = SharedIds::open_existing(
            [open_id_file(5)?, open_id_file(6)?],
            open_id_file(7)?,
            files.main.id,
            history_limit,
        )?;
        let identity = RecoveryIdentity {
            database: DatabaseGeneration {
                generation: record.log_generation(),
                checkpoint_timestamp: record.prefix().committed_timestamp,
            },
            prefix: record.prefix(),
        };
        let mut inspection = ids.inspect_completed_target(guard, identity, -i64::from(pages))?;
        let inspected = io.block(|| Ok(inspection.poll()?))?;
        drop(inspection);
        if inspected.has_pending_checkpoint() || inspected.source_database().is_some() {
            return Err(invalid("terminal downgrade IDs are not finalized"));
        }
        let logical_floor = inspected.logical_floor();
        let (ids, guard) = inspected.suspend();
        if !guard.io_retired() {
            return Err(LimboError::Busy);
        }
        let result = Self {
            files,
            record,
            guard,
            header,
            bytes,
            logical_floor,
            _ids: ids,
            _pause: pause,
            #[cfg(test)]
            mirror_syncs: std::sync::atomic::AtomicUsize::new(0),
        };
        result.revalidate()?;
        Ok(result)
    }
    pub(super) fn revalidate(&self) -> Result<()> {
        self.guard.ensure_no_snapshot_locks()?;
        if !self.guard.io_retired() {
            return Err(LimboError::Busy);
        }
        self.files.revalidate_marker()?;
        if self.files.selected_manifest()? != self.record {
            return Err(invalid("downgrade terminal record changed"));
        }
        Ok(())
    }
    pub(super) fn validate_layout(&self, header: &DatabaseHeader, bytes: u64) -> Result<()> {
        if header.page_size != self.header.page_size
            || header.reserved_space != self.header.reserved_space
            || header.database_size != self.header.database_size
            || bytes != self.bytes
        {
            return Err(invalid("recovered downgrade physical layout changed"));
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn mirror_syncs(&self) -> usize {
        self.mirror_syncs.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub(super) fn mirror(&self) -> Result<()> {
        self.revalidate()?;
        let bytes = self.record.encode();
        let file = &self.files.files[0].file;
        // All I/O uses retained synchronous native aliases under continuous ownership.
        for offset in [0, SLOT_SIZE] {
            let mut existing = [0; RECORD_SIZE];
            file.read_exact_at(&mut existing, offset as u64)
                .map_err(super::fs_error)?;
            if existing != bytes {
                file.write_all_at(&bytes, offset as u64)
                    .map_err(super::fs_error)?;
                file.sync_all().map_err(super::fs_error)?;
                #[cfg(test)]
                self.mirror_syncs
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        self.files.require_mirrored(self.record)
    }
}

pub(in crate::mvcc) fn resume_marker(
    path: &Path,
    history_limit: u64,
    _: OfflinePrerequisite,
) -> Result<downgrade_marker::CompletedDowngradeMarker> {
    downgrade_marker::commit_recovered(RecoveredDowngradeSource::open(path, history_limit)?)
}
fn invalid(message: &str) -> LimboError {
    LimboError::InvalidArgument(message.into())
}
