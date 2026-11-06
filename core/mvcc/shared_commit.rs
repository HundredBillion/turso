//! Publication of one shared logical-log frame under an existing authority lease.
use super::commit_manifest::{LoadedManifest, ManifestPublication, ManifestRecord};
use super::database::LogRecord;
use super::guarded_file::{FileBinding, GuardedFile};
use super::persistent_storage::logical_log::{LogTxFrameInfo, LOG_RECORD_PREFIX_SIZE};
use super::persistent_storage::{DurableStorage, Storage};
use super::shared_authority::{
    AuthorityGuard, IoPin, PublicationLease, PublishedPrefix, MAX_TIMESTAMP,
};
use crate::io::{File, FileSyncType, SharedBufferData};
use crate::sync::Arc;
use crate::types::IOCompletions;
use crate::{Completion, IOResult, LimboError, Result};
use std::cell::Cell;

/// The caller supplies a globally reserved timestamp and finalizes local versions
/// before finishing the returned guard. The caller keeps this concrete Storage
/// exclusive until the driver and all backend pins retire. A failed or dropped driver requires recovery,
/// even when `is_durable` is false: an unobserved sync may already have completed.
pub(super) struct SharedCommit {
    stage: Stage,
    storage: Arc<Storage>,
    binding: Option<FileBinding>,
    cleanup_pin: Option<IoPin>,
    pending_owned: bool,
    durable: bool,
    preflight_rejected: bool,
    id_budget: u64,
}

enum Stage {
    Start(LoadedManifest, LogRecord),
    Writing(LoadedManifest, ManifestRecord, u64, Completion),
    Syncing(LoadedManifest, ManifestRecord, u64, Completion),
    Publishing(ManifestPublication, u64),
    Retiring(AuthorityGuard),
    Finished,
}

fn invalid(message: &str) -> LimboError {
    LimboError::InvalidArgument(message.into())
}

impl SharedCommit {
    pub(super) fn new(
        loaded: LoadedManifest,
        storage: Arc<Storage>,
        file: Arc<GuardedFile>,
        record: LogRecord,
        id_budget: u64,
    ) -> Result<Self> {
        if id_budget < crate::mvcc::shared_ids::MIN_ID_BUDGET_BYTES {
            return Err(invalid(
                "commit lacks the minimum reservation metadata budget",
            ));
        }
        let selected = loaded.record();
        let prefix = selected.prefix();
        let guard = loaded.guard()?;
        if selected.requires_recovery()
            || guard.published()? != prefix
            || record.tx_timestamp <= prefix.committed_timestamp
            || record.tx_timestamp >= MAX_TIMESTAMP
            || record.buf.len() < LOG_RECORD_PREFIX_SIZE
        {
            return Err(invalid(
                "shared commit prefix, timestamp, or frame is invalid",
            ));
        }
        selected
            .sequence()
            .checked_add(1)
            .ok_or(LimboError::DatabaseFull)?;
        let expected: Arc<dyn File> = file.clone();
        let state = storage.shared_commit_snapshot(&expected)?;
        if state.offset != prefix.end_offset {
            return Err(invalid(
                "shared commit storage offset differs from manifest",
            ));
        }
        if state.offset == 0 {
            let valid = match state.header {
                None => state.running_crc == 0 && selected.log_salt() == 0,
                Some((_, salt)) => {
                    state.running_crc == crc32c::crc32c(&salt.to_le_bytes())
                        && (selected.log_salt() == 0 || selected.log_salt() == salt)
                }
            };
            if !valid {
                return Err(invalid("shared empty log seed differs from manifest"));
            }
        } else {
            let Some((version, salt)) = state.header else {
                return Err(invalid("shared acknowledged log has no header"));
            };
            if salt != selected.log_salt() || state.running_crc != prefix.running_crc {
                return Err(invalid("shared log checksum or salt differs from manifest"));
            }
            #[cfg(feature = "conn_raw_api")]
            let portable = record.portable_changes_enabled
                || record.portable_changes_required
                || !record.portable_changes.is_empty();
            #[cfg(not(feature = "conn_raw_api"))]
            let portable = false;
            if version == 2 && portable {
                return Err(invalid(
                    "shared portable append requires a new log generation",
                ));
            }
        }
        let binding = file.bind(guard)?;
        // This independent pin protects cleanup when a consuming manifest method
        // drops its guard on error. It must retire before the successful handoff.
        let cleanup_pin = PublicationLease::pin(&guard.io_lease())?;
        Ok(Self {
            stage: Stage::Start(loaded, record),
            storage,
            binding: Some(binding),
            cleanup_pin: Some(cleanup_pin),
            pending_owned: false,
            durable: false,
            preflight_rejected: false,
            id_budget,
        })
    }

    /// Sticky as soon as manifest FULL sync is observed, including later errors.
    pub(super) fn is_durable(&self) -> bool {
        self.durable
    }

    pub(super) fn preflight_rejected(&self) -> bool {
        self.preflight_rejected
    }

    pub(super) fn poll(&mut self) -> Result<IOResult<AuthorityGuard>> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort_pending();
        }
        result
    }

    fn poll_inner(&mut self) -> Result<IOResult<AuthorityGuard>> {
        loop {
            match std::mem::replace(&mut self.stage, Stage::Finished) {
                Stage::Start(loaded, record) => {
                    let selected = loaded.record();
                    let prefix = selected.prefix();
                    let timestamp = record.tx_timestamp;
                    let framed = Cell::new(None);
                    let preflight_rejected = Cell::new(false);
                    let callback = |bytes: SharedBufferData, info: LogTxFrameInfo| -> Result<()> {
                        let len = u64::try_from(bytes.len()).map_err(|_| LimboError::TooBig)?;
                        let salt = if prefix.end_offset == 0 {
                            let raw = bytes
                                .as_slice()
                                .get(8..16)
                                .ok_or_else(|| invalid("missing shared log header"))?;
                            u64::from_le_bytes(raw.try_into().unwrap())
                        } else {
                            selected.log_salt()
                        };
                        let start_crc = if prefix.end_offset == 0 {
                            crc32c::crc32c(&salt.to_le_bytes())
                        } else {
                            prefix.running_crc
                        };
                        if info.logical_start_offset != prefix.end_offset
                            || info.start_crc32c != start_crc
                        {
                            return Err(invalid("shared frame does not extend selected prefix"));
                        }
                        if let Err(error) = loaded.guard()?.check_append_size_with_metadata(
                            len,
                            self.id_budget,
                            crate::mvcc::shared_ids::MIN_ID_BUDGET_BYTES,
                        ) {
                            preflight_rejected.set(true);
                            return Err(error);
                        }
                        let next = ManifestRecord::new(
                            selected.sequence() + 1,
                            selected.database_id(),
                            selected.log_generation(),
                            salt,
                            PublishedPrefix {
                                committed_timestamp: timestamp,
                                end_offset: prefix
                                    .end_offset
                                    .checked_add(len)
                                    .ok_or(LimboError::TooBig)?,
                                running_crc: info.end_crc32c,
                            },
                        )?;
                        framed.set(Some((next, len)));
                        Ok(())
                    };
                    self.pending_owned = true;
                    let result = self.storage.log_tx(record, Some(&callback));
                    self.preflight_rejected = preflight_rejected.get();
                    let (completion, bytes) = result?;
                    let (next, len) = framed
                        .get()
                        .ok_or_else(|| invalid("shared serializer omitted frame validation"))?;
                    if bytes != len {
                        return Err(invalid(
                            "shared serializer returned inconsistent frame length",
                        ));
                    }
                    self.stage = Stage::Writing(loaded, next, len, completion);
                }
                Stage::Writing(loaded, next, bytes, completion) => {
                    if !completion.finished() {
                        self.stage = Stage::Writing(loaded, next, bytes, completion.clone());
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    if let Some(error) = completion.get_error() {
                        return Err(error.into());
                    }
                    self.stage = Stage::Syncing(
                        loaded,
                        next,
                        bytes,
                        self.storage.sync(FileSyncType::FullFsync)?,
                    );
                }
                Stage::Syncing(loaded, next, bytes, completion) => {
                    if !completion.finished() {
                        self.stage = Stage::Syncing(loaded, next, bytes, completion.clone());
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    if let Some(error) = completion.get_error() {
                        return Err(error.into());
                    }
                    self.stage = Stage::Publishing(loaded.publish(next)?, bytes);
                }
                Stage::Publishing(mut publication, bytes) => match publication.poll()? {
                    IOResult::IO(io) => {
                        self.stage = Stage::Publishing(publication, bytes);
                        return Ok(IOResult::IO(io));
                    }
                    IOResult::Done(durable) => {
                        self.durable = true;
                        // Once durable, a failure must preserve unresolved local state
                        // for manifest-based recovery rather than discard its pending CRC.
                        self.storage
                            .advance_logical_log_offset_after_success(bytes)?;
                        self.pending_owned = false;
                        let guard = durable.publish()?;
                        self.binding.take();
                        self.cleanup_pin.take();
                        self.stage = Stage::Retiring(guard);
                    }
                },
                Stage::Retiring(guard) => {
                    if !guard.io_retired() {
                        let completion = guard.retirement_completion();
                        self.stage = Stage::Retiring(guard);
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    return Ok(IOResult::Done(guard));
                }
                Stage::Finished => return Err(invalid("shared commit already terminated")),
            }
        }
    }

    fn abort_pending(&mut self) {
        if self.pending_owned && !self.durable {
            // Concrete Storage currently cannot fail this operation. Ownership is
            // retained until cleanup completes, including after stage-local errors.
            let _ = self.storage.discard_pending_log_write();
            self.pending_owned = false;
        }
        self.binding.take();
        self.cleanup_pin.take();
    }
}
impl Drop for SharedCommit {
    fn drop(&mut self) {
        self.abort_pending();
    }
}

#[cfg(all(test, not(shuttle)))]
mod tests {
    use super::*;
    use crate::io::{Buffer, Completion, File, FileId, FileSyncType, MemoryIO, SharedWalLockKind};
    use crate::mvcc::commit_manifest::{ManifestLoad, ManifestRecord};
    use crate::mvcc::persistent_storage::DurableStorage;
    use crate::mvcc::shared_authority::{authority_for_test, PublishedPrefix, SharedMvccAuthority};
    use crate::storage::sqlite3_ondisk::DatabaseHeader;
    use crate::sync::Mutex;
    use crate::LimboError;
    use std::sync::atomic::{AtomicBool, Ordering};

    enum Op {
        Write(u64, Arc<Buffer>),
        Sync,
    }
    struct Pending {
        op: Op,
        completion: Completion,
    }
    #[derive(Default)]
    struct ControlledFile {
        bytes: Mutex<Vec<u8>>,
        pending: Mutex<Vec<Pending>>,
        locked: AtomicBool,
    }
    impl ControlledFile {
        fn complete(&self, index: usize) {
            let pending = self.pending.lock();
            let p = &pending[index];
            let count = match &p.op {
                Op::Write(offset, buffer) => {
                    let mut bytes = self.bytes.lock();
                    let end = *offset as usize + buffer.len();
                    if bytes.len() < end {
                        bytes.resize(end, 0);
                    }
                    bytes[*offset as usize..end].copy_from_slice(buffer.as_slice());
                    buffer.len() as i32
                }
                Op::Sync => 0,
            };
            p.completion.complete(count);
        }
        fn retire(&self) {
            self.pending.lock().clear();
        }
    }
    impl File for ControlledFile {
        fn lock_file(&self, _: bool) -> Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }
        fn pread(&self, offset: u64, c: Completion) -> Result<Completion> {
            let bytes = self.bytes.lock();
            let buffer = c.as_read().buf();
            let n = buffer
                .len()
                .min(bytes.len().saturating_sub(offset as usize));
            buffer.as_mut_slice()[..n]
                .copy_from_slice(&bytes[offset as usize..offset as usize + n]);
            c.complete(n as i32);
            Ok(c)
        }
        fn pwrite(&self, offset: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.pending.lock().push(Pending {
                op: Op::Write(offset, buffer),
                completion: c.clone(),
            });
            Ok(c)
        }
        fn sync(&self, c: Completion, kind: FileSyncType) -> Result<Completion> {
            assert!(matches!(kind, FileSyncType::FullFsync));
            self.pending.lock().push(Pending {
                op: Op::Sync,
                completion: c.clone(),
            });
            Ok(c)
        }
        fn size(&self) -> Result<u64> {
            Ok(self.bytes.lock().len() as u64)
        }
        fn truncate(&self, _: u64, _: Completion) -> Result<Completion> {
            panic!("commit must not truncate")
        }
        fn shared_wal_try_lock_byte(&self, _: u64, _: bool, _: SharedWalLockKind) -> Result<bool> {
            Ok(!self.locked.swap(true, Ordering::AcqRel))
        }
        fn shared_wal_unlock_byte(&self, _: u64, _: SharedWalLockKind) -> Result<()> {
            self.locked.store(false, Ordering::Release);
            Ok(())
        }
    }
    struct Fixture {
        log: Arc<ControlledFile>,
        manifest: Arc<ControlledFile>,
        authority: Arc<SharedMvccAuthority>,
        storage: Arc<Storage>,
        file: Arc<GuardedFile>,
    }
    impl Fixture {
        fn new() -> Self {
            let log = Arc::new(ControlledFile::default());
            let manifest = Arc::new(ControlledFile::default());
            let authority = authority_for_test(Arc::new(ControlledFile::default()));
            let file = GuardedFile::new(log.clone(), &authority);
            let storage = Arc::new(Storage::new(file.clone(), Arc::new(MemoryIO::new()), None));
            let record = ManifestRecord::new(
                1,
                FileId { dev: 1, ino: 2 },
                1,
                0,
                PublishedPrefix::default(),
            )
            .unwrap();
            let mut bytes = vec![0; 8192];
            bytes[..128].copy_from_slice(&record.encode());
            *manifest.bytes.lock() = bytes;
            Self {
                log,
                manifest,
                authority,
                storage,
                file,
            }
        }
        fn loaded(&self) -> LoadedManifest {
            let mut guard = self.authority.try_acquire().unwrap();
            guard.recover(PublishedPrefix::default()).unwrap();
            let mut load =
                ManifestLoad::new(self.manifest.clone(), guard, FileId { dev: 1, ino: 2 }).unwrap();
            let IOResult::Done(loaded) = load.poll().unwrap() else {
                panic!("immediate reads")
            };
            loaded
        }
        fn record(&self) -> LogRecord {
            let mut record = LogRecord::new(1, crate::alloc::DynAllocator::default()).unwrap();
            self.storage
                .serialize_database_header(&mut record, &DatabaseHeader::default())
                .unwrap();
            record
        }
        fn start(&self) -> SharedCommit {
            SharedCommit::new(
                self.loaded(),
                self.storage.clone(),
                self.file.clone(),
                self.record(),
                crate::mvcc::shared_ids::MIN_ID_BUDGET_BYTES,
            )
            .unwrap()
        }
    }
    #[test]
    fn shared_commit_rejects_checkpoint_intent_before_submitting_bytes() {
        let f = Fixture::new();
        let intent = ManifestRecord::decode(&f.manifest.bytes.lock()[..128])
            .unwrap()
            .unwrap()
            .begin_checkpoint(0)
            .unwrap();
        f.manifest.bytes.lock()[..128].copy_from_slice(&intent.encode());
        assert!(SharedCommit::new(
            f.loaded(),
            f.storage.clone(),
            f.file.clone(),
            f.record(),
            crate::mvcc::shared_ids::MIN_ID_BUDGET_BYTES
        )
        .is_err());
        assert!(f.log.bytes.lock().is_empty());
        assert!(f.log.pending.lock().is_empty());
        assert!(f.manifest.pending.lock().is_empty());
        assert!(f.authority.try_acquire().unwrap().requires_recovery());
    }

    #[test]
    fn shared_commit_orders_durability_and_waits_for_physical_retirement() {
        let f = Fixture::new();
        let mut commit = f.start();
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        assert_eq!(f.log.pending.lock().len(), 1);
        assert!(f.manifest.pending.lock().is_empty());
        f.log.complete(0);
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        assert_eq!(f.log.pending.lock().len(), 2);
        assert!(f.manifest.pending.lock().is_empty());
        f.log.complete(1);
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        assert_eq!(f.manifest.pending.lock().len(), 1);
        assert_eq!(f.storage.logical_log_offset(), 0);
        assert!(!commit.is_durable());
        f.manifest.complete(0);
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        assert_eq!(f.manifest.pending.lock().len(), 2);
        assert_eq!(f.storage.logical_log_offset(), 0);
        assert!(!commit.is_durable());
        f.manifest.complete(1);
        let IOResult::IO(retirement) = commit.poll().unwrap() else {
            panic!("retirement wait")
        };
        assert!(
            !retirement.0.finished(),
            "retirement must drive backend progress"
        );
        assert!(commit.is_durable());
        assert!(f.storage.logical_log_offset() > 0);
        assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
        f.log.retire();
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        f.manifest.retire();
        let IOResult::Done(guard) = commit.poll().unwrap() else {
            panic!("retired completion pins")
        };
        assert_eq!(guard.published().unwrap().committed_timestamp, 1);
        guard.finish().unwrap();
        assert!(commit.poll().is_err());
        assert!(!f.authority.try_acquire().unwrap().requires_recovery());
    }
    #[test]
    fn shared_commit_cap_rejects_before_write_and_allows_fresh_retry() {
        use crate::mvcc::database::{
            tests::generate_simple_string_row, PackedTs, RowVersion, TxTimestampOrID, WalPos,
        };
        let f = Fixture::new();
        let mut record = f.record();
        let row = generate_simple_string_row((-2).into(), 1, &"x".repeat(1_100_000));
        let version = RowVersion {
            id: 1,
            begin: PackedTs::pack(Some(TxTimestampOrID::Timestamp(1))),
            end: PackedTs::pack(None),
            data: row.data.clone(),
            btree_resident: false,
            materialized_at: WalPos::ORIGIN,
        };
        f.storage
            .serialize_row_version(&mut record, &row.id, &version, None)
            .unwrap();
        let mut commit = SharedCommit::new(
            f.loaded(),
            f.storage.clone(),
            f.file.clone(),
            record,
            crate::mvcc::shared_ids::MIN_ID_BUDGET_BYTES,
        )
        .unwrap();
        assert!(matches!(commit.poll(), Err(LimboError::TooBig)));
        assert!(!commit.is_durable());
        assert!(f.log.pending.lock().is_empty());
        assert!(f.manifest.pending.lock().is_empty());
        assert!(!f.storage.logical_log.read().has_pending_write());
        drop(commit);
        let mut retry = f.start();
        assert!(matches!(retry.poll().unwrap(), IOResult::IO(_)));
        assert_eq!(f.log.pending.lock().len(), 1);
    }

    #[test]
    fn shared_commit_metadata_pressure_rejects_before_submitting_log_bytes() {
        let f = Fixture::new();
        let mut commit = SharedCommit::new(
            f.loaded(),
            f.storage.clone(),
            f.file.clone(),
            f.record(),
            999_999,
        )
        .unwrap();
        assert!(matches!(commit.poll(), Err(LimboError::Busy)));
        assert!(commit.preflight_rejected());
        assert!(!commit.is_durable());
        assert!(f.log.pending.lock().is_empty());
        assert!(f.manifest.pending.lock().is_empty());
        assert_eq!(f.storage.logical_log_offset(), 0);
        drop(commit);
        let mut retry = f.start();
        assert!(matches!(retry.poll().unwrap(), IOResult::IO(_)));
    }

    #[test]
    fn shared_commit_drop_discards_pending_without_releasing_backend_ownership() {
        let f = Fixture::new();
        let mut commit = f.start();
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        assert!(f.storage.logical_log.read().has_pending_write());
        drop(commit);
        assert!(!f.storage.logical_log.read().has_pending_write());
        assert_eq!(f.storage.logical_log_offset(), 0);
        assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
        f.log.complete(0);
        assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
        f.log.retire();
        assert!(f.authority.try_acquire().unwrap().requires_recovery());
    }
    #[test]
    fn shared_commit_preflight_rejects_stale_or_unrelated_state_without_io() {
        for case in 0..6 {
            let f = Fixture::new();
            let loaded = f.loaded();
            let mut record = f.record();
            let storage = match case {
                0 => {
                    record.tx_timestamp = 0;
                    f.storage.clone()
                }
                1 => {
                    record.tx_timestamp = (1 << 62) - 1;
                    f.storage.clone()
                }
                2 => {
                    record.buf.clear();
                    f.storage.clone()
                }
                3 => {
                    f.storage.logical_log.write().running_crc = 123;
                    f.storage.clone()
                }
                4 => {
                    f.storage.logical_log.write().offset = 1;
                    f.storage.clone()
                }
                _ => Arc::new(Storage::new(
                    Arc::new(ControlledFile::default()),
                    Arc::new(MemoryIO::new()),
                    None,
                )),
            };
            assert!(
                SharedCommit::new(
                    loaded,
                    storage,
                    f.file.clone(),
                    record,
                    crate::mvcc::shared_ids::MIN_ID_BUDGET_BYTES
                )
                .is_err(),
                "case {case}"
            );
            assert!(f.log.pending.lock().is_empty());
            assert!(f.manifest.pending.lock().is_empty());
        }
    }

    #[test]
    fn shared_commit_unobserved_manifest_sync_requires_recovery_not_rollback() {
        let f = Fixture::new();
        let mut commit = f.start();
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        f.log.complete(0);
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        f.log.complete(1);
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        f.manifest.complete(0);
        assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
        f.manifest.complete(1);
        assert!(!commit.is_durable());
        drop(commit);
        assert_eq!(f.storage.logical_log_offset(), 0);
        assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
        f.log.retire();
        f.manifest.retire();
        let guard = f.authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        let mut load =
            ManifestLoad::new(f.manifest.clone(), guard, FileId { dev: 1, ino: 2 }).unwrap();
        let IOResult::Done(loaded) = load.poll().unwrap() else {
            panic!("immediate reads")
        };
        assert_eq!(loaded.record().prefix().committed_timestamp, 1);
        assert!(loaded.record().prefix().end_offset > 0);
    }

    #[test]
    fn shared_commit_short_write_and_sync_error_leave_no_published_success() {
        for fail_sync in [false, true] {
            let f = Fixture::new();
            let mut commit = f.start();
            assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
            if fail_sync {
                f.log.complete(0);
                assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
                f.log.pending.lock()[1]
                    .completion
                    .error(crate::CompletionError::Aborted);
            } else {
                f.log.pending.lock()[0].completion.complete(1);
            }
            assert!(commit.poll().is_err());
            assert!(!commit.is_durable());
            assert_eq!(f.storage.logical_log_offset(), 0);
            assert!(!f.storage.logical_log.read().has_pending_write());
            assert!(f.manifest.pending.lock().is_empty());
            assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
            f.log.retire();
            assert!(f.authority.try_acquire().unwrap().requires_recovery());
        }
    }

    #[cfg(feature = "conn_raw_api")]
    #[test]
    fn shared_commit_never_rewrites_acknowledged_v2_header_for_portable_append() {
        let f = Fixture::new();
        let mut first = f.start();
        assert!(matches!(first.poll().unwrap(), IOResult::IO(_)));
        f.log.complete(0);
        assert!(matches!(first.poll().unwrap(), IOResult::IO(_)));
        f.log.complete(1);
        assert!(matches!(first.poll().unwrap(), IOResult::IO(_)));
        f.manifest.complete(0);
        assert!(matches!(first.poll().unwrap(), IOResult::IO(_)));
        f.manifest.complete(1);
        f.log.retire();
        f.manifest.retire();
        let IOResult::Done(guard) = first.poll().unwrap() else {
            panic!("retired")
        };
        let before = f.log.bytes.lock().clone();
        assert_eq!(f.storage.logical_log.read().header().unwrap().version(), 2);
        let mut load =
            ManifestLoad::new(f.manifest.clone(), guard, FileId { dev: 1, ino: 2 }).unwrap();
        let IOResult::Done(loaded) = load.poll().unwrap() else {
            panic!("immediate reads")
        };
        let mut record = f.record();
        record.tx_timestamp = 2;
        record.portable_changes_required = true;
        assert!(SharedCommit::new(
            loaded,
            f.storage.clone(),
            f.file.clone(),
            record,
            crate::mvcc::shared_ids::MIN_ID_BUDGET_BYTES
        )
        .is_err());
        assert_eq!(*f.log.bytes.lock(), before);
        assert!(f.log.pending.lock().is_empty());
        assert!(f.manifest.pending.lock().is_empty());
    }
    #[test]
    fn shared_commit_manifest_failure_never_advances_local_prefix() {
        for fail_sync in [false, true] {
            let f = Fixture::new();
            let mut commit = f.start();
            assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
            f.log.complete(0);
            assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
            f.log.complete(1);
            assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
            if fail_sync {
                f.manifest.complete(0);
                assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
                f.manifest.pending.lock()[1]
                    .completion
                    .error(crate::CompletionError::Aborted);
            } else {
                f.manifest.pending.lock()[0].completion.complete(1);
            }
            assert!(commit.poll().is_err());
            assert!(!commit.is_durable());
            assert_eq!(f.storage.logical_log_offset(), 0);
            assert!(!f.storage.logical_log.read().has_pending_write());
            assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
            f.log.retire();
            f.manifest.retire();
            assert!(f.authority.try_acquire().unwrap().requires_recovery());
        }
    }
    #[test]
    fn shared_commit_successive_frames_preserve_prefix_bytes_and_crc_chain() {
        let f = Fixture::new();
        let mut prefix = PublishedPrefix::default();
        for timestamp in [1, 2] {
            let mut guard = f.authority.try_acquire().unwrap();
            if guard.requires_recovery() {
                guard.recover(prefix).unwrap();
            } else {
                assert_eq!(guard.published().unwrap(), prefix);
            }
            let mut load =
                ManifestLoad::new(f.manifest.clone(), guard, FileId { dev: 1, ino: 2 }).unwrap();
            let IOResult::Done(loaded) = load.poll().unwrap() else {
                panic!("immediate reads")
            };
            let old = f.log.bytes.lock().clone();
            let mut record = f.record();
            record.tx_timestamp = timestamp;
            let mut commit = SharedCommit::new(
                loaded,
                f.storage.clone(),
                f.file.clone(),
                record,
                crate::mvcc::shared_ids::MIN_ID_BUDGET_BYTES,
            )
            .unwrap();
            assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
            f.log.complete(0);
            assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
            f.log.complete(1);
            assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
            f.manifest.complete(0);
            assert!(matches!(commit.poll().unwrap(), IOResult::IO(_)));
            f.manifest.complete(1);
            f.log.retire();
            f.manifest.retire();
            let IOResult::Done(guard) = commit.poll().unwrap() else {
                panic!("retired")
            };
            let next = guard.published().unwrap();
            let bytes = f.log.bytes.lock();
            assert_eq!(&bytes[..old.len()], old.as_slice());
            assert_eq!(next.end_offset as usize, bytes.len());
            let start = if old.is_empty() { 56 } else { old.len() };
            let seed = if old.is_empty() {
                crc32c::crc32c(&bytes[8..16])
            } else {
                prefix.running_crc
            };
            assert_eq!(
                next.running_crc,
                crc32c::crc32c_append(seed, &bytes[start..bytes.len() - 8])
            );
            assert_eq!(
                next.running_crc,
                u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
            );
            assert_eq!(next.committed_timestamp, timestamp);
            prefix = next;
            guard.finish().unwrap();
        }
    }
}
