use super::shared_authority::{AuthorityGuard, PublishedPrefix};
use crate::io::FileId;
use crate::io::{Buffer, Completion, File, FileSyncType};
use crate::sync::{Arc, Mutex};
use crate::types::IOCompletions;
use crate::IOResult;
use crate::{LimboError, Result};

const RECORD_SIZE: usize = 128;
const MAGIC: [u8; 8] = *b"TMVCCM01";
const VERSION: u32 = 1;
const CHECKSUM_OFFSET: usize = 124;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ManifestRecord {
    sequence: u64,
    database_id: FileId,
    log_generation: u64,
    log_salt: u64,
    prefix: PublishedPrefix,
}

impl ManifestRecord {
    pub(super) fn new(
        sequence: u64,
        database_id: FileId,
        log_generation: u64,
        log_salt: u64,
        prefix: PublishedPrefix,
    ) -> Result<Self> {
        if sequence == 0
            || log_generation == 0
            || prefix.committed_timestamp >= (1 << 62)
            || (prefix.end_offset == 0 && prefix.running_crc != 0)
            || (prefix.end_offset != 0
                && prefix.end_offset < super::persistent_storage::logical_log::LOG_HDR_SIZE as u64)
        {
            return Err(LimboError::InvalidArgument(
                "invalid MVCC commit manifest record".into(),
            ));
        }
        Ok(Self {
            sequence,
            database_id,
            log_generation,
            log_salt,
            prefix,
        })
    }

    pub(super) fn encode(self) -> [u8; RECORD_SIZE] {
        let mut bytes = [0; RECORD_SIZE];
        bytes[..8].copy_from_slice(&MAGIC);
        bytes[8..12].copy_from_slice(&VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&(RECORD_SIZE as u32).to_le_bytes());
        for (offset, value) in [
            (16, self.sequence),
            (24, self.database_id.dev),
            (32, self.database_id.ino),
            (40, self.log_generation),
            (48, self.log_salt),
            (56, self.prefix.committed_timestamp),
            (64, self.prefix.end_offset),
        ] {
            bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }
        bytes[72..76].copy_from_slice(&self.prefix.running_crc.to_le_bytes());
        let checksum = crc32c::crc32c(&bytes[..CHECKSUM_OFFSET]);
        bytes[CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Option<Self>> {
        if bytes.len() < RECORD_SIZE {
            return Ok(None);
        }
        if bytes.len() != RECORD_SIZE {
            return Err(LimboError::Corrupt(
                "invalid MVCC manifest record length".into(),
            ));
        }
        let checksum = u32::from_le_bytes(bytes[CHECKSUM_OFFSET..].try_into().unwrap());
        if crc32c::crc32c(&bytes[..CHECKSUM_OFFSET]) != checksum {
            return Ok(None);
        }
        if bytes[..8] != MAGIC || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) != VERSION {
            return Err(LimboError::InvalidArgument(
                "incompatible MVCC commit manifest format".into(),
            ));
        }
        if u32::from_le_bytes(bytes[12..16].try_into().unwrap()) != RECORD_SIZE as u32
            || bytes[76..CHECKSUM_OFFSET].iter().any(|byte| *byte != 0)
        {
            return Err(LimboError::Corrupt(
                "invalid MVCC manifest record layout".into(),
            ));
        }
        let read_u64 = |offset| u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        Self::new(
            read_u64(16),
            FileId {
                dev: read_u64(24),
                ino: read_u64(32),
            },
            read_u64(40),
            read_u64(48),
            PublishedPrefix {
                committed_timestamp: read_u64(56),
                end_offset: read_u64(64),
                running_crc: u32::from_le_bytes(bytes[72..76].try_into().unwrap()),
            },
        )
        .map(Some)
        .map_err(|_| LimboError::Corrupt("invalid MVCC manifest record values".into()))
    }
}

fn select_latest(
    records: [Option<ManifestRecord>; 2],
    database_id: FileId,
) -> Result<(usize, ManifestRecord)> {
    if records
        .iter()
        .flatten()
        .any(|record| record.database_id != database_id)
    {
        return Err(LimboError::Corrupt(
            "MVCC manifest belongs to a different database".into(),
        ));
    }
    let (slot, newest, previous) = match records {
        [None, None] => {
            return Err(LimboError::Corrupt(
                "no valid MVCC commit manifest record".into(),
            ))
        }
        [Some(record), None] => return Ok((0, record)),
        [None, Some(record)] => return Ok((1, record)),
        [Some(first), Some(second)] if first.sequence == second.sequence => {
            if first != second {
                return Err(LimboError::Corrupt(
                    "conflicting MVCC manifest sequence".into(),
                ));
            }
            return Ok((0, first));
        }
        [Some(first), Some(second)] if first.sequence > second.sequence => (0, first, second),
        [Some(first), Some(second)] => (1, second, first),
    };
    if newest.log_generation < previous.log_generation
        || newest.prefix.committed_timestamp < previous.prefix.committed_timestamp
        || (newest.log_generation == previous.log_generation
            && (newest.log_salt != previous.log_salt
                || newest.prefix.end_offset < previous.prefix.end_offset
                || (newest.prefix.end_offset == previous.prefix.end_offset
                    && newest.prefix != previous.prefix)))
    {
        return Err(LimboError::Corrupt(
            "MVCC commit manifest rewinds committed history".into(),
        ));
    }
    Ok((slot, newest))
}

const SLOT_SIZE: usize = 4096;

struct ManifestLoad {
    file: Arc<dyn File>,
    ownership: Arc<Mutex<Option<AuthorityGuard>>>,
    database_id: FileId,
    size: usize,
    slot: usize,
    filled: usize,
    bytes: [u8; RECORD_SIZE],
    records: [Option<ManifestRecord>; 2],
    pending: Option<(Completion, Arc<Buffer>, Arc<std::sync::atomic::AtomicI32>)>,
    terminated: bool,
}

impl ManifestLoad {
    fn new(file: Arc<dyn File>, guard: AuthorityGuard, database_id: FileId) -> Result<Self> {
        Self::start(file, Some(guard), database_id)
    }

    fn start(
        file: Arc<dyn File>,
        guard: Option<AuthorityGuard>,
        database_id: FileId,
    ) -> Result<Self> {
        let size = file.size()?;
        if size > (SLOT_SIZE * 2) as u64 {
            return Err(LimboError::Corrupt("oversized MVCC manifest".into()));
        }
        Ok(Self {
            file,
            ownership: Arc::new(Mutex::new(guard)),
            database_id,
            size: size as usize,
            slot: 0,
            filled: 0,
            bytes: [0; RECORD_SIZE],
            records: [None; 2],
            pending: None,
            terminated: false,
        })
    }

    fn poll(&mut self) -> Result<IOResult<LoadedManifest>> {
        use std::sync::atomic::{AtomicI32, Ordering};
        if self.terminated {
            return Err(LimboError::InvalidArgument(
                "manifest load already terminated".into(),
            ));
        }
        self.terminated = true;
        loop {
            if let Some((completion, buffer, count)) = self.pending.take() {
                if !completion.finished() {
                    self.pending = Some((completion.clone(), buffer, count));
                    self.terminated = false;
                    return Ok(IOResult::IO(IOCompletions(completion)));
                }
                if let Some(error) = completion.get_error() {
                    return Err(error.into());
                }
                let n = count.load(Ordering::Acquire);
                if n <= 0 || n as usize > buffer.len() {
                    return Err(LimboError::Corrupt(
                        "unexpected MVCC manifest read length".into(),
                    ));
                }
                let n = n as usize;
                self.bytes[self.filled..self.filled + n].copy_from_slice(&buffer.as_slice()[..n]);
                self.filled += n;
            }
            if self.slot == 2 {
                let (slot, record) = select_latest(self.records, self.database_id)?;
                return Ok(IOResult::Done(LoadedManifest {
                    file: self.file.clone(),
                    guard: self.ownership.lock().take(),
                    slot,
                    record,
                }));
            }
            let expected = self
                .size
                .saturating_sub(self.slot * SLOT_SIZE)
                .min(RECORD_SIZE);
            if self.filled == expected {
                self.records[self.slot] = ManifestRecord::decode(&self.bytes[..expected])?;
                self.slot += 1;
                self.filled = 0;
                continue;
            }
            let buffer = Arc::new(Buffer::new_temporary(expected - self.filled));
            let count = Arc::new(AtomicI32::new(-1));
            let read = count.clone();
            let ownership = self.ownership.clone();
            let completion = Completion::new_read(buffer.clone(), move |result| {
                let _keep_alive = &ownership;
                if let Ok((_, n)) = result {
                    read.store(n, Ordering::Release);
                }
                None
            });
            let completion = self
                .file
                .pread((self.slot * SLOT_SIZE + self.filled) as u64, completion)?;
            self.pending = Some((completion, buffer, count));
        }
    }
}

struct LoadedManifest {
    file: Arc<dyn File>,
    guard: Option<AuthorityGuard>,
    slot: usize,
    record: ManifestRecord,
}

struct DurableManifest {
    guard: Option<AuthorityGuard>,
    slot: usize,
    record: ManifestRecord,
}

struct ManifestPublication {
    file: Arc<dyn File>,
    ownership: Arc<Mutex<Option<AuthorityGuard>>>,
    slot: usize,
    record: ManifestRecord,
    stage: PublicationStage,
}

enum PublicationStage {
    Start,
    Writing(Completion, Arc<std::sync::atomic::AtomicI32>),
    Syncing(Completion),
    Finished,
}

impl LoadedManifest {
    fn publish(self, next: ManifestRecord) -> Result<ManifestPublication> {
        if self.slot > 1 || self.record.sequence.checked_add(1) != Some(next.sequence) {
            return Err(LimboError::InvalidArgument(
                "invalid next manifest sequence".into(),
            ));
        }
        select_latest([Some(self.record), Some(next)], self.record.database_id)?;
        Ok(ManifestPublication {
            file: self.file,
            ownership: Arc::new(Mutex::new(self.guard)),
            slot: 1 - self.slot,
            record: next,
            stage: PublicationStage::Start,
        })
    }
}

impl ManifestPublication {
    fn poll(&mut self) -> Result<IOResult<DurableManifest>> {
        use std::sync::atomic::{AtomicI32, Ordering};
        loop {
            match std::mem::replace(&mut self.stage, PublicationStage::Finished) {
                PublicationStage::Start => {
                    let buffer = Arc::new(Buffer::new_temporary(SLOT_SIZE));
                    buffer.as_mut_slice().fill(0);
                    buffer.as_mut_slice()[..RECORD_SIZE].copy_from_slice(&self.record.encode());
                    let written = Arc::new(AtomicI32::new(-1));
                    let count = written.clone();
                    let ownership = self.ownership.clone();
                    let completion = Completion::new_write(move |result| {
                        // Keep authority until the backend retires the operation, even after abort.
                        let _keep_alive = &ownership;
                        if let Ok(n) = result {
                            count.store(n, Ordering::Release);
                        }
                    });
                    let completion =
                        self.file
                            .pwrite((self.slot * SLOT_SIZE) as u64, buffer, completion)?;
                    self.stage = PublicationStage::Writing(completion, written);
                }
                PublicationStage::Writing(completion, written) => {
                    if !completion.finished() {
                        self.stage = PublicationStage::Writing(completion.clone(), written);
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    if let Some(error) = completion.get_error() {
                        return Err(error.into());
                    }
                    if written.load(Ordering::Acquire) != SLOT_SIZE as i32 {
                        return Err(LimboError::Corrupt("short MVCC manifest write".into()));
                    }
                    let ownership = self.ownership.clone();
                    let sync = Completion::new_sync(move |_| {
                        let _keep_alive = &ownership;
                    });
                    let sync = self.file.sync(sync, FileSyncType::FullFsync)?;
                    self.stage = PublicationStage::Syncing(sync);
                }
                PublicationStage::Syncing(completion) => {
                    if !completion.finished() {
                        self.stage = PublicationStage::Syncing(completion.clone());
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    if let Some(error) = completion.get_error() {
                        return Err(error.into());
                    }
                    return Ok(IOResult::Done(DurableManifest {
                        guard: self.ownership.lock().take(),
                        slot: self.slot,
                        record: self.record,
                    }));
                }
                PublicationStage::Finished => {
                    return Err(LimboError::InvalidArgument(
                        "manifest publication already terminated".into(),
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATABASE_ID: FileId = FileId { dev: 1, ino: 2 };

    #[test]
    fn commit_manifest_round_trip_preserves_the_acknowledged_boundary() {
        let record = ManifestRecord::new(
            1,
            DATABASE_ID,
            1,
            123,
            PublishedPrefix {
                committed_timestamp: 42,
                end_offset: 1024,
                running_crc: 987,
            },
        )
        .unwrap();
        let bytes = record.encode();
        assert_eq!(ManifestRecord::decode(&bytes).unwrap(), Some(record));
        assert_eq!(
            select_latest([Some(record), None], DATABASE_ID).unwrap(),
            (0, record)
        );
    }

    #[test]
    fn commit_manifest_generated_records_round_trip() {
        fn property(
            sequence: u64,
            generation: u64,
            offset: u64,
            dev: u64,
            ino: u64,
            empty: bool,
        ) -> bool {
            let record = ManifestRecord::new(
                sequence.max(1),
                FileId { dev, ino },
                generation.max(1),
                sequence.rotate_left(13),
                PublishedPrefix {
                    committed_timestamp: sequence & ((1 << 62) - 1),
                    end_offset: if empty { 0 } else { offset.max(56) },
                    running_crc: if empty { 0 } else { offset as u32 },
                },
            )
            .unwrap();
            ManifestRecord::decode(&record.encode()).unwrap() == Some(record)
        }
        quickcheck::QuickCheck::new()
            .tests(200)
            .quickcheck(property as fn(u64, u64, u64, u64, u64, bool) -> bool);
    }

    #[test]
    fn commit_manifest_incomplete_successor_preserves_previous_boundary() {
        let previous = record(1, 1, 123, 10, 100);
        let successor = record(2, 1, 123, 11, 200);
        let bytes = successor.encode();
        for cut in 0..RECORD_SIZE {
            assert_eq!(ManifestRecord::decode(&bytes[..cut]).unwrap(), None);
            assert_eq!(
                select_latest(
                    [
                        Some(previous),
                        ManifestRecord::decode(&bytes[..cut]).unwrap()
                    ],
                    DATABASE_ID
                )
                .unwrap(),
                (0, previous),
                "cut {cut}",
            );
            let mut partial = [0; RECORD_SIZE];
            partial[..cut].copy_from_slice(&bytes[..cut]);
            let (_, selected) = select_latest(
                [Some(previous), ManifestRecord::decode(&partial).unwrap()],
                DATABASE_ID,
            )
            .unwrap();
            assert!(selected == previous || selected == successor, "cut {cut}");
        }
        assert_eq!(
            select_latest([Some(previous), Some(successor)], DATABASE_ID).unwrap(),
            (1, successor)
        );
    }

    #[test]
    fn commit_manifest_detects_every_single_bit_corruption() {
        let previous = record(1, 1, 123, 10, 100);
        let successor = record(2, 1, 123, 11, 200);
        let original = successor.encode();
        for byte in 0..RECORD_SIZE {
            for bit in 0..8 {
                let mut changed = original;
                changed[byte] ^= 1 << bit;
                assert_eq!(
                    ManifestRecord::decode(&changed).unwrap(),
                    None,
                    "byte {byte}, bit {bit}"
                );
                assert_eq!(
                    select_latest(
                        [Some(previous), ManifestRecord::decode(&changed).unwrap()],
                        DATABASE_ID
                    )
                    .unwrap(),
                    (0, previous)
                );
            }
        }
    }

    #[test]
    fn commit_manifest_rejects_checked_incompatible_and_impossible_records() {
        let original = record(1, 1, 123, 10, 100).encode();
        for offset in [0, 8, 12, 76] {
            let mut changed = original;
            changed[offset] ^= 1;
            fix_checksum(&mut changed);
            assert!(ManifestRecord::decode(&changed).is_err(), "offset {offset}");
        }
        for offset in [16, 40] {
            let mut changed = original;
            changed[offset..offset + 8].fill(0);
            fix_checksum(&mut changed);
            assert!(ManifestRecord::decode(&changed).is_err(), "offset {offset}");
        }
        assert!(select_latest([None, None], DATABASE_ID).is_err());
        assert_eq!(ManifestRecord::decode(&[0; RECORD_SIZE]).unwrap(), None);
        assert!(select_latest(
            [Some(record(1, 1, 123, 10, 100)), None],
            FileId { dev: 2, ino: 2 }
        )
        .is_err());
    }

    #[test]
    fn commit_manifest_rejects_history_rewinds_and_ambiguous_sequences() {
        let previous = record(4, 3, 123, 10, 100);
        for invalid in [
            record(4, 3, 123, 11, 200),
            record(5, 2, 123, 11, 200),
            record(5, 3, 123, 9, 200),
            record(5, 3, 456, 11, 200),
            record(5, 3, 123, 11, 99),
        ] {
            assert!(select_latest([Some(previous), Some(invalid)], DATABASE_ID).is_err());
            assert!(select_latest([Some(invalid), Some(previous)], DATABASE_ID).is_err());
        }
        let checkpointed = ManifestRecord::new(
            5,
            DATABASE_ID,
            4,
            0,
            PublishedPrefix {
                committed_timestamp: 10,
                end_offset: 0,
                running_crc: 0,
            },
        )
        .unwrap();
        assert_eq!(
            select_latest([Some(previous), Some(checkpointed)], DATABASE_ID).unwrap(),
            (1, checkpointed)
        );
    }

    #[test]
    fn commit_manifest_unchanged_log_boundary_preserves_timestamp_and_crc() {
        let previous = record(4, 3, 123, 10, 100);
        let advanced_timestamp = record(5, 3, 123, 11, 100);
        let mut changed_crc = record(5, 3, 123, 10, 100);
        changed_crc.prefix.running_crc ^= 1;
        for invalid in [advanced_timestamp, changed_crc] {
            assert!(select_latest([Some(previous), Some(invalid)], DATABASE_ID).is_err());
            assert!(select_latest([Some(invalid), Some(previous)], DATABASE_ID).is_err());
        }
        let identical_boundary = record(5, 3, 123, 10, 100);
        assert_eq!(
            select_latest([Some(previous), Some(identical_boundary)], DATABASE_ID).unwrap(),
            (1, identical_boundary)
        );
    }

    struct DelayedManifestFile {
        bytes: Mutex<Vec<u8>>,
        pending: Mutex<Option<(Option<(u64, Arc<Buffer>)>, Completion)>>,
        syncs: std::sync::atomic::AtomicUsize,
        pending_read: Mutex<Option<(u64, Completion)>>,
    }

    impl DelayedManifestFile {
        fn initialized(record: ManifestRecord) -> Arc<Self> {
            let mut bytes = vec![0; SLOT_SIZE];
            bytes[..RECORD_SIZE].copy_from_slice(&record.encode());
            Arc::new(Self {
                bytes: Mutex::new(bytes),
                pending: Mutex::new(None),
                syncs: std::sync::atomic::AtomicUsize::new(0),
                pending_read: Mutex::new(None),
            })
        }

        fn retire(&self, count: usize, fail: bool) {
            let (write, completion) = self.pending.lock().take().unwrap();
            if let Some((offset, buffer)) = write {
                let mut bytes = self.bytes.lock();
                let offset = offset as usize;
                let len = bytes.len().max(offset + count);
                bytes.resize(len, 0);
                bytes[offset..offset + count].copy_from_slice(&buffer.as_slice()[..count]);
            }
            if fail {
                completion.error(crate::CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "injected manifest IO",
                ));
            } else {
                completion.complete(count as i32);
            }
        }

        fn retire_read(&self, limit: usize) {
            let (offset, completion) = self.pending_read.lock().take().unwrap();
            let bytes = self.bytes.lock();
            let offset = offset as usize;
            let buffer = completion.as_read().buf();
            let n = limit
                .min(buffer.len())
                .min(bytes.len().saturating_sub(offset));
            buffer.as_mut_slice()[..n].copy_from_slice(&bytes[offset..offset + n]);
            completion.complete(n as i32);
        }

        fn selected(&self) -> ManifestRecord {
            let bytes = self.bytes.lock();
            let decode = |offset: usize| {
                let start = offset.min(bytes.len());
                let end = (offset + RECORD_SIZE).min(bytes.len());
                ManifestRecord::decode(&bytes[start..end]).unwrap()
            };
            select_latest([decode(0), decode(SLOT_SIZE)], DATABASE_ID)
                .unwrap()
                .1
        }
    }

    impl File for DelayedManifestFile {
        fn lock_file(&self, _: bool) -> Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }
        fn size(&self) -> Result<u64> {
            Ok(self.bytes.lock().len() as u64)
        }
        fn truncate(&self, _: u64, _: Completion) -> Result<Completion> {
            unreachable!()
        }
        fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
            assert!(self.pending_read.lock().replace((pos, c.clone())).is_none());
            Ok(c)
        }
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            assert_eq!(pos, SLOT_SIZE as u64);
            assert_eq!(buffer.len(), SLOT_SIZE);
            assert!(self
                .pending
                .lock()
                .replace((Some((pos, buffer)), c.clone()))
                .is_none());
            Ok(c)
        }
        fn sync(&self, c: Completion, _: FileSyncType) -> Result<Completion> {
            self.syncs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            assert!(self.pending.lock().replace((None, c.clone())).is_none());
            Ok(c)
        }
    }

    fn publishing(
        file: Arc<dyn File>,
        previous: ManifestRecord,
        next: ManifestRecord,
    ) -> ManifestPublication {
        LoadedManifest {
            file,
            guard: None,
            slot: 0,
            record: previous,
        }
        .publish(next)
        .unwrap()
    }

    #[test]
    fn commit_manifest_publication_waits_for_exact_write_then_sync() {
        let previous = record(1, 1, 123, 10, 100);
        let next = record(2, 1, 123, 11, 200);
        let file = DelayedManifestFile::initialized(previous);
        let mut publication = publishing(file.clone(), previous, next);
        assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
        assert_eq!(file.selected(), previous);
        assert_eq!(file.syncs.load(std::sync::atomic::Ordering::Relaxed), 0);
        file.retire(SLOT_SIZE, false);
        assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
        assert_eq!(file.syncs.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
        file.retire(0, false);
        let IOResult::Done(durable) = publication.poll().unwrap() else {
            panic!("sync completed")
        };
        assert_eq!(durable.record, next);
        assert_eq!(durable.slot, 1);
        assert_eq!(file.selected(), next);
        assert!(publication.poll().is_err());
    }

    #[test]
    fn commit_manifest_short_write_or_failed_sync_never_acknowledges() {
        let previous = record(1, 1, 123, 10, 100);
        let next = record(2, 1, 123, 11, 200);
        for count in 0..SLOT_SIZE {
            let file = DelayedManifestFile::initialized(previous);
            let mut publication = publishing(file.clone(), previous, next);
            assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
            file.retire(count, false);
            assert!(publication.poll().is_err(), "count {count}");
            assert!(publication.poll().is_err(), "count {count}");
            assert_eq!(file.syncs.load(std::sync::atomic::Ordering::Relaxed), 0);
            assert!(file.selected() == previous || file.selected() == next);
            assert_eq!(&file.bytes.lock()[..RECORD_SIZE], &previous.encode());
        }
        let file = DelayedManifestFile::initialized(previous);
        let mut publication = publishing(file.clone(), previous, next);
        assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
        file.retire(SLOT_SIZE, false);
        assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
        file.retire(0, true);
        assert!(publication.poll().is_err());
        assert!(publication.poll().is_err());
    }

    #[cfg(all(target_os = "linux", target_pointer_width = "64", not(shuttle)))]
    #[test]
    fn commit_manifest_aborted_state_retains_authority_until_backend_retirement() {
        use super::super::shared_authority::SharedMvccAuthority;
        for at_sync in [false, true] {
            for abort in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("authority");
                let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
                let authority =
                    SharedMvccAuthority::open(&io, path.to_str().unwrap(), DATABASE_ID, 8192)
                        .unwrap();
                let contender =
                    SharedMvccAuthority::open(&io, path.to_str().unwrap(), DATABASE_ID, 8192)
                        .unwrap();
                let mut guard = authority.try_acquire().unwrap();
                guard.recover(PublishedPrefix::default()).unwrap();
                let previous = record(1, 1, 123, 10, 100);
                let next = record(2, 1, 123, 11, 200);
                let file = DelayedManifestFile::initialized(previous);
                let mut publication = LoadedManifest {
                    file: file.clone(),
                    guard: Some(guard),
                    slot: 0,
                    record: previous,
                }
                .publish(next)
                .unwrap();
                assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
                if at_sync {
                    file.retire(SLOT_SIZE, false);
                    assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
                }
                if abort {
                    file.pending.lock().as_ref().unwrap().1.abort();
                    assert!(publication.poll().is_err());
                }
                drop(publication);
                assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
                assert!(matches!(contender.try_acquire(), Err(LimboError::Busy)));
                file.retire(if at_sync { 0 } else { SLOT_SIZE }, false);
                let recovered = contender.try_acquire().unwrap();
                assert!(recovered.requires_recovery());
            }
        }
    }

    #[test]
    fn commit_manifest_load_reads_separated_slots_and_rejects_missing_manifest() {
        use crate::io::{MemoryIO, OpenFlags};
        use crate::IO;
        let previous = record(1, 1, 123, 10, 100);
        let next = record(2, 1, 123, 11, 200);
        let io = MemoryIO::new();
        let file = io.open_file("manifest", OpenFlags::Create, false).unwrap();
        let mut missing = ManifestLoad::start(file.clone(), None, DATABASE_ID).unwrap();
        assert!(missing.poll().is_err());
        for (slot, record) in [(0, previous), (1, next)] {
            let buffer = Arc::new(Buffer::new_temporary(SLOT_SIZE));
            buffer.as_mut_slice().fill(0);
            buffer.as_mut_slice()[..RECORD_SIZE].copy_from_slice(&record.encode());
            drop(
                file.pwrite(
                    (slot * SLOT_SIZE) as u64,
                    buffer,
                    Completion::new_write(|_| {}),
                )
                .unwrap(),
            );
            let mut load = ManifestLoad::start(file.clone(), None, DATABASE_ID).unwrap();
            let IOResult::Done(loaded) = load.poll().unwrap() else {
                panic!("synchronous read")
            };
            assert_eq!(loaded.record, record);
            assert_eq!(loaded.slot, slot);
            assert!(load.poll().is_err());
        }
    }

    #[test]
    fn commit_manifest_load_handles_delayed_short_reads_and_torn_inactive_slot() {
        let previous = record(1, 1, 123, 10, 100);
        let next = record(2, 1, 123, 11, 200);
        for next_bytes in [0, 7, 127, 128] {
            let file = DelayedManifestFile::initialized(previous);
            if next_bytes > 0 {
                let mut bytes = file.bytes.lock();
                bytes.resize(SLOT_SIZE + next_bytes, 0);
                bytes[SLOT_SIZE..].copy_from_slice(&next.encode()[..next_bytes]);
            }
            let mut load = ManifestLoad::start(file.clone(), None, DATABASE_ID).unwrap();
            let mut waits = 0;
            loop {
                match load.poll().unwrap() {
                    IOResult::IO(_) => {
                        waits += 1;
                        file.retire_read(7);
                    }
                    IOResult::Done(loaded) => {
                        assert!(waits >= RECORD_SIZE.div_ceil(7));
                        assert_eq!(
                            loaded.record,
                            if next_bytes == RECORD_SIZE {
                                next
                            } else {
                                previous
                            }
                        );
                        break;
                    }
                }
            }
        }
        let file = DelayedManifestFile::initialized(previous);
        let mut load = ManifestLoad::start(file.clone(), None, DATABASE_ID).unwrap();
        assert!(matches!(load.poll().unwrap(), IOResult::IO(_)));
        file.retire_read(0);
        assert!(load.poll().is_err());
        assert!(load.poll().is_err());
    }

    fn record(
        sequence: u64,
        generation: u64,
        salt: u64,
        timestamp: u64,
        offset: u64,
    ) -> ManifestRecord {
        ManifestRecord::new(
            sequence,
            DATABASE_ID,
            generation,
            salt,
            PublishedPrefix {
                committed_timestamp: timestamp,
                end_offset: offset,
                running_crc: 17,
            },
        )
        .unwrap()
    }

    fn fix_checksum(bytes: &mut [u8; RECORD_SIZE]) {
        let checksum = crc32c::crc32c(&bytes[..CHECKSUM_OFFSET]);
        bytes[CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
    }
}
