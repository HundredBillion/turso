pub(super) mod checkpoint_publication;

use super::shared_authority::{AuthorityGuard, IoPin, PublicationLease, PublishedPrefix};
use crate::io::FileId;
use crate::io::{Buffer, Completion, File, FileSyncType};
use crate::sync::{Arc, Mutex};
use crate::types::IOCompletions;
use crate::IOResult;
use crate::{LimboError, Result};

pub(super) const RECORD_SIZE: usize = 128;
const MAGIC: [u8; 8] = *b"TMVCCM01";
const VERSION: u32 = 1;
const CHECKPOINT_INTENT_VERSION: u32 = 2;
const DOWNGRADE_INTENT_VERSION: u32 = 3;
const CHECKSUM_OFFSET: usize = 124;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ManifestRecord {
    sequence: u64,
    database_id: FileId,
    log_generation: u64,
    log_salt: u64,
    prefix: PublishedPrefix,
    checkpoint: Option<CheckpointIntent>,
    downgrade: Option<DowngradeIntent>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CheckpointIntent {
    pub source_checkpoint_timestamp: u64,
    pub target_generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DowngradeIntent {
    pub basename_wal_id: FileId,
}

impl ManifestRecord {
    pub(super) fn sequence(&self) -> u64 {
        self.sequence
    }
    pub(super) fn database_id(&self) -> FileId {
        self.database_id
    }
    pub(super) fn log_generation(&self) -> u64 {
        self.log_generation
    }
    pub(super) fn log_salt(&self) -> u64 {
        self.log_salt
    }

    pub(super) fn prefix(&self) -> PublishedPrefix {
        self.prefix
    }

    pub(super) fn checkpoint_intent(&self) -> Option<CheckpointIntent> {
        self.checkpoint
    }

    pub(super) fn downgrade_intent(&self) -> Option<DowngradeIntent> {
        self.downgrade
    }

    pub(super) fn requires_recovery(&self) -> bool {
        self.checkpoint.is_some() || self.downgrade.is_some()
    }

    /// Encodes a transition request, not proof that physical materialization finished.
    /// Only the future coordinator-owned quiescent token may authorize publication.
    pub(super) fn begin_downgrade(mut self, basename_wal_id: FileId) -> Result<Self> {
        if self.requires_recovery()
            || self.prefix.end_offset != 0
            || self.log_salt != 0
            || basename_wal_id.dev != self.database_id.dev
            || basename_wal_id == self.database_id
        {
            return Err(LimboError::InvalidArgument(
                "invalid shared downgrade source".into(),
            ));
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(LimboError::DatabaseFull)?;
        self.downgrade = Some(DowngradeIntent { basename_wal_id });
        Ok(self)
    }

    pub(super) fn begin_checkpoint(mut self, source_checkpoint_timestamp: u64) -> Result<Self> {
        if self.requires_recovery()
            || source_checkpoint_timestamp > self.prefix.committed_timestamp
            || (self.prefix.end_offset == 0
                && source_checkpoint_timestamp != self.prefix.committed_timestamp)
        {
            return Err(LimboError::InvalidArgument(
                "invalid shared checkpoint source".into(),
            ));
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(LimboError::DatabaseFull)?;
        self.checkpoint = Some(CheckpointIntent {
            source_checkpoint_timestamp,
            target_generation: self
                .log_generation
                .checked_add(1)
                .ok_or(LimboError::DatabaseFull)?,
        });
        Ok(self)
    }

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
            checkpoint: None,
            downgrade: None,
        })
    }

    pub(super) fn encode(self) -> [u8; RECORD_SIZE] {
        let mut bytes = [0; RECORD_SIZE];
        bytes[..8].copy_from_slice(&MAGIC);
        let version = if self.downgrade.is_some() {
            DOWNGRADE_INTENT_VERSION
        } else if self.checkpoint.is_some() {
            CHECKPOINT_INTENT_VERSION
        } else {
            VERSION
        };
        bytes[8..12].copy_from_slice(&version.to_le_bytes());
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
        if let Some(intent) = self.checkpoint {
            bytes[80..88].copy_from_slice(&intent.source_checkpoint_timestamp.to_le_bytes());
            bytes[88..96].copy_from_slice(&intent.target_generation.to_le_bytes());
        }
        if let Some(intent) = self.downgrade {
            bytes[80..88].copy_from_slice(&intent.basename_wal_id.dev.to_le_bytes());
            bytes[88..96].copy_from_slice(&intent.basename_wal_id.ino.to_le_bytes());
        }
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
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if bytes[..8] != MAGIC
            || !matches!(
                version,
                VERSION | CHECKPOINT_INTENT_VERSION | DOWNGRADE_INTENT_VERSION
            )
        {
            return Err(LimboError::InvalidArgument(
                "incompatible MVCC commit manifest format".into(),
            ));
        }
        if u32::from_le_bytes(bytes[12..16].try_into().unwrap()) != RECORD_SIZE as u32
            || bytes[76..80].iter().any(|byte| *byte != 0)
            || bytes[96..CHECKSUM_OFFSET].iter().any(|byte| *byte != 0)
            || (version == VERSION && bytes[80..96].iter().any(|byte| *byte != 0))
        {
            return Err(LimboError::Corrupt(
                "invalid MVCC manifest record layout".into(),
            ));
        }
        let read_u64 = |offset| u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let mut record = Self::new(
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
        .map_err(|_| LimboError::Corrupt("invalid MVCC manifest record values".into()))?;
        if version == CHECKPOINT_INTENT_VERSION {
            let source_checkpoint_timestamp = read_u64(80);
            let target_generation = read_u64(88);
            if record.log_generation.checked_add(1) != Some(target_generation)
                || source_checkpoint_timestamp > record.prefix.committed_timestamp
                || (record.prefix.end_offset == 0
                    && source_checkpoint_timestamp != record.prefix.committed_timestamp)
            {
                return Err(LimboError::Corrupt("invalid MVCC checkpoint intent".into()));
            }
            record.checkpoint = Some(CheckpointIntent {
                source_checkpoint_timestamp,
                target_generation,
            });
        }
        if version == DOWNGRADE_INTENT_VERSION {
            let basename_wal_id = FileId {
                dev: read_u64(80),
                ino: read_u64(88),
            };
            if record.sequence < 2
                || record.prefix.end_offset != 0
                || record.log_salt != 0
                || basename_wal_id.dev != record.database_id.dev
                || basename_wal_id == record.database_id
            {
                return Err(LimboError::Corrupt("invalid MVCC downgrade intent".into()));
            }
            record.downgrade = Some(DowngradeIntent { basename_wal_id });
        }
        Ok(Some(record))
    }
}

pub(super) fn select_latest(
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
    if previous.downgrade.is_some() {
        return Err(LimboError::Corrupt(
            "MVCC downgrade intent is terminal".into(),
        ));
    }
    if newest.downgrade.is_some()
        && (previous.requires_recovery()
            || previous.sequence.checked_add(1) != Some(newest.sequence)
            || previous.log_generation != newest.log_generation
            || previous.prefix != newest.prefix
            || previous.log_salt != 0
            || newest.log_salt != 0)
    {
        return Err(LimboError::Corrupt(
            "invalid MVCC downgrade manifest transition".into(),
        ));
    }
    let valid_checkpoint_transition = match (previous.checkpoint, newest.checkpoint) {
        (None, None) => true,
        (None, Some(_)) => {
            newest.log_generation == previous.log_generation
                && newest.log_salt == previous.log_salt
                && newest.prefix == previous.prefix
        }
        (Some(old), Some(new)) => {
            old == new
                && newest.log_generation == previous.log_generation
                && newest.log_salt == previous.log_salt
                && newest.prefix == previous.prefix
        }
        (Some(intent), None) => {
            newest.log_generation == intent.target_generation
                && newest.prefix.committed_timestamp == previous.prefix.committed_timestamp
                && newest.prefix.end_offset == 0
                && newest.prefix.running_crc == 0
                && newest.log_salt == 0
        }
    };
    if !valid_checkpoint_transition {
        return Err(LimboError::Corrupt(
            "invalid MVCC checkpoint manifest transition".into(),
        ));
    }
    // An empty generation has no log header yet. Its first durable append
    // establishes the salt without replacing any acknowledged log bytes.
    let initializes_empty_log =
        previous.prefix.end_offset == 0 && previous.log_salt == 0 && newest.prefix.end_offset > 0;
    if newest.log_generation < previous.log_generation
        || newest.prefix.committed_timestamp < previous.prefix.committed_timestamp
        || (newest.log_generation == previous.log_generation
            && ((newest.log_salt != previous.log_salt && !initializes_empty_log)
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

pub(super) const SLOT_SIZE: usize = 4096;

fn pin_authority(ownership: &Mutex<Option<AuthorityGuard>>) -> Result<Option<IoPin>> {
    ownership
        .lock()
        .as_ref()
        .map(|guard| PublicationLease::pin(&guard.io_lease()))
        .transpose()
}

pub(super) struct ManifestLoad {
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
    pub(super) fn new(
        file: Arc<dyn File>,
        guard: AuthorityGuard,
        database_id: FileId,
    ) -> Result<Self> {
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

    pub(super) fn poll(&mut self) -> Result<IOResult<LoadedManifest>> {
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
            let pin = pin_authority(&self.ownership)?;
            let ownership = self.ownership.clone();
            let completion = Completion::new_read(buffer.clone(), move |result| {
                let _keep_alive = (&ownership, &pin);
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

pub(super) struct LoadedManifest {
    file: Arc<dyn File>,
    guard: Option<AuthorityGuard>,
    slot: usize,
    record: ManifestRecord,
}

pub(super) struct DurableManifest {
    guard: Option<AuthorityGuard>,
    slot: usize,
    record: ManifestRecord,
}

impl DurableManifest {
    pub(super) fn prefix(&self) -> PublishedPrefix {
        self.record.prefix
    }

    pub(super) fn publish(mut self) -> Result<AuthorityGuard> {
        let mut guard = self.guard.take().ok_or_else(|| {
            LimboError::InvalidArgument("manifest publication has no authority".into())
        })?;
        guard.publish_committed(&self)?;
        Ok(guard)
    }
}

pub(super) struct ManifestPublication {
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
    pub(super) fn record(&self) -> ManifestRecord {
        self.record
    }

    pub(super) fn guard(&self) -> Result<&AuthorityGuard> {
        self.guard
            .as_ref()
            .ok_or_else(|| LimboError::InvalidArgument("manifest has no authority".into()))
    }

    pub(super) fn guard_mut(&mut self) -> Result<&mut AuthorityGuard> {
        self.guard
            .as_mut()
            .ok_or_else(|| LimboError::InvalidArgument("manifest has no authority".into()))
    }

    pub(super) fn into_guard(mut self) -> Result<AuthorityGuard> {
        self.guard
            .take()
            .ok_or_else(|| LimboError::InvalidArgument("manifest has no authority".into()))
    }

    pub(super) fn publish(self, next: ManifestRecord) -> Result<ManifestPublication> {
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
    pub(super) fn poll(&mut self) -> Result<IOResult<DurableManifest>> {
        use std::sync::atomic::{AtomicI32, Ordering};
        loop {
            match std::mem::replace(&mut self.stage, PublicationStage::Finished) {
                PublicationStage::Start => {
                    let buffer = Arc::new(Buffer::new_temporary(SLOT_SIZE));
                    buffer.as_mut_slice().fill(0);
                    buffer.as_mut_slice()[..RECORD_SIZE].copy_from_slice(&self.record.encode());
                    let written = Arc::new(AtomicI32::new(-1));
                    let count = written.clone();
                    let pin = pin_authority(&self.ownership)?;
                    let ownership = self.ownership.clone();
                    let completion = Completion::new_write(move |result| {
                        // The pin survives successful guard handoff as well as caller abort.
                        let _keep_alive = (&ownership, &pin);
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
                    let pin = pin_authority(&self.ownership)?;
                    let ownership = self.ownership.clone();
                    let sync = Completion::new_sync(move |_| {
                        let _keep_alive = (&ownership, &pin);
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

    const WAL_ID: FileId = FileId { dev: 1, ino: 3 };

    #[test]
    fn commit_manifest_downgrade_roundtrip_is_distinct_terminal_state() {
        let normal = record(5, 3, 0, 20, 0);
        let intent = normal.begin_downgrade(WAL_ID).unwrap();
        assert_eq!(intent.sequence(), 6);
        assert_eq!(intent.log_generation(), 3);
        assert_eq!(intent.prefix(), normal.prefix());
        assert_eq!(
            intent.downgrade_intent(),
            Some(DowngradeIntent {
                basename_wal_id: WAL_ID
            })
        );
        assert!(intent.checkpoint_intent().is_none());
        assert!(intent.requires_recovery());
        assert!(!normal.requires_recovery());
        assert_eq!(
            u32::from_le_bytes(intent.encode()[8..12].try_into().unwrap()),
            3
        );
        assert_eq!(
            ManifestRecord::decode(&intent.encode()).unwrap(),
            Some(intent)
        );
        assert_eq!(
            select_latest([Some(normal), Some(intent)], DATABASE_ID)
                .unwrap()
                .1,
            intent
        );
        assert!(intent.begin_checkpoint(20).is_err());
        assert!(intent.begin_downgrade(WAL_ID).is_err());
        for next in [
            record(7, 3, 0, 20, 0),
            record(7, 3, 9, 21, 128),
            record(7, 4, 0, 20, 0),
        ] {
            assert!(select_latest([Some(intent), Some(next)], DATABASE_ID).is_err());
        }
    }

    #[test]
    fn commit_manifest_downgrade_requires_empty_normal_state_and_exact_identity() {
        for source in [
            record(5, 3, 7, 20, 128),
            record(5, 3, 7, 20, 0),
            record(5, 3, 0, 20, 0).begin_checkpoint(20).unwrap(),
        ] {
            assert!(source.begin_downgrade(WAL_ID).is_err());
        }
        let source = record(5, 3, 0, 20, 0);
        assert!(source.begin_downgrade(DATABASE_ID).is_err());
        assert!(source.begin_downgrade(FileId { dev: 2, ino: 3 }).is_err());
        assert!(record(u64::MAX, 3, 0, 20, 0)
            .begin_downgrade(WAL_ID)
            .is_err());
        let intent = source.begin_downgrade(WAL_ID).unwrap();
        for previous in [
            record(4, 3, 0, 20, 0),
            record(5, 2, 0, 20, 0),
            record(5, 3, 0, 19, 0),
            record(5, 3, 7, 20, 128),
        ] {
            assert!(select_latest([Some(previous), Some(intent)], DATABASE_ID).is_err());
        }
        let other_wal = source.begin_downgrade(FileId { dev: 1, ino: 4 }).unwrap();
        assert!(select_latest([Some(intent), Some(other_wal)], DATABASE_ID).is_err());
    }

    #[test]
    fn commit_manifest_downgrade_mirrors_fence_after_loss_of_either_copy() {
        let intent = record(5, 3, 0, 20, 0).begin_downgrade(WAL_ID).unwrap();
        let bytes = intent.encode();
        for cut in 0..RECORD_SIZE {
            let torn = ManifestRecord::decode(&bytes[..cut]).unwrap();
            for records in [[Some(intent), torn], [torn, Some(intent)]] {
                let selected = select_latest(records, DATABASE_ID).unwrap().1;
                assert_eq!(selected, intent);
                assert!(selected.requires_recovery());
            }
        }
        for byte in 0..RECORD_SIZE {
            for bit in 0..8 {
                let mut torn = bytes;
                torn[byte] ^= 1 << bit;
                let torn = ManifestRecord::decode(&torn).unwrap();
                assert_eq!(
                    select_latest([torn, Some(intent)], DATABASE_ID).unwrap().1,
                    intent
                );
            }
        }
        let mut unknown = bytes;
        unknown[8..12].copy_from_slice(&4u32.to_le_bytes());
        fix_checksum(&mut unknown);
        assert!(ManifestRecord::decode(&unknown).is_err());
        for offset in [48, 64, 76, 96] {
            let mut invalid = bytes;
            invalid[offset] = 1;
            fix_checksum(&mut invalid);
            assert!(ManifestRecord::decode(&invalid).is_err(), "offset {offset}");
        }
    }

    #[test]
    fn commit_manifest_generated_downgrade_intents_roundtrip() {
        fn property(sequence: u64, generation: u64, timestamp: u64, wal_inode: u64) -> bool {
            let source = record(
                sequence.clamp(1, u64::MAX - 1),
                generation.max(1),
                0,
                timestamp & ((1 << 62) - 1),
                0,
            );
            let intent = source
                .begin_downgrade(FileId {
                    dev: DATABASE_ID.dev,
                    ino: wal_inode.max(3),
                })
                .unwrap();
            ManifestRecord::decode(&intent.encode()).unwrap() == Some(intent)
                && select_latest([Some(source), Some(intent)], DATABASE_ID)
                    .unwrap()
                    .1
                    == intent
        }
        quickcheck::QuickCheck::new()
            .tests(200)
            .quickcheck(property as fn(u64, u64, u64, u64) -> bool);
    }

    #[test]
    fn commit_manifest_checkpoint_intent_roundtrip_retains_old_base_and_exact_target() {
        let normal = record(5, 3, 7, 20, 128);
        let intent = normal.begin_checkpoint(12).unwrap();
        assert_eq!(
            intent.checkpoint_intent(),
            Some(CheckpointIntent {
                source_checkpoint_timestamp: 12,
                target_generation: 4,
            })
        );
        assert_eq!(intent.prefix(), normal.prefix());
        assert_eq!(intent.sequence(), normal.sequence() + 1);
        assert_eq!(
            ManifestRecord::decode(&intent.encode()).unwrap(),
            Some(intent)
        );
    }

    #[test]
    fn commit_manifest_checkpoint_intent_only_completes_at_its_materialized_boundary() {
        let normal = record(5, 3, 7, 20, 128);
        let intent = normal.begin_checkpoint(12).unwrap();
        let completed = record(7, 4, 0, 20, 0);
        assert_eq!(
            select_latest([Some(intent), Some(completed)], DATABASE_ID)
                .unwrap()
                .1,
            completed
        );
        for invalid in [
            record(7, 3, 7, 21, 200),
            record(7, 5, 0, 20, 0),
            record(7, 4, 0, 21, 0),
            record(7, 4, 7, 20, 128),
        ] {
            assert!(select_latest([Some(intent), Some(invalid)], DATABASE_ID).is_err());
        }
        assert!(intent.begin_checkpoint(12).is_err());
        assert!(normal.begin_checkpoint(21).is_err());
    }

    #[test]
    fn commit_manifest_mirrored_intent_survives_loss_of_either_copy() {
        let normal = record(5, 3, 7, 20, 128);
        let intent = normal.begin_checkpoint(12).unwrap();
        let encoded = intent.encode();
        assert_eq!(
            u32::from_le_bytes(encoded[8..12].try_into().unwrap()),
            CHECKPOINT_INTENT_VERSION
        );
        assert_eq!(
            u32::from_le_bytes(normal.encode()[8..12].try_into().unwrap()),
            VERSION
        );
        for cut in 0..RECORD_SIZE {
            let torn = ManifestRecord::decode(&encoded[..cut]).unwrap();
            assert_eq!(
                select_latest([Some(normal), torn], DATABASE_ID).unwrap().1,
                normal
            );
            assert_eq!(
                select_latest([Some(intent), torn], DATABASE_ID).unwrap().1,
                intent
            );
            assert_eq!(
                select_latest([torn, Some(intent)], DATABASE_ID).unwrap().1,
                intent
            );
        }
        for byte in 0..RECORD_SIZE {
            for bit in 0..8 {
                let mut torn = encoded;
                torn[byte] ^= 1 << bit;
                let torn = ManifestRecord::decode(&torn).unwrap();
                assert_eq!(
                    select_latest([Some(intent), torn], DATABASE_ID).unwrap().1,
                    intent
                );
            }
        }
        for offset in [80, 88] {
            let mut malformed = encoded;
            malformed[offset..offset + 8].fill(255);
            fix_checksum(&mut malformed);
            assert!(ManifestRecord::decode(&malformed).is_err());
        }
    }

    #[test]
    fn commit_manifest_generated_checkpoint_intents_roundtrip() {
        fn property(sequence: u64, generation: u64, timestamp: u64, source: u64) -> bool {
            let sequence = sequence.clamp(1, u64::MAX - 1);
            let generation = generation.clamp(1, u64::MAX - 1);
            let timestamp = timestamp & ((1 << 62) - 1);
            let normal = record(sequence, generation, 7, timestamp, 128);
            let intent = normal.begin_checkpoint(source % (timestamp + 1)).unwrap();
            ManifestRecord::decode(&intent.encode()).unwrap() == Some(intent)
                && select_latest([Some(normal), Some(intent)], DATABASE_ID)
                    .unwrap()
                    .1
                    == intent
        }
        quickcheck::QuickCheck::new()
            .tests(200)
            .quickcheck(property as fn(u64, u64, u64, u64) -> bool);
    }

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
    fn commit_manifest_initializes_salt_only_after_an_empty_prefix() {
        let empty = ManifestRecord::new(
            1,
            DATABASE_ID,
            7,
            0,
            PublishedPrefix {
                committed_timestamp: 10,
                end_offset: 0,
                running_crc: 0,
            },
        )
        .unwrap();
        let initialized = record(2, 7, 123, 11, 100);
        assert_eq!(
            select_latest([Some(empty), Some(initialized)], DATABASE_ID).unwrap(),
            (1, initialized)
        );
        let rebound = record(3, 7, 456, 12, 200);
        assert!(select_latest([Some(initialized), Some(rebound)], DATABASE_ID).is_err());
        let mut invalid_empty = empty;
        invalid_empty.log_salt = 99;
        assert!(select_latest([Some(invalid_empty), Some(initialized)], DATABASE_ID).is_err());
        let mut no_bytes = empty;
        no_bytes.sequence = 2;
        no_bytes.log_salt = 123;
        assert!(select_latest([Some(empty), Some(no_bytes)], DATABASE_ID).is_err());
        let mut regressed = initialized;
        regressed.prefix.committed_timestamp = 9;
        assert!(select_latest([Some(empty), Some(regressed)], DATABASE_ID).is_err());
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
        fn shared_wal_try_lock_byte(
            &self,
            _: u64,
            _: bool,
            _: crate::io::SharedWalLockKind,
        ) -> Result<bool> {
            Ok(true)
        }
        fn shared_wal_unlock_byte(&self, _: u64, _: crate::io::SharedWalLockKind) -> Result<()> {
            Ok(())
        }
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
            assert!(pos == 0 || pos == SLOT_SIZE as u64);
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

    #[cfg(not(shuttle))]
    #[test]
    fn commit_manifest_checkpoint_intent_proof_waits_for_both_syncs_and_retirement() {
        use super::super::shared_authority::authority_for_test;
        let previous = record(1, 1, 123, 10, 100);
        let file = DelayedManifestFile::initialized(previous);
        let authority = authority_for_test(file.clone());
        let mut guard = authority.try_acquire().unwrap();
        guard.recover(previous.prefix()).unwrap();
        let mut publication = LoadedManifest {
            file: file.clone(),
            guard: Some(guard),
            slot: 0,
            record: previous,
        }
        .begin_checkpoint_intent(0)
        .unwrap();
        for step in 0..4 {
            assert!(
                matches!(publication.poll().unwrap(), IOResult::IO(_)),
                "proof escaped at step {step}"
            );
            if step == 3 {
                let retained = file.pending.lock().as_ref().unwrap().1.clone();
                file.retire(0, false);
                let IOResult::IO(waiter) = publication.poll().unwrap() else {
                    panic!("backend retirement still pending")
                };
                assert!(!waiter.0.finished());
                drop(retained);
            } else {
                file.retire(if step % 2 == 0 { SLOT_SIZE } else { 0 }, false);
            }
        }
        let IOResult::Done(proof) = publication.poll().unwrap() else {
            panic!("both mirrored copies are durable")
        };
        assert_eq!(file.syncs.load(std::sync::atomic::Ordering::Relaxed), 2);
        let bytes = file.bytes.lock();
        for offset in [0, SLOT_SIZE] {
            assert_eq!(
                ManifestRecord::decode(&bytes[offset..offset + RECORD_SIZE]).unwrap(),
                Some(proof.record())
            );
        }
        assert_eq!(proof.guard().published().unwrap(), previous.prefix());
        proof.into_guard().finish().unwrap();
    }

    #[cfg(not(shuttle))]
    #[test]
    fn commit_manifest_checkpoint_intent_failure_and_abort_never_release_early() {
        use super::super::shared_authority::authority_for_test;
        for boundary in 0..4 {
            for abort in [false, true] {
                let previous = record(1, 1, 123, 10, 100);
                let file = DelayedManifestFile::initialized(previous);
                let authority = authority_for_test(file.clone());
                let mut guard = authority.try_acquire().unwrap();
                guard.recover(previous.prefix()).unwrap();
                let mut publication = LoadedManifest {
                    file: file.clone(),
                    guard: Some(guard),
                    slot: 0,
                    record: previous,
                }
                .begin_checkpoint_intent(0)
                .unwrap();
                for step in 0..=boundary {
                    let IOResult::IO(waiter) = publication.poll().unwrap() else {
                        panic!("early intent proof");
                    };
                    if step == boundary {
                        if abort {
                            waiter.0.abort();
                            drop(publication);
                            assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
                            file.retire(if step % 2 == 0 { SLOT_SIZE } else { 0 }, false);
                        } else {
                            file.retire(0, true);
                            assert!(publication.poll().is_err());
                            assert!(publication.poll().is_err());
                            drop(publication);
                        }
                        drop(waiter);
                        let guard = authority.try_acquire().unwrap();
                        assert!(guard.requires_recovery());
                        break;
                    }
                    drop(waiter);
                    file.retire(if step % 2 == 0 { SLOT_SIZE } else { 0 }, false);
                }
            }
        }
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

    #[cfg(not(shuttle))]
    #[test]
    fn commit_manifest_success_handoff_retains_backend_ownership() {
        use super::super::shared_authority::authority_for_test;
        for retained_write in [true, false] {
            let previous = record(1, 1, 123, 10, 100);
            let next = record(2, 1, 123, 11, 200);
            let file = DelayedManifestFile::initialized(previous);
            let authority = authority_for_test(file.clone());
            let mut guard = authority.try_acquire().unwrap();
            guard.recover(previous.prefix()).unwrap();
            let mut publication = LoadedManifest {
                file: file.clone(),
                guard: Some(guard),
                slot: 0,
                record: previous,
            }
            .publish(next)
            .unwrap();
            assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
            let mut retained =
                retained_write.then(|| file.pending.lock().as_ref().unwrap().1.clone());
            file.retire(SLOT_SIZE, false);
            assert!(matches!(publication.poll().unwrap(), IOResult::IO(_)));
            if !retained_write {
                retained = Some(file.pending.lock().as_ref().unwrap().1.clone());
            }
            file.retire(0, false);
            let IOResult::Done(durable) = publication.poll().unwrap() else {
                panic!("sync completed")
            };
            let guard = durable.publish().unwrap();
            drop(publication);
            assert!(
                !guard.io_retired(),
                "successful handoff lost a backend lease pin"
            );
            drop(guard);
            assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
            drop(retained);
            let mut guard = authority.try_acquire().unwrap();
            assert!(guard.requires_recovery());
            guard.recover(next.prefix()).unwrap();
            guard.finish().unwrap();
        }
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
                running_crc: if offset == 0 { 0 } else { 17 },
            },
        )
        .unwrap()
    }

    fn fix_checksum(bytes: &mut [u8; RECORD_SIZE]) {
        let checksum = crc32c::crc32c(&bytes[..CHECKSUM_OFFSET]);
        bytes[CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
    }
}
#[cfg(all(test, any(target_os = "linux", target_os = "macos"), not(shuttle)))]
mod process_tests {
    use super::super::shared_authority::SharedMvccAuthority;
    use super::*;
    use crate::io::{OpenFlags, SyscallIO};
    use crate::mvcc::database::{LogRecord, PackedTs, RowVersion, TxTimestampOrID, WalPos};
    use crate::mvcc::persistent_storage::logical_log::{LogicalLog, StreamingLogicalLogReader};
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    const DATABASE_ID: FileId = FileId { dev: 1, ino: 2 };
    const CHILD_TEST: &str = "mvcc::commit_manifest::process_tests::commit_manifest_process_child";

    fn boundary() {
        println!("MANIFEST_READY");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
    }

    struct PausingFile {
        inner: Arc<dyn File>,
        phase: String,
    }
    impl File for PausingFile {
        fn lock_file(&self, exclusive: bool) -> Result<()> {
            self.inner.lock_file(exclusive)
        }
        fn unlock_file(&self) -> Result<()> {
            self.inner.unlock_file()
        }
        fn pread(&self, offset: u64, c: Completion) -> Result<Completion> {
            self.inner.pread(offset, c)
        }
        fn pwrite(&self, offset: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            let c = self.inner.pwrite(offset, buffer, c)?;
            assert!(c.succeeded());
            if self.phase == "written" {
                boundary();
            }
            Ok(c)
        }
        fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
            if self.phase == "before_sync" {
                boundary();
            }
            let c = self.inner.sync(c, sync_type)?;
            assert!(c.succeeded());
            if self.phase == "synced" {
                boundary();
            }
            Ok(c)
        }
        fn size(&self) -> Result<u64> {
            self.inner.size()
        }
        fn truncate(&self, n: u64, c: Completion) -> Result<Completion> {
            self.inner.truncate(n, c)
        }
    }

    fn open(io: &Arc<dyn crate::IO>, dir: &std::path::Path, name: &str) -> Arc<dyn File> {
        io.open_file(
            dir.join(name).to_str().unwrap(),
            OpenFlags::Create | OpenFlags::NoLock,
            false,
        )
        .unwrap()
    }

    fn synced_write(path: &std::path::Path, bytes: &[u8]) {
        let mut file = std::fs::File::create(path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }

    fn seed(dir: &std::path::Path) {
        let io: Arc<dyn crate::IO> = Arc::new(SyscallIO::new().unwrap());
        let mut log = LogicalLog::new(open(&io, dir, "log"), io.clone(), None);
        for timestamp in [10, 20] {
            let row = crate::mvcc::database::tests::generate_simple_string_row(
                (-2).into(),
                timestamp as i64,
                "durable payload",
            );
            let version = RowVersion {
                id: timestamp,
                begin: PackedTs::pack(Some(TxTimestampOrID::Timestamp(timestamp))),
                end: PackedTs::pack(None),
                data: row.data,
                btree_resident: false,
                materialized_at: WalPos::ORIGIN,
            };
            io.wait_for_completion(
                log.log_tx(LogRecord::for_test(timestamp, &[(row.id, version)], None))
                    .unwrap(),
            )
            .unwrap();
            io.wait_for_completion(log.sync(FileSyncType::FullFsync).unwrap())
                .unwrap();
            let record = ManifestRecord::new(
                timestamp / 10,
                DATABASE_ID,
                1,
                log.header().unwrap().salt,
                PublishedPrefix {
                    committed_timestamp: timestamp,
                    end_offset: log.offset,
                    running_crc: log.running_crc,
                },
            )
            .unwrap();
            if timestamp == 10 {
                let mut block = [0; SLOT_SIZE];
                block[..RECORD_SIZE].copy_from_slice(&record.encode());
                synced_write(&dir.join("manifest"), &block);
            } else {
                synced_write(&dir.join("successor"), &record.encode());
            }
        }
        synced_write(&dir.join("acknowledged"), &10u64.to_le_bytes());
        std::fs::File::open(dir).unwrap().sync_all().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn commit_manifest_process_death_preserves_acknowledged_log_prefix() {
        for phase in [
            "before_write",
            "written",
            "before_sync",
            "synced",
            "published",
            "acknowledged",
        ] {
            let dir = tempfile::tempdir().unwrap();
            seed(dir.path());
            let mut publisher = spawn(dir.path(), phase);
            publisher.kill();
            let mut recovery = spawn(dir.path(), "recover");
            recovery.kill();
        }
    }

    #[test]
    fn commit_manifest_process_fixture_has_synced_real_log_prefixes() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path());
        let io: Arc<dyn crate::IO> = Arc::new(SyscallIO::new().unwrap());
        let mut load =
            ManifestLoad::start(open(&io, dir.path(), "manifest"), None, DATABASE_ID).unwrap();
        let IOResult::Done(loaded) = load.poll().unwrap() else {
            panic!("syscall load yields")
        };
        let next = ManifestRecord::decode(&std::fs::read(dir.path().join("successor")).unwrap())
            .unwrap()
            .unwrap();
        for record in [loaded.record, next] {
            let mut reader = StreamingLogicalLogReader::new(open(&io, dir.path(), "log"), None)
                .with_committed_prefix(record.prefix.end_offset, record.prefix.running_crc)
                .unwrap();
            reader.read_header(&io).unwrap();
            let mut frames = 0;
            while let Some(frame) = reader.next_frame_blocking(&io).unwrap() {
                assert_eq!(frame.len(), 1);
                frames += 1;
            }
            assert_eq!(frames, record.prefix.committed_timestamp / 10);
        }
    }

    #[test]
    fn commit_manifest_process_child() {
        let Ok(dir) = std::env::var("TURSO_MANIFEST_PROCESS_DIR") else {
            return;
        };
        let dir = std::path::Path::new(&dir);
        let phase = std::env::var("TURSO_MANIFEST_PROCESS_PHASE").unwrap();
        let io: Arc<dyn crate::IO> = Arc::new(SyscallIO::new().unwrap());
        let authority = SharedMvccAuthority::open(
            &io,
            dir.join("authority").to_str().unwrap(),
            DATABASE_ID,
            8192,
        )
        .unwrap();
        let guard = authority.try_acquire().unwrap();
        let file: Arc<dyn File> = Arc::new(PausingFile {
            inner: open(&io, dir, "manifest"),
            phase: phase.clone(),
        });
        let mut load = ManifestLoad::new(file, guard, DATABASE_ID).unwrap();
        let IOResult::Done(mut loaded) = load.poll().unwrap() else {
            panic!("syscall load yields")
        };
        let log_file = open(&io, dir, "log");
        let mut reader = StreamingLogicalLogReader::new(log_file, None)
            .with_committed_prefix(
                loaded.record.prefix.end_offset,
                loaded.record.prefix.running_crc,
            )
            .unwrap();
        reader.read_header(&io).unwrap();
        assert_eq!(reader.header().unwrap().salt, loaded.record.log_salt);
        let mut frames = 0;
        while let Some(frame) = reader.next_frame_blocking(&io).unwrap() {
            assert_eq!(frame.len(), 1);
            frames += 1;
        }
        assert_eq!(frames, loaded.record.prefix.committed_timestamp / 10);
        let acknowledged = u64::from_le_bytes(
            std::fs::read(dir.join("acknowledged"))
                .unwrap()
                .try_into()
                .unwrap(),
        );
        // A complete unacknowledged successor is acceptable only because its log prefix was synced before publication and verified above.
        assert!(loaded.record.prefix.committed_timestamp >= acknowledged);
        loaded
            .guard
            .as_mut()
            .unwrap()
            .recover(loaded.record.prefix)
            .unwrap();
        if phase == "recover" {
            loaded.guard.take().unwrap().finish().unwrap();
            boundary();
            return;
        }
        let next = ManifestRecord::decode(&std::fs::read(dir.join("successor")).unwrap())
            .unwrap()
            .unwrap();
        if phase == "before_write" {
            boundary();
        }
        let mut publication = loaded.publish(next).unwrap();
        let IOResult::Done(durable) = publication.poll().unwrap() else {
            panic!("syscall publish yields")
        };
        let committed_timestamp = durable.prefix().committed_timestamp;
        durable.publish().unwrap().finish().unwrap();
        if phase == "published" {
            boundary();
        }
        synced_write(
            &dir.join("acknowledged"),
            &committed_timestamp.to_le_bytes(),
        );
        boundary();
    }

    fn spawn(dir: &std::path::Path, phase: &str) -> ChildProcess {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env("TURSO_MANIFEST_PROCESS_DIR", dir)
            .env("TURSO_MANIFEST_PROCESS_PHASE", phase)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut child = ChildProcess(child);
        let stdout = child.0.stdout.take().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap().contains("MANIFEST_READY") {
                    let _ = sender.send(());
                    return;
                }
            }
        });
        receiver
            .recv_timeout(Duration::from_secs(20))
            .unwrap_or_else(|error| panic!("phase {phase}: {error}"));
        child
    }

    struct ChildProcess(Child);
    impl ChildProcess {
        fn kill(&mut self) {
            self.0.kill().unwrap();
            self.0.wait().unwrap();
        }
    }
    impl Drop for ChildProcess {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
