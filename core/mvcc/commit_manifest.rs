use super::shared_authority::PublishedPrefix;
use crate::io::FileId;
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
