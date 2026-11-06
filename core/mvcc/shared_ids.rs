use crate::io::FileId;
use crate::{LimboError, Result};

const PAGE_SIZE: usize = 4096;
const SELECTOR_SIZE: usize = PAGE_SIZE;
const SELECTOR_CRC: usize = SELECTOR_SIZE - 4;
const BANK_HEADER_SIZE: usize = 128;
const BANK_HEADER_CRC: usize = BANK_HEADER_SIZE - 4;
const SELECTOR_MAGIC: [u8; 8] = *b"TMVCCIS1";
const BANK_MAGIC: [u8; 8] = *b"TMVCCIB1";
const VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DatabaseGeneration {
    pub generation: u64,
    pub checkpoint_timestamp: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BankRef {
    bank: u8,
    bank_generation: u64,
    database: DatabaseGeneration,
    end: u64,
    snapshot_end: u64,
    running_crc: u32,
}

impl BankRef {
    fn validate(&self) -> Result<()> {
        if self.bank > 1
            || self.bank_generation == 0
            || self.database.generation == 0
            || self.database.checkpoint_timestamp >= (1 << 62)
            || self.snapshot_end < PAGE_SIZE as u64
            || self.snapshot_end > self.end
            || self.end % PAGE_SIZE as u64 != 0
            || self.snapshot_end % PAGE_SIZE as u64 != 0
        {
            return Err(corrupt("invalid ID bank reference"));
        }
        Ok(())
    }

    fn encode(&self, bytes: &mut [u8]) {
        for (offset, value) in [
            (0, u64::from(self.bank)),
            (8, self.bank_generation),
            (16, self.database.generation),
            (24, self.database.checkpoint_timestamp),
            (32, self.end),
            (40, self.snapshot_end),
            (48, u64::from(self.running_crc)),
        ] {
            put_u64(bytes, offset, value);
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes[56..64].iter().any(|byte| *byte != 0) {
            return Err(corrupt("invalid ID bank reference padding"));
        }
        let reference = Self {
            bank: read_u64(bytes, 0)
                .try_into()
                .map_err(|_| corrupt("invalid ID bank number"))?,
            bank_generation: read_u64(bytes, 8),
            database: DatabaseGeneration {
                generation: read_u64(bytes, 16),
                checkpoint_timestamp: read_u64(bytes, 24),
            },
            end: read_u64(bytes, 32),
            snapshot_end: read_u64(bytes, 40),
            running_crc: read_u64(bytes, 48)
                .try_into()
                .map_err(|_| corrupt("invalid ID stream checksum"))?,
        };
        reference.validate()?;
        Ok(reference)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct IdSelector {
    sequence: u64,
    database_id: FileId,
    active: BankRef,
    prepared: Option<BankRef>,
}

impl IdSelector {
    fn validate(&self) -> Result<()> {
        if self.sequence == 0 {
            return Err(corrupt("invalid ID selector sequence"));
        }
        self.active.validate()?;
        if let Some(next) = self.prepared {
            next.validate()?;
            if next.bank == self.active.bank
                || next.bank_generation <= self.active.bank_generation
                || next.database.generation <= self.active.database.generation
                || next.database.checkpoint_timestamp < self.active.database.checkpoint_timestamp
            {
                return Err(corrupt("invalid prepared ID bank transition"));
            }
        }
        Ok(())
    }

    fn encode(&self) -> Result<[u8; PAGE_SIZE]> {
        self.validate()?;
        let mut bytes = [0; PAGE_SIZE];
        bytes[..8].copy_from_slice(&SELECTOR_MAGIC);
        bytes[8..12].copy_from_slice(&VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&(SELECTOR_SIZE as u32).to_le_bytes());
        put_u64(&mut bytes, 16, self.sequence);
        put_u64(&mut bytes, 24, self.database_id.dev);
        put_u64(&mut bytes, 32, self.database_id.ino);
        self.active.encode(&mut bytes[40..104]);
        if let Some(next) = self.prepared {
            put_u64(&mut bytes, 104, 1);
            next.encode(&mut bytes[112..176]);
        }
        let crc = crc32c::crc32c(&bytes[..SELECTOR_CRC]);
        bytes[SELECTOR_CRC..SELECTOR_SIZE].copy_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Option<Self>> {
        if bytes.len() < PAGE_SIZE {
            return Ok(None);
        }
        if bytes.len() != PAGE_SIZE {
            return Err(corrupt("invalid ID selector slot length"));
        }
        let crc = u32::from_le_bytes(bytes[SELECTOR_CRC..SELECTOR_SIZE].try_into().unwrap());
        if crc32c::crc32c(&bytes[..SELECTOR_CRC]) != crc {
            return Ok(None);
        }
        if bytes[..8] != SELECTOR_MAGIC || read_u32(bytes, 8) != VERSION {
            return Err(LimboError::InvalidArgument(
                "incompatible ID selector protocol".into(),
            ));
        }
        if read_u32(bytes, 12) != SELECTOR_SIZE as u32
            || bytes[176..SELECTOR_CRC].iter().any(|byte| *byte != 0)
            || bytes[SELECTOR_SIZE..].iter().any(|byte| *byte != 0)
        {
            return Err(corrupt("invalid ID selector layout"));
        }
        let prepared = match read_u64(bytes, 104) {
            0 if bytes[112..176].iter().all(|byte| *byte == 0) => None,
            1 => Some(BankRef::decode(&bytes[112..176])?),
            _ => return Err(corrupt("invalid prepared ID bank flag")),
        };
        let selector = Self {
            sequence: read_u64(bytes, 16),
            database_id: FileId {
                dev: read_u64(bytes, 24),
                ino: read_u64(bytes, 32),
            },
            active: BankRef::decode(&bytes[40..104])?,
            prepared,
        };
        selector.validate()?;
        Ok(Some(selector))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BankHeader {
    database_id: FileId,
    bank_generation: u64,
    database: DatabaseGeneration,
}

impl BankHeader {
    fn encode(&self) -> Result<[u8; PAGE_SIZE]> {
        if self.bank_generation == 0
            || self.database.generation == 0
            || self.database.checkpoint_timestamp >= (1 << 62)
        {
            return Err(corrupt("invalid ID bank header"));
        }
        let mut bytes = [0; PAGE_SIZE];
        bytes[..8].copy_from_slice(&BANK_MAGIC);
        bytes[8..12].copy_from_slice(&VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&(BANK_HEADER_SIZE as u32).to_le_bytes());
        for (offset, value) in [
            (16, self.database_id.dev),
            (24, self.database_id.ino),
            (32, self.bank_generation),
            (40, self.database.generation),
            (48, self.database.checkpoint_timestamp),
        ] {
            put_u64(&mut bytes, offset, value);
        }
        let crc = crc32c::crc32c(&bytes[..BANK_HEADER_CRC]);
        bytes[BANK_HEADER_CRC..BANK_HEADER_SIZE].copy_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    fn validate(bytes: &[u8], id: FileId, reference: BankRef) -> Result<()> {
        if bytes.len() < PAGE_SIZE {
            return Err(corrupt("truncated selected ID bank header"));
        }
        let expected = Self {
            database_id: id,
            bank_generation: reference.bank_generation,
            database: reference.database,
        }
        .encode()?;
        if bytes[..PAGE_SIZE] != expected {
            return Err(corrupt("selected ID bank header differs from its witness"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SelectedIds {
    selector: IdSelector,
    selected: BankRef,
    // Agreement in read bytes is not proof that either slot's sync completed.
    slots_agree: bool,
}

fn select(
    slots: [&[u8]; 2],
    banks: [&[u8]; 2],
    database_id: FileId,
    database: DatabaseGeneration,
) -> Result<SelectedIds> {
    let records = [IdSelector::decode(slots[0])?, IdSelector::decode(slots[1])?];
    for record in records.iter().flatten() {
        if record.database_id != database_id {
            return Err(corrupt("ID selector belongs to a different database"));
        }
    }
    let selector = match records {
        [Some(first), Some(second)] if first.sequence == second.sequence => {
            if first != second {
                return Err(corrupt("conflicting ID selectors at one sequence"));
            }
            first
        }
        [Some(first), Some(second)] => {
            if first.sequence > second.sequence {
                first
            } else {
                second
            }
        }
        [Some(record), None] | [None, Some(record)] => record,
        [None, None] => return Err(corrupt("no valid ID selector witness")),
    };
    // Selection precedes data validation. A corrupt selected prefix never permits fallback.
    validate_bank(
        banks[usize::from(selector.active.bank)],
        database_id,
        selector.active,
    )?;
    if let Some(next) = selector.prepared {
        validate_bank(banks[usize::from(next.bank)], database_id, next)?;
    }
    let selected = if selector.active.database == database {
        selector.active
    } else if let Some(next) = selector.prepared.filter(|next| next.database == database) {
        next
    } else {
        return Err(corrupt(
            "ID selector does not cover the recovered database generation",
        ));
    };
    Ok(SelectedIds {
        selector,
        selected,
        slots_agree: records[0].is_some() && records[0] == records[1],
    })
}

fn validate_bank(bytes: &[u8], id: FileId, reference: BankRef) -> Result<()> {
    BankHeader::validate(bytes, id, reference)?;
    let end: usize = reference
        .end
        .try_into()
        .map_err(|_| corrupt("ID bank end exceeds address space"))?;
    let prefix = bytes
        .get(..end)
        .ok_or_else(|| corrupt("truncated selected ID bank prefix"))?;
    if crc32c::crc32c(prefix) != reference.running_crc {
        return Err(corrupt("selected ID bank prefix checksum mismatch"));
    }
    Ok(())
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}
fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}
fn corrupt(message: &str) -> LimboError {
    LimboError::Corrupt(message.into())
}

#[cfg(test)]
mod codec {
    use super::*;
    const ID: FileId = FileId { dev: 3, ino: 7 };
    const DATABASE: DatabaseGeneration = DatabaseGeneration {
        generation: 4,
        checkpoint_timestamp: 12,
    };

    fn bank(number: u8, generation: u64, database: DatabaseGeneration) -> (Vec<u8>, BankRef) {
        let bytes = BankHeader {
            database_id: ID,
            bank_generation: generation,
            database,
        }
        .encode()
        .unwrap()
        .to_vec();
        let reference = BankRef {
            bank: number,
            bank_generation: generation,
            database,
            end: PAGE_SIZE as u64,
            snapshot_end: PAGE_SIZE as u64,
            running_crc: crc32c::crc32c(&bytes),
        };
        (bytes, reference)
    }

    #[test]
    fn selector_loads_mirrored_acknowledged_bank() {
        let (bytes, active) = bank(0, 1, DATABASE);
        let selector = IdSelector {
            sequence: 2,
            database_id: ID,
            active,
            prepared: None,
        };
        let slot = selector.encode().unwrap();
        let loaded = select([&slot, &slot], [&bytes, &[]], ID, DATABASE).unwrap();
        assert_eq!(loaded.selector, selector);
        assert_eq!(loaded.selected, active);
        assert!(loaded.slots_agree);
    }

    #[test]
    fn newer_torn_selector_and_partial_inactive_bank_keep_old_witness() {
        let (bytes, active) = bank(0, 1, DATABASE);
        let record = IdSelector {
            sequence: 3,
            database_id: ID,
            active,
            prepared: None,
        };
        let good = record.encode().unwrap();
        let mut newer = record;
        newer.sequence = 4;
        let next = newer.encode().unwrap();
        for cut in 0..PAGE_SIZE {
            let selected = select(
                [&good, &next[..cut]],
                [&bytes, b"partial replacement"],
                ID,
                DATABASE,
            )
            .unwrap();
            assert_eq!(selected.selector, record, "cut={cut}");
            assert!(!selected.slots_agree);
        }
    }

    #[test]
    fn mirrored_ack_survives_one_corrupt_selector_but_not_two() {
        let (bytes, active) = bank(0, 1, DATABASE);
        let record = IdSelector {
            sequence: 3,
            database_id: ID,
            active,
            prepared: None,
        };
        let good = record.encode().unwrap();
        let mut bad = good;
        bad[40] ^= 1;
        for slots in [[&good[..], &bad[..]], [&bad[..], &good[..]]] {
            let selected = select(slots, [&bytes, &[]], ID, DATABASE).unwrap();
            assert_eq!(selected.selector, record);
            assert!(!selected.slots_agree);
        }
        assert!(select([&bad, &bad], [&bytes, &[]], ID, DATABASE).is_err());
    }

    #[test]
    fn selected_data_corruption_cannot_fall_back_to_older_complete_bank() {
        let (old, first) = bank(0, 1, DATABASE);
        let (new, second) = bank(1, 2, DATABASE);
        let low = IdSelector {
            sequence: 1,
            database_id: ID,
            active: first,
            prepared: None,
        }
        .encode()
        .unwrap();
        let high = IdSelector {
            sequence: 2,
            database_id: ID,
            active: second,
            prepared: None,
        }
        .encode()
        .unwrap();
        assert_eq!(
            select([&low, &high], [&old, &new], ID, DATABASE)
                .unwrap()
                .selected,
            second
        );
        let mut corrupt_header = new.clone();
        corrupt_header[0] ^= 1;
        assert!(select([&low, &high], [&old, &corrupt_header], ID, DATABASE).is_err());
        assert!(select([&high, &high], [&old, &corrupt_header], ID, DATABASE).is_err());
        assert!(select([&low, &high], [&old, &new[..100]], ID, DATABASE).is_err());
        let mut extended = new.clone();
        extended.extend_from_slice(&[17; PAGE_SIZE]);
        let reference = BankRef {
            end: extended.len() as u64,
            running_crc: crc32c::crc32c(&extended),
            ..second
        };
        let high = IdSelector {
            sequence: 3,
            database_id: ID,
            active: reference,
            prepared: None,
        }
        .encode()
        .unwrap();
        extended[PAGE_SIZE + 3] ^= 1;
        assert!(select([&low, &high], [&old, &extended], ID, DATABASE).is_err());
    }

    #[test]
    fn prepared_bank_requires_matching_recovered_generation() {
        let target = DatabaseGeneration {
            generation: DATABASE.generation + 1,
            checkpoint_timestamp: 40,
        };
        let (old, active) = bank(0, 1, DATABASE);
        let (new, prepared) = bank(1, 2, target);
        let record = IdSelector {
            sequence: 4,
            database_id: ID,
            active,
            prepared: Some(prepared),
        };
        let slot = record.encode().unwrap();
        assert_eq!(
            select([&slot, &slot], [&old, &new], ID, DATABASE)
                .unwrap()
                .selected,
            active
        );
        assert_eq!(
            select([&slot, &slot], [&old, &new], ID, target)
                .unwrap()
                .selected,
            prepared
        );
        assert!(select(
            [&slot, &slot],
            [&old, &new],
            ID,
            DatabaseGeneration {
                checkpoint_timestamp: 41,
                ..target
            }
        )
        .is_err());
        let final_record = IdSelector {
            sequence: 5,
            active: prepared,
            prepared: None,
            ..record
        };
        let final_slot = final_record.encode().unwrap();
        assert!(select([&final_slot, &final_slot], [&[], &new], ID, DATABASE).is_err());
        assert_eq!(
            select([&final_slot, &final_slot], [&[], &new], ID, target)
                .unwrap()
                .selected,
            prepared
        );
    }

    #[test]
    fn conflicting_same_sequence_and_wrong_identity_fail_closed() {
        let (old, first) = bank(0, 1, DATABASE);
        let (new, second) = bank(1, 2, DATABASE);
        let first = IdSelector {
            sequence: 7,
            database_id: ID,
            active: first,
            prepared: None,
        };
        let second = IdSelector {
            active: second,
            ..first
        };
        assert!(select(
            [&first.encode().unwrap(), &second.encode().unwrap()],
            [&old, &new],
            ID,
            DATABASE
        )
        .is_err());
        let slot = first.encode().unwrap();
        assert!(select(
            [&slot, &slot],
            [&old, &new],
            FileId {
                ino: ID.ino + 1,
                ..ID
            },
            DATABASE
        )
        .is_err());
    }

    #[test]
    fn selector_rejects_invalid_fields_and_crc_valid_unknown_format() {
        let (_, active) = bank(0, 1, DATABASE);
        let record = IdSelector {
            sequence: 1,
            database_id: ID,
            active,
            prepared: None,
        };
        for reference in [
            BankRef { bank: 2, ..active },
            BankRef {
                bank_generation: 0,
                ..active
            },
            BankRef { end: 1, ..active },
            BankRef {
                snapshot_end: u64::MAX,
                ..active
            },
        ] {
            assert!(IdSelector {
                active: reference,
                ..record
            }
            .encode()
            .is_err());
        }
        assert!(IdSelector {
            sequence: 0,
            ..record
        }
        .encode()
        .is_err());
        assert!(IdSelector {
            prepared: Some(active),
            ..record
        }
        .encode()
        .is_err());
        let mut bytes = record.encode().unwrap();
        bytes[8..12].copy_from_slice(&999u32.to_le_bytes());
        let crc = crc32c::crc32c(&bytes[..SELECTOR_CRC]);
        bytes[SELECTOR_CRC..SELECTOR_SIZE].copy_from_slice(&crc.to_le_bytes());
        assert!(IdSelector::decode(&bytes).is_err());
    }

    #[test]
    fn generated_selector_roundtrips_and_single_bit_corruption_is_rejected() {
        let mut seed = 0x42abc987_u64;
        for index in 0..PAGE_SIZE {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let generation = seed.max(1);
            let database = DatabaseGeneration {
                generation,
                checkpoint_timestamp: seed % (1 << 62),
            };
            let (_, active) = bank((index % 2) as u8, generation, database);
            let selector = IdSelector {
                sequence: seed.max(1),
                database_id: FileId {
                    dev: seed,
                    ino: seed.rotate_left(19),
                },
                active,
                prepared: None,
            };
            let bytes = selector.encode().unwrap();
            assert_eq!(IdSelector::decode(&bytes).unwrap(), Some(selector));
            let mut corrupted = bytes;
            corrupted[index % SELECTOR_SIZE] ^= 1 << (index % 8);
            assert_eq!(IdSelector::decode(&corrupted).unwrap(), None);
        }
    }
}
