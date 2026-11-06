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

fn replay_selected(selection: SelectedIds, banks: [&[u8]; 2]) -> Result<journal::State> {
    let reference = selection.selected;
    let bytes = banks[usize::from(reference.bank)];
    validate_bank(bytes, selection.selector.database_id, reference)?;
    let end =
        usize::try_from(reference.end).map_err(|_| corrupt("ID bank end exceeds address space"))?;
    let snapshot_end = usize::try_from(reference.snapshot_end)
        .map_err(|_| corrupt("ID snapshot end exceeds address space"))?;
    journal::State::replay_snapshot(
        &bytes[PAGE_SIZE..end],
        reference.database.generation,
        snapshot_end - PAGE_SIZE,
    )
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

mod journal {
    use super::*;
    use std::collections::HashMap;

    const MAGIC: [u8; 8] = *b"TMVCCIR1";
    const CRC_OFFSET: usize = PAGE_SIZE - 4;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub(super) struct Owner {
        pub epoch: u64,
        pub begin: u64,
    }
    impl Owner {
        fn validate(self) -> Result<()> {
            if self.epoch == 0 || self.begin == 0 || self.begin >= (1 << 62) {
                return Err(corrupt("invalid ID reservation owner"));
            }
            Ok(())
        }
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub(super) struct Domain {
        pub generation: u64,
        pub table: i64,
        pub sequence: bool,
    }
    impl Domain {
        fn validate(self) -> Result<()> {
            if self.generation == 0 || self.table >= 0 || self.table == i64::MIN {
                return Err(corrupt("invalid ID reservation domain"));
            }
            Ok(())
        }
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum ClaimKey {
        Domain(Domain),
        LogicalTable,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) struct Claim {
        pub key: ClaimKey,
        pub value: i64,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum Change {
        Claim {
            owner: Owner,
            ordinal: u64,
            claim: Claim,
        },
        ReleaseAfter {
            owner: Owner,
            next_ordinal: u64,
        },
        ReleaseOwner {
            owner: Owner,
        },
        LogicalFloor(i64),
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) struct Record {
        pub sequence: u64,
        pub change: Change,
    }
    impl Record {
        fn encode(self) -> Result<[u8; PAGE_SIZE]> {
            if self.sequence == 0 {
                return Err(corrupt("invalid ID journal sequence"));
            }
            let mut bytes = [0; PAGE_SIZE];
            bytes[..8].copy_from_slice(&MAGIC);
            bytes[8..12].copy_from_slice(&VERSION.to_le_bytes());
            bytes[12..16].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
            put_u64(&mut bytes, 16, self.sequence);
            let (tag, owner, ordinal, key, value) = match self.change {
                Change::Claim {
                    owner,
                    ordinal,
                    claim,
                } => {
                    owner.validate()?;
                    if ordinal == 0 {
                        return Err(corrupt("invalid ID reservation ordinal"));
                    }
                    match claim.key {
                        ClaimKey::Domain(domain) => domain.validate()?,
                        ClaimKey::LogicalTable => validate_floor(claim.value)?,
                    }
                    (1, Some(owner), ordinal, Some(claim.key), claim.value)
                }
                Change::ReleaseAfter {
                    owner,
                    next_ordinal,
                } => {
                    owner.validate()?;
                    if next_ordinal == 0 {
                        return Err(corrupt("invalid ID savepoint ordinal"));
                    }
                    (2, Some(owner), next_ordinal, None, 0)
                }
                Change::ReleaseOwner { owner } => {
                    owner.validate()?;
                    (3, Some(owner), 0, None, 0)
                }
                Change::LogicalFloor(value) => {
                    validate_floor(value)?;
                    (4, None, 0, None, value)
                }
            };
            put_u64(&mut bytes, 24, tag);
            if let Some(owner) = owner {
                put_u64(&mut bytes, 32, owner.epoch);
                put_u64(&mut bytes, 40, owner.begin);
            }
            put_u64(&mut bytes, 48, ordinal);
            match key {
                Some(ClaimKey::Domain(domain)) => {
                    put_u64(&mut bytes, 56, domain.generation);
                    put_u64(&mut bytes, 64, domain.table as u64);
                    put_u64(&mut bytes, 72, u64::from(domain.sequence));
                }
                Some(ClaimKey::LogicalTable) => put_u64(&mut bytes, 72, 2),
                None => {}
            }
            put_u64(&mut bytes, 80, value as u64);
            let crc = crc32c::crc32c(&bytes[..CRC_OFFSET]);
            bytes[CRC_OFFSET..].copy_from_slice(&crc.to_le_bytes());
            Ok(bytes)
        }

        fn decode(bytes: &[u8]) -> Result<Self> {
            if bytes.len() != PAGE_SIZE {
                return Err(corrupt("invalid selected ID journal record length"));
            }
            let crc = read_u32(bytes, CRC_OFFSET);
            if crc32c::crc32c(&bytes[..CRC_OFFSET]) != crc {
                return Err(corrupt("invalid selected ID journal record checksum"));
            }
            if bytes[..8] != MAGIC || read_u32(bytes, 8) != VERSION {
                return Err(corrupt("incompatible ID journal record format"));
            }
            let owner = Owner {
                epoch: read_u64(bytes, 32),
                begin: read_u64(bytes, 40),
            };
            let ordinal = read_u64(bytes, 48);
            let value = read_u64(bytes, 80) as i64;
            let change = match read_u64(bytes, 24) {
                1 => {
                    let key = match read_u64(bytes, 72) {
                        flag @ (0 | 1) => ClaimKey::Domain(Domain {
                            generation: read_u64(bytes, 56),
                            table: read_u64(bytes, 64) as i64,
                            sequence: flag == 1,
                        }),
                        2 => ClaimKey::LogicalTable,
                        _ => return Err(corrupt("invalid ID claim kind")),
                    };
                    Change::Claim {
                        owner,
                        ordinal,
                        claim: Claim { key, value },
                    }
                }
                2 => Change::ReleaseAfter {
                    owner,
                    next_ordinal: ordinal,
                },
                3 => Change::ReleaseOwner { owner },
                4 => Change::LogicalFloor(value),
                _ => return Err(corrupt("invalid ID journal operation")),
            };
            let record = Self {
                sequence: read_u64(bytes, 16),
                change,
            };
            if record.encode()?.as_slice() != bytes {
                return Err(corrupt("noncanonical ID journal record"));
            }
            Ok(record)
        }
    }

    #[derive(Default)]
    struct OwnerClaims {
        last_ordinal: u64,
        retired: bool,
        claims: HashMap<u64, Claim>,
    }
    pub(super) struct State {
        owners: HashMap<Owner, OwnerClaims>,
        rows: HashMap<(Domain, i64), (Owner, usize)>,
        pub logical_floor: i64,
        pub last_record: u64,
    }
    impl State {
        pub(super) fn replay(bytes: &[u8], database_generation: u64) -> Result<Self> {
            Self::replay_snapshot(bytes, database_generation, 0)
        }
        pub(super) fn replay_snapshot(
            bytes: &[u8],
            database_generation: u64,
            snapshot_bytes: usize,
        ) -> Result<Self> {
            if bytes.len() % PAGE_SIZE != 0
                || database_generation == 0
                || snapshot_bytes % PAGE_SIZE != 0
                || snapshot_bytes > bytes.len()
            {
                return Err(corrupt("invalid ID journal stream"));
            }
            let mut state = Self {
                owners: HashMap::new(),
                rows: HashMap::new(),
                logical_floor: -1,
                last_record: 0,
            };
            for (index, page) in bytes.chunks_exact(PAGE_SIZE).enumerate() {
                let record = Record::decode(page)?;
                if index < snapshot_bytes / PAGE_SIZE
                    && !matches!(record.change, Change::LogicalFloor(_))
                {
                    return Err(corrupt("compacted ID snapshot contains live owner claims"));
                }
                if state.last_record.checked_add(1) != Some(record.sequence) {
                    return Err(corrupt("noncontiguous ID journal sequence"));
                }
                state.apply(record.change, database_generation)?;
                state.last_record = record.sequence;
            }
            Ok(state)
        }

        // Replay owns this temporary state and discards it on any error.
        // Live mutation must stage changes before publishing a durable selector.
        fn apply(&mut self, change: Change, generation: u64) -> Result<()> {
            match change {
                Change::Claim {
                    owner,
                    ordinal,
                    claim,
                } => {
                    self.owners.try_reserve(1)?;
                    let claims = self.owners.entry(owner).or_default();
                    if claims.retired || claims.last_ordinal.checked_add(1) != Some(ordinal) {
                        return Err(corrupt("reused or noncontiguous ID owner ordinal"));
                    }
                    claims.claims.try_reserve(1)?;
                    match claim.key {
                        ClaimKey::LogicalTable => {
                            if claim.value >= self.logical_floor {
                                return Err(corrupt("logical ID floor cannot rise or reuse an ID"));
                            }
                            self.logical_floor = claim.value;
                        }
                        ClaimKey::Domain(domain) => {
                            if domain.generation != generation {
                                return Err(corrupt(
                                    "ID claim belongs to another database generation",
                                ));
                            }
                            if !domain.sequence {
                                self.rows.try_reserve(1)?;
                                let entry =
                                    self.rows.entry((domain, claim.value)).or_insert((owner, 0));
                                if entry.0 != owner {
                                    return Err(corrupt(
                                        "overlapping row ID owners in selected journal",
                                    ));
                                }
                                entry.1 = entry
                                    .1
                                    .checked_add(1)
                                    .ok_or_else(|| corrupt("ID claim count overflow"))?;
                            }
                        }
                    }
                    claims.claims.insert(ordinal, claim);
                    claims.last_ordinal = ordinal;
                }
                Change::ReleaseAfter {
                    owner,
                    next_ordinal,
                } => {
                    let claims = self
                        .owners
                        .get_mut(&owner)
                        .ok_or_else(|| corrupt("ID savepoint owner is unknown"))?;
                    if claims.retired || next_ordinal.saturating_sub(1) > claims.last_ordinal {
                        return Err(corrupt("invalid ID savepoint release"));
                    }
                    claims.claims.retain(|ordinal, claim| {
                        if *ordinal < next_ordinal {
                            return true;
                        }
                        release_row(&mut self.rows, owner, *claim);
                        false
                    });
                }
                Change::ReleaseOwner { owner } => {
                    self.owners.try_reserve(1)?;
                    let claims = self.owners.entry(owner).or_default();
                    for (_, claim) in claims.claims.drain() {
                        release_row(&mut self.rows, owner, claim);
                    }
                    claims.retired = true;
                }
                Change::LogicalFloor(floor) => {
                    if floor > self.logical_floor {
                        return Err(corrupt("logical ID floor cannot rise"));
                    }
                    self.logical_floor = floor;
                }
            }
            Ok(())
        }

        pub(super) fn claims(&self, owner: Owner) -> impl Iterator<Item = (u64, Claim)> + '_ {
            self.owners.get(&owner).into_iter().flat_map(|owner| {
                owner
                    .claims
                    .iter()
                    .map(|(ordinal, claim)| (*ordinal, *claim))
            })
        }
        pub(super) fn sequence_claim_watermark(&self, domain: Domain) -> Option<i64> {
            self.owners
                .values()
                .flat_map(|owner| owner.claims.values())
                .filter_map(|claim| (claim.key == ClaimKey::Domain(domain)).then_some(claim.value))
                .min()
        }
    }

    fn release_row(rows: &mut HashMap<(Domain, i64), (Owner, usize)>, owner: Owner, claim: Claim) {
        if let ClaimKey::Domain(domain) = claim.key {
            if !domain.sequence {
                let key = (domain, claim.value);
                let entry = rows
                    .get_mut(&key)
                    .expect("validated row claim remains indexed");
                assert_eq!(entry.0, owner);
                entry.1 -= 1;
                if entry.1 == 0 {
                    rows.remove(&key);
                }
            }
        }
    }
    fn validate_floor(value: i64) -> Result<()> {
        if value >= 0 || value == i64::MIN {
            return Err(corrupt("invalid logical ID floor"));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        const A: Owner = Owner { epoch: 1, begin: 2 };
        const B: Owner = Owner { epoch: 1, begin: 3 };
        const DOMAIN: Domain = Domain {
            generation: 1,
            table: -2,
            sequence: false,
        };
        fn claim(owner: Owner, ordinal: u64, domain: Domain, value: i64) -> Change {
            Change::Claim {
                owner,
                ordinal,
                claim: Claim {
                    key: ClaimKey::Domain(domain),
                    value,
                },
            }
        }
        fn load(changes: &[Change]) -> Result<State> {
            let mut bytes = Vec::new();
            for (i, change) in changes.iter().enumerate() {
                bytes.extend_from_slice(
                    &Record {
                        sequence: i as u64 + 1,
                        change: *change,
                    }
                    .encode()?,
                );
            }
            State::replay(&bytes, 1)
        }
        #[test]
        fn release_after_keeps_ordinals_and_never_raises_logical_floor() {
            let changes = [
                claim(A, 1, DOMAIN, 1),
                Change::Claim {
                    owner: A,
                    ordinal: 2,
                    claim: Claim {
                        key: ClaimKey::LogicalTable,
                        value: -100,
                    },
                },
                Change::ReleaseAfter {
                    owner: A,
                    next_ordinal: 2,
                },
                claim(A, 3, DOMAIN, 2),
            ];
            let state = load(&changes).unwrap();
            assert_eq!(state.logical_floor, -100);
            assert_eq!(state.claims(A).count(), 2);
            let mut changes = changes.to_vec();
            changes.push(Change::ReleaseOwner { owner: A });
            let state = load(&changes).unwrap();
            assert_eq!(state.logical_floor, -100);
            assert_eq!(state.claims(A).count(), 0);
            changes.push(claim(A, 4, DOMAIN, 3));
            assert!(load(&changes).is_err());
        }
        #[test]
        fn distinct_owners_cannot_share_live_row_claim_but_release_allows_reuse() {
            assert!(load(&[claim(A, 1, DOMAIN, 7), claim(B, 1, DOMAIN, 7)]).is_err());
            let state = load(&[
                claim(A, 1, DOMAIN, 7),
                Change::ReleaseOwner { owner: A },
                claim(B, 1, DOMAIN, 7),
            ])
            .unwrap();
            assert_eq!(state.claims(B).count(), 1);
            assert!(load(&[
                claim(A, 1, DOMAIN, 7),
                Change::ReleaseAfter {
                    owner: A,
                    next_ordinal: 1
                },
                claim(A, 1, DOMAIN, 8)
            ])
            .is_err());
        }
        #[test]
        fn sequence_claim_release_changes_watermark_without_rewinding_cursor() {
            let domain = Domain {
                sequence: true,
                ..DOMAIN
            };
            let mut changes = vec![claim(A, 1, domain, 5), claim(B, 1, domain, 9)];
            assert_eq!(
                load(&changes).unwrap().sequence_claim_watermark(domain),
                Some(5)
            );
            changes.push(Change::ReleaseOwner { owner: A });
            assert_eq!(
                load(&changes).unwrap().sequence_claim_watermark(domain),
                Some(9)
            );
            changes.push(Change::ReleaseOwner { owner: B });
            assert_eq!(
                load(&changes).unwrap().sequence_claim_watermark(domain),
                None
            );
        }
        #[test]
        fn dynamic_domains_have_no_fixed_table_capacity() {
            let changes: Vec<_> = (1..=2048)
                .map(|ordinal| {
                    claim(
                        A,
                        ordinal,
                        Domain {
                            table: -(ordinal as i64) - 1,
                            ..DOMAIN
                        },
                        ordinal as i64,
                    )
                })
                .collect();
            assert_eq!(load(&changes).unwrap().claims(A).count(), 2048);
        }
        #[test]
        fn selected_record_corruption_wrong_generation_and_ordinal_fail_closed() {
            let record = Record {
                sequence: 1,
                change: claim(A, 1, DOMAIN, 10),
            };
            let bytes = record.encode().unwrap();
            for cut in 0..PAGE_SIZE {
                assert!(Record::decode(&bytes[..cut]).is_err());
            }
            for offset in 0..PAGE_SIZE {
                let mut bad = bytes;
                bad[offset] ^= 1 << (offset % 8);
                assert!(Record::decode(&bad).is_err());
            }
            assert!(State::replay(&bytes, 2).is_err());
            assert!(load(&[claim(A, 2, DOMAIN, 7)]).is_err());
            assert!(load(&[Change::LogicalFloor(-100), Change::LogicalFloor(-99)]).is_err());
            assert!(Record {
                sequence: 1,
                change: Change::LogicalFloor(i64::MIN)
            }
            .encode()
            .is_err());
        }
        #[test]
        fn selected_stream_replay_excludes_unacknowledged_tail_and_checks_snapshot() {
            let database = DatabaseGeneration {
                generation: 1,
                checkpoint_timestamp: 12,
            };
            let id = FileId { dev: 3, ino: 7 };
            let mut bytes = BankHeader {
                database_id: id,
                bank_generation: 1,
                database,
            }
            .encode()
            .unwrap()
            .to_vec();
            bytes.extend_from_slice(
                &Record {
                    sequence: 1,
                    change: Change::LogicalFloor(-100),
                }
                .encode()
                .unwrap(),
            );
            bytes.extend_from_slice(
                &Record {
                    sequence: 2,
                    change: claim(A, 1, DOMAIN, 9),
                }
                .encode()
                .unwrap(),
            );
            let reference = BankRef {
                bank: 0,
                bank_generation: 1,
                database,
                end: bytes.len() as u64,
                snapshot_end: (2 * PAGE_SIZE) as u64,
                running_crc: crc32c::crc32c(&bytes),
            };
            let selector = IdSelector {
                sequence: 1,
                database_id: id,
                active: reference,
                prepared: None,
            };
            let slot = selector.encode().unwrap();
            bytes.extend_from_slice(b"unacknowledged partial tail");
            let selected = select([&slot, &slot], [&bytes, &[]], id, database).unwrap();
            let state = replay_selected(selected, [&bytes, &[]]).unwrap();
            assert_eq!(state.logical_floor, -100);
            assert_eq!(state.last_record, 2);
            assert_eq!(state.claims(A).count(), 1);
            let invalid = IdSelector {
                active: BankRef {
                    snapshot_end: reference.end,
                    ..reference
                },
                ..selector
            }
            .encode()
            .unwrap();
            let selected = select([&invalid, &invalid], [&bytes, &[]], id, database).unwrap();
            assert!(replay_selected(selected, [&bytes, &[]]).is_err());
        }
        #[test]
        fn record_roundtrips_cover_signed_values_domains_and_release_forms() {
            for value in [i64::MIN, -99, -1, 0, 1, i64::MAX] {
                for sequence in [false, true] {
                    let record = Record {
                        sequence: u64::MAX,
                        change: claim(A, u64::MAX, Domain { sequence, ..DOMAIN }, value),
                    };
                    assert_eq!(Record::decode(&record.encode().unwrap()).unwrap(), record);
                }
            }
            for change in [
                Change::LogicalFloor(i64::MIN + 1),
                Change::ReleaseOwner { owner: B },
                Change::ReleaseAfter {
                    owner: A,
                    next_ordinal: u64::MAX,
                },
            ] {
                let record = Record {
                    sequence: 17,
                    change,
                };
                assert_eq!(Record::decode(&record.encode().unwrap()).unwrap(), record);
            }
        }
        #[test]
        fn repeated_owner_row_claim_survives_partial_savepoint_release() {
            let mut changes = vec![
                claim(A, 1, DOMAIN, 7),
                claim(A, 2, DOMAIN, 7),
                Change::ReleaseAfter {
                    owner: A,
                    next_ordinal: 2,
                },
            ];
            let mut conflicting = changes.clone();
            conflicting.push(claim(B, 1, DOMAIN, 7));
            assert!(load(&conflicting).is_err());
            changes.push(Change::ReleaseOwner { owner: A });
            changes.push(claim(B, 1, DOMAIN, 7));
            assert_eq!(load(&changes).unwrap().claims(B).count(), 1);
        }
    }
}
