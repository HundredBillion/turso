use crate::io::{File, FileId, SharedWalLockKind, SharedWalMappedRegion};
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::{Arc, Mutex, Weak};
use crate::{Completion, LimboError, Result, IO};
use std::sync::atomic::AtomicU64 as MappedAtomic;

const MAP_LEN: usize = 4096;
const MAGIC: u64 = u64::from_le_bytes(*b"TMVCC001");
const VERSION: u64 = 3;
const SLOT_COUNT: usize = 64;
const SLOT_START: usize = 128;
const SLOT_WIDTH: usize = 32;
pub(super) const MAX_TIMESTAMP: u64 = (1 << 62) - 1;
const LIFETIME_LOCK: u64 = 0;
const PUBLICATION_LOCK: u64 = 1;
const SLOT_LOCK_START: u64 = 2;
const LOCK_KIND: SharedWalLockKind = SharedWalLockKind::LinuxOfd;

const MAGIC_OFFSET: usize = 0;
const VERSION_OFFSET: usize = 8;
const DEVICE_OFFSET: usize = 16;
const INODE_OFFSET: usize = 24;
const EPOCH_OFFSET: usize = 32;
const NEXT_TIMESTAMP_OFFSET: usize = 40;
const COMMITTED_TIMESTAMP_OFFSET: usize = 48;
const LOG_OFFSET: usize = 56;
const LOG_CRC_OFFSET: usize = 64;
const RECOVERY_OFFSET: usize = 72;
const HISTORY_LIMIT_OFFSET: usize = 80;
const SLOT_COUNT_OFFSET: usize = 88;
const INCARNATION_READY_OFFSET: usize = 96;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PublishedPrefix {
    pub committed_timestamp: u64,
    pub end_offset: u64,
    pub running_crc: u32,
}

pub(super) struct SharedMvccAuthority {
    file: Arc<dyn File>,
    mapping: Box<dyn SharedWalMappedRegion>,
    publication_owned: AtomicBool,
    poisoned: AtomicBool,
    local_slots: Mutex<[bool; SLOT_COUNT]>,
}

pub(super) struct AuthorityGuard {
    authority: Arc<SharedMvccAuthority>,
    recovery_required: bool,
    lease: Arc<PublicationLease>,
}

struct LeaseState {
    active: bool,
    owned: bool,
    pins: usize,
    retirement: Option<Completion>,
}

pub(super) struct PublicationLease {
    authority: Arc<SharedMvccAuthority>,
    state: Mutex<LeaseState>,
}

pub(super) struct IoPin {
    lease: Option<Arc<PublicationLease>>,
}

impl PublicationLease {
    pub(super) fn pin(lease: &Weak<Self>) -> Result<IoPin> {
        let lease = lease.upgrade().ok_or(LimboError::Busy)?;
        {
            let mut state = lease.state.lock();
            if !state.active || !state.owned {
                return Err(LimboError::Busy);
            }
            state.pins = state.pins.checked_add(1).ok_or(LimboError::TooBig)?;
        }
        Ok(IoPin { lease: Some(lease) })
    }

    fn release(&self, state: &mut LeaseState) -> Result<()> {
        state.owned = false;
        if let Err(error) = self
            .authority
            .file
            .shared_wal_unlock_byte(PUBLICATION_LOCK, LOCK_KIND)
        {
            self.authority.store(RECOVERY_OFFSET, 1);
            self.authority.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        self.authority
            .publication_owned
            .store(false, Ordering::Release);
        Ok(())
    }
}

impl Drop for IoPin {
    fn drop(&mut self) {
        let lease = self.lease.take().expect("I/O pin owns its lease");
        let retirement = {
            let mut state = lease.state.lock();
            state.pins -= 1;
            if state.pins == 0 {
                state.retirement.take()
            } else {
                None
            }
        };
        // Lease destruction also locks state and may release OS ownership.
        // Notify only after releasing both the state mutex and this pin's Arc.
        drop(lease);
        if let Some(retirement) = retirement {
            retirement.complete(0);
        }
    }
}

impl Drop for PublicationLease {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        if state.owned {
            self.authority.store(RECOVERY_OFFSET, 1);
            if let Err(error) = self.release(&mut state) {
                tracing::error!(%error, "failed releasing shared MVCC publication ownership");
            }
        }
    }
}

pub(super) struct SnapshotRegistration {
    pub begin_timestamp: u64,
    pub epoch: u64,
    pub prefix: PublishedPrefix,
    authority: Arc<SharedMvccAuthority>,
    slot: usize,
}

impl std::fmt::Debug for SnapshotRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotRegistration")
            .field("begin_timestamp", &self.begin_timestamp)
            .field("epoch", &self.epoch)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl SharedMvccAuthority {
    pub(super) fn file_id(&self) -> Result<Option<FileId>> {
        self.file.file_id()
    }

    pub(super) fn open(
        io: &Arc<dyn IO>,
        path: &str,
        database_id: FileId,
        history_limit: u64,
    ) -> Result<Arc<Self>> {
        Self::open_with_creation(io, path, database_id, history_limit, true)
    }

    /// Reopen published coordination without recreating an unlinked live mapping.
    pub(super) fn open_existing(
        io: &Arc<dyn IO>,
        path: &str,
        database_id: FileId,
        history_limit: u64,
    ) -> Result<Arc<Self>> {
        Self::open_with_creation(io, path, database_id, history_limit, false)
    }

    fn open_with_creation(
        io: &Arc<dyn IO>,
        path: &str,
        database_id: FileId,
        history_limit: u64,
        allow_creation: bool,
    ) -> Result<Arc<Self>> {
        if !cfg!(all(target_os = "linux", target_pointer_width = "64"))
            || !io.supports_shared_wal_coordination()
        {
            return Err(LimboError::InvalidArgument(
                "shared MVCC authority requires a supported 64-bit Linux filesystem backend".into(),
            ));
        }
        if history_limit == 0 {
            return Err(LimboError::InvalidArgument(
                "shared MVCC history limit must be positive".into(),
            ));
        }
        let file = if allow_creation {
            io.open_shared_wal_file(path)?
        } else {
            io.open_file(path, crate::OpenFlags::NoLock, false)?
        };
        let exclusive = file.shared_wal_try_lock_byte(LIFETIME_LOCK, true, LOCK_KIND)?;
        if !exclusive && !file.shared_wal_try_lock_byte(LIFETIME_LOCK, false, LOCK_KIND)? {
            return Err(LimboError::Busy);
        }
        let result = (|| {
            match file.size()? {
                0 if exclusive && allow_creation => file.shared_wal_set_len(MAP_LEN as u64)?,
                size if size == MAP_LEN as u64 => {}
                _ => {
                    return Err(LimboError::Corrupt(
                        "invalid shared MVCC coordination file length".into(),
                    ));
                }
            }
            let mapping = file.shared_wal_map(0, MAP_LEN)?;
            if mapping.len() < MAP_LEN || mapping.ptr().as_ptr() as usize % 8 != 0 {
                return Err(LimboError::InvalidArgument(
                    "shared MVCC mapping must cover its aligned header".into(),
                ));
            }
            let authority = Arc::new(Self {
                file: file.clone(),
                mapping,
                publication_owned: AtomicBool::new(false),
                poisoned: AtomicBool::new(false),
                local_slots: Mutex::new([false; SLOT_COUNT]),
            });
            if authority.load(MAGIC_OFFSET) == 0 && exclusive && allow_creation {
                authority.initialize(database_id, history_limit);
            }
            authority.validate_header(database_id, history_limit)?;
            if exclusive {
                authority.store(INCARNATION_READY_OFFSET, 0);
                let epoch = authority
                    .load(EPOCH_OFFSET)
                    .checked_add(1)
                    .ok_or(LimboError::DatabaseFull)?;
                authority.store(EPOCH_OFFSET, epoch);
                authority.store(RECOVERY_OFFSET, 1);
                for slot in 0..SLOT_COUNT {
                    authority.store(slot_offset(slot), 0);
                }
                file.shared_wal_lock_byte(LIFETIME_LOCK, false, LOCK_KIND)?;
            }
            Ok(authority)
        })();
        if result.is_err() {
            file.shared_wal_unlock_byte(LIFETIME_LOCK, LOCK_KIND)?;
        }
        result
    }

    pub(super) fn try_acquire(self: &Arc<Self>) -> Result<AuthorityGuard> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(LimboError::InvalidArgument(
                "shared MVCC authority lost lock ownership; close all handles".into(),
            ));
        }
        if self
            .publication_owned
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(LimboError::Busy);
        }
        match self
            .file
            .shared_wal_try_lock_byte(PUBLICATION_LOCK, true, LOCK_KIND)
        {
            Ok(true) => {}
            result => {
                self.publication_owned.store(false, Ordering::Release);
                return match result {
                    Ok(false) => Err(LimboError::Busy),
                    Err(error) => Err(error),
                    Ok(true) => unreachable!(),
                };
            }
        }
        let recovery_required = self.load(RECOVERY_OFFSET) != 0;
        self.store(RECOVERY_OFFSET, 1);
        Ok(AuthorityGuard {
            authority: self.clone(),
            recovery_required,
            lease: Arc::new(PublicationLease {
                authority: self.clone(),
                state: Mutex::new(LeaseState {
                    active: true,
                    owned: true,
                    pins: 0,
                    retirement: None,
                }),
            }),
        })
    }

    #[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
    pub(in crate::mvcc) fn recovery_state_for_test(&self) -> Result<(u64, bool, u64)> {
        let ready = match self.load(INCARNATION_READY_OFFSET) {
            0 => false,
            1 => true,
            _ => {
                return Err(LimboError::Corrupt(
                    "invalid shared incarnation readiness".into(),
                ))
            }
        };
        Ok((
            self.load(EPOCH_OFFSET),
            ready,
            self.load(NEXT_TIMESTAMP_OFFSET),
        ))
    }

    fn initialize(&self, database_id: FileId, history_limit: u64) {
        for offset in (8..MAP_LEN).step_by(8) {
            self.store(offset, 0);
        }
        self.store(VERSION_OFFSET, VERSION);
        self.store(DEVICE_OFFSET, database_id.dev);
        self.store(INODE_OFFSET, database_id.ino);
        self.store(NEXT_TIMESTAMP_OFFSET, 1);
        self.store(HISTORY_LIMIT_OFFSET, history_limit);
        self.store(SLOT_COUNT_OFFSET, SLOT_COUNT as u64);
        self.store(RECOVERY_OFFSET, 1);
        self.store(MAGIC_OFFSET, MAGIC);
    }

    fn validate_header(&self, database_id: FileId, history_limit: u64) -> Result<()> {
        if self.load(MAGIC_OFFSET) != MAGIC || self.load(VERSION_OFFSET) != VERSION {
            return Err(LimboError::InvalidArgument(
                "incompatible shared MVCC coordination protocol".into(),
            ));
        }
        if self.load(DEVICE_OFFSET) != database_id.dev || self.load(INODE_OFFSET) != database_id.ino
        {
            return Err(LimboError::InvalidArgument(
                "shared MVCC coordination belongs to a different database".into(),
            ));
        }
        if self.load(SLOT_COUNT_OFFSET) != SLOT_COUNT as u64
            || self.load(HISTORY_LIMIT_OFFSET) != history_limit
        {
            return Err(LimboError::InvalidArgument(
                "incompatible shared MVCC coordination options".into(),
            ));
        }
        Ok(())
    }

    fn load(&self, offset: usize) -> u64 {
        u64::from_le(
            self.field(offset)
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn store(&self, offset: usize, value: u64) {
        self.field(offset)
            .store(value.to_le(), std::sync::atomic::Ordering::Release);
    }

    fn field(&self, offset: usize) -> &MappedAtomic {
        assert!(offset % 8 == 0 && offset + 8 <= MAP_LEN);
        // The mapping owns aligned bytes for the lifetime of every borrowed atomic.
        unsafe { MappedAtomic::from_ptr(self.mapping.ptr().as_ptr().add(offset).cast()) }
    }
}

impl AuthorityGuard {
    /// Observe backend retirement without consuming the guard; concurrent submissions may race it.
    pub(super) fn io_retired(&self) -> bool {
        let state = self.lease.state.lock();
        state.active && state.owned && state.pins == 0
    }

    /// Wait for admitted backend pins, including completion objects whose callback ran.
    /// A cancelled waiter is replaced so a later caller still drives I/O progress.
    pub(super) fn retirement_completion(&self) -> Completion {
        let mut state = self.lease.state.lock();
        if state.pins == 0 {
            return Completion::new_yield();
        }
        if state.retirement.as_ref().is_none_or(Completion::finished) {
            state.retirement = Some(Completion::new_wait());
        }
        state.retirement.as_ref().unwrap().clone()
    }

    pub(super) fn belongs_to(&self, authority: &Weak<SharedMvccAuthority>) -> bool {
        Weak::ptr_eq(authority, &Arc::downgrade(&self.authority))
    }

    pub(super) fn io_lease(&self) -> Weak<PublicationLease> {
        Arc::downgrade(&self.lease)
    }

    pub(super) fn requires_recovery(&self) -> bool {
        self.recovery_required
    }

    pub(super) fn recover(&mut self, prefix: PublishedPrefix) -> Result<()> {
        if !self.recovery_required {
            return Err(LimboError::InvalidArgument(
                "shared MVCC authority does not require recovery".into(),
            ));
        }
        self.store_prefix(prefix)?;
        self.recovery_required = false;
        Ok(())
    }

    pub(super) fn publish_committed(
        &mut self,
        durable: &super::commit_manifest::DurableManifest,
    ) -> Result<()> {
        self.advance_committed(durable.prefix())
    }

    pub(super) fn publish_checkpoint(
        &mut self,
        proof: &super::commit_manifest::checkpoint_publication::CompletedCheckpointProof,
    ) -> Result<()> {
        let target = proof.target_identity();
        let previous = self.published()?;
        if !proof.matches_lease(self)
            || target.prefix.end_offset != 0
            || target.prefix.running_crc != 0
            || target.prefix.committed_timestamp != previous.committed_timestamp
            || proof
                .source_identity()
                .is_some_and(|source| source.prefix != previous)
            || (proof.source_identity().is_none() && previous != target.prefix)
        {
            return Err(LimboError::InvalidArgument(
                "checkpoint authority proof differs".into(),
            ));
        }
        if !self.live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        self.store_prefix(target.prefix)
    }

    fn advance_committed(&mut self, prefix: PublishedPrefix) -> Result<()> {
        let previous = self.published()?;
        if prefix.committed_timestamp <= previous.committed_timestamp
            || prefix.end_offset <= previous.end_offset
        {
            return Err(LimboError::InvalidArgument(
                "shared MVCC commit must advance timestamp and log boundary".into(),
            ));
        }
        self.store_prefix(prefix)
    }

    fn store_prefix(&mut self, prefix: PublishedPrefix) -> Result<()> {
        if prefix.committed_timestamp >= MAX_TIMESTAMP
            || (prefix.end_offset == 0 && prefix.running_crc != 0)
            || (prefix.end_offset != 0
                && prefix.end_offset < super::persistent_storage::logical_log::LOG_HDR_SIZE as u64)
        {
            return Err(LimboError::Corrupt("invalid recovered MVCC prefix".into()));
        }
        let next = self
            .authority
            .load(NEXT_TIMESTAMP_OFFSET)
            .max(prefix.committed_timestamp + 1);
        self.authority.store(NEXT_TIMESTAMP_OFFSET, next);
        self.authority
            .store(COMMITTED_TIMESTAMP_OFFSET, prefix.committed_timestamp);
        self.authority.store(LOG_OFFSET, prefix.end_offset);
        self.authority
            .store(LOG_CRC_OFFSET, u64::from(prefix.running_crc));
        Ok(())
    }

    pub(super) fn published(&self) -> Result<PublishedPrefix> {
        self.require_recovered()?;
        let crc = u32::try_from(self.authority.load(LOG_CRC_OFFSET))
            .map_err(|_| LimboError::Corrupt("invalid shared MVCC checksum".into()))?;
        Ok(PublishedPrefix {
            committed_timestamp: self.authority.load(COMMITTED_TIMESTAMP_OFFSET),
            end_offset: self.authority.load(LOG_OFFSET),
            running_crc: crc,
        })
    }

    pub(super) fn incarnation_ready(&self) -> Result<bool> {
        self.require_recovered()?;
        match self.authority.load(INCARNATION_READY_OFFSET) {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(LimboError::Corrupt(
                "invalid shared incarnation readiness".into(),
            )),
        }
    }

    /// The caller first mirrors the selected durable ID witness and waits for I/O retirement.
    /// Its maximum includes retired owners and every bank retained by that witness.
    pub(super) fn reconcile_incarnation(&mut self, durable_owner_epoch: u64) -> Result<()> {
        if self.incarnation_ready()? || !self.live_registrations()?.is_empty() {
            return Err(LimboError::InvalidArgument(
                "shared incarnation reconciliation requires fresh quiescent authority".into(),
            ));
        }
        let next = durable_owner_epoch
            .checked_add(1)
            .ok_or(LimboError::DatabaseFull)?;
        let epoch = self.authority.load(EPOCH_OFFSET).max(next);
        self.authority.store(EPOCH_OFFSET, epoch);
        self.authority.store(INCARNATION_READY_OFFSET, 1);
        Ok(())
    }

    pub(super) fn register_snapshot(&mut self) -> Result<SnapshotRegistration> {
        if !self.incarnation_ready()? {
            return Err(LimboError::InvalidArgument(
                "shared incarnation requires durable ID reconciliation before admission".into(),
            ));
        }
        let prefix = self.published()?;
        let begin_timestamp = self.allocate_timestamp()?;
        let epoch = self.authority.load(EPOCH_OFFSET);
        let mut slots = self.authority.local_slots.lock();
        for (slot, owned) in slots.iter_mut().enumerate() {
            if *owned
                || !self.authority.file.shared_wal_try_lock_byte(
                    SLOT_LOCK_START + slot as u64,
                    true,
                    LOCK_KIND,
                )?
            {
                continue;
            }
            *owned = true;
            self.authority.store(slot_offset(slot) + 8, epoch);
            self.authority
                .store(slot_offset(slot) + 16, prefix.end_offset);
            self.authority.store(slot_offset(slot), begin_timestamp);
            return Ok(SnapshotRegistration {
                begin_timestamp,
                epoch,
                prefix,
                authority: self.authority.clone(),
                slot,
            });
        }
        Err(LimboError::Busy)
    }

    /// Return incarnation and begin timestamp while publication ownership excludes new admissions.
    pub(super) fn live_registrations(&self) -> Result<Vec<(u64, u64)>> {
        let mut live = Vec::new();
        live.try_reserve_exact(SLOT_COUNT)?;
        self.visit_live_registrations(|epoch, timestamp| live.push((epoch, timestamp)))?;
        Ok(live)
    }

    pub(super) fn oldest_snapshot(&self) -> Result<Option<u64>> {
        let mut oldest = None;
        self.visit_live_registrations(|_, timestamp| {
            oldest = Some(oldest.map_or(timestamp, |old: u64| old.min(timestamp)));
        })?;
        Ok(oldest)
    }

    /// Recovery checks native ownership before mapped snapshot timestamps can be trusted.
    pub(super) fn ensure_no_snapshot_locks(&self) -> Result<()> {
        if self.authority.poisoned.load(Ordering::Acquire) {
            return Err(LimboError::InvalidArgument(
                "shared MVCC authority lost lock ownership".into(),
            ));
        }
        let slots = self.authority.local_slots.lock();
        for (slot, locally_owned) in slots.iter().enumerate() {
            if *locally_owned
                || !self.authority.file.shared_wal_try_lock_byte(
                    SLOT_LOCK_START + slot as u64,
                    true,
                    LOCK_KIND,
                )?
            {
                return Err(LimboError::Busy);
            }
            if let Err(error) = self
                .authority
                .file
                .shared_wal_unlock_byte(SLOT_LOCK_START + slot as u64, LOCK_KIND)
            {
                self.authority.poisoned.store(true, Ordering::Release);
                return Err(error);
            }
        }
        Ok(())
    }

    fn visit_live_registrations(&self, mut visit: impl FnMut(u64, u64)) -> Result<()> {
        self.require_recovered()?;
        let slots = self.authority.local_slots.lock();
        for (slot, locally_owned) in slots.iter().enumerate() {
            if !locally_owned
                && self.authority.file.shared_wal_try_lock_byte(
                    SLOT_LOCK_START + slot as u64,
                    true,
                    LOCK_KIND,
                )?
            {
                self.authority.store(slot_offset(slot), 0);
                if let Err(error) = self
                    .authority
                    .file
                    .shared_wal_unlock_byte(SLOT_LOCK_START + slot as u64, LOCK_KIND)
                {
                    self.authority.poisoned.store(true, Ordering::Release);
                    return Err(error);
                }
                continue;
            }
            let timestamp = self.authority.load(slot_offset(slot));
            if timestamp != 0 {
                let epoch = self.authority.load(slot_offset(slot) + 8);
                if timestamp >= MAX_TIMESTAMP
                    || epoch == 0
                    || epoch != self.authority.load(EPOCH_OFFSET)
                {
                    return Err(LimboError::Corrupt(
                        "invalid live shared MVCC registration".into(),
                    ));
                }
                visit(epoch, timestamp);
            }
        }
        Ok(())
    }

    pub(super) fn allocate_timestamp(&mut self) -> Result<u64> {
        self.require_recovered()?;
        let next = self.authority.load(NEXT_TIMESTAMP_OFFSET);
        if next == 0 || next >= MAX_TIMESTAMP {
            return Err(LimboError::DatabaseFull);
        }
        self.authority.store(NEXT_TIMESTAMP_OFFSET, next + 1);
        Ok(next)
    }

    pub(super) fn check_append_size(&self, bytes: u64) -> Result<()> {
        let prefix = self.published()?;
        let limit = self.authority.load(HISTORY_LIMIT_OFFSET);
        if bytes > limit {
            return Err(LimboError::TooBig);
        }
        if prefix
            .end_offset
            .checked_add(bytes)
            .is_none_or(|end| end > limit)
        {
            return Err(LimboError::Busy);
        }
        Ok(())
    }

    pub(super) fn check_append_size_with_metadata(
        &self,
        bytes: u64,
        retained_metadata: u64,
        minimum_metadata: u64,
    ) -> Result<()> {
        let prefix = self.published()?;
        check_combined_history(
            prefix.end_offset,
            bytes,
            retained_metadata,
            minimum_metadata,
            self.authority.load(HISTORY_LIMIT_OFFSET),
        )
    }

    pub(super) fn finish(self) -> Result<()> {
        self.require_recovered()?;
        let mut state = self.lease.state.lock();
        // Closing admission also rejects weak upgrades that race the final pin check.
        state.active = false;
        if state.pins != 0 {
            return Err(LimboError::Busy);
        }
        self.authority.store(RECOVERY_OFFSET, 0);
        self.lease.release(&mut state)
    }

    fn require_recovered(&self) -> Result<()> {
        if self.recovery_required || self.authority.poisoned.load(Ordering::Acquire) {
            return Err(LimboError::InvalidArgument(
                "shared MVCC authority must recover before admitting transactions".into(),
            ));
        }
        Ok(())
    }
}

impl Drop for AuthorityGuard {
    fn drop(&mut self) {
        let mut state = self.lease.state.lock();
        state.active = false;
        if state.owned {
            self.authority.store(RECOVERY_OFFSET, 1);
        }
    }
}

impl Drop for SnapshotRegistration {
    fn drop(&mut self) {
        let mut slots = self.authority.local_slots.lock();
        assert!(slots[self.slot]);
        self.authority.store(slot_offset(self.slot), 0);
        match self
            .authority
            .file
            .shared_wal_unlock_byte(SLOT_LOCK_START + self.slot as u64, LOCK_KIND)
        {
            Ok(()) => slots[self.slot] = false,
            Err(error) => {
                self.authority.poisoned.store(true, Ordering::Release);
                tracing::error!(%error, "failed releasing shared MVCC snapshot ownership");
            }
        }
    }
}

fn slot_offset(slot: usize) -> usize {
    assert!(slot < SLOT_COUNT);
    SLOT_START + slot * SLOT_WIDTH
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    const CHILD_TEST: &str = "mvcc::shared_authority::tests::authority_child";
    const DATABASE_ID: FileId = FileId { dev: 1, ino: 2 };

    #[test]
    fn authority_incarnation_does_not_reuse_durable_owner_after_mapped_state_loss() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        let mut guard = authority.try_acquire().unwrap();
        let snapshot = guard.register_snapshot().unwrap();
        let acknowledged_owner = (snapshot.epoch, snapshot.begin_timestamp);
        guard.finish().unwrap();
        drop(snapshot);
        drop(authority);

        // Preserve the durable reservation's owner while restoring older coordination bytes.
        // A process-kill test cannot simulate this loss of unsynchronized mapped updates.
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(
            &(acknowledged_owner.0 - 1).to_le_bytes(),
            EPOCH_OFFSET as u64,
        )
        .unwrap();
        file.write_all_at(&1u64.to_le_bytes(), NEXT_TIMESTAMP_OFFSET as u64)
            .unwrap();
        file.sync_all().unwrap();
        drop(file);
        let authority = open(&path);
        let mut guard = authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        assert!(guard.register_snapshot().is_err());
        guard.reconcile_incarnation(acknowledged_owner.0).unwrap();
        let snapshot = guard.register_snapshot().unwrap();
        assert!(snapshot.epoch > acknowledged_owner.0);
        assert_ne!(
            (snapshot.epoch, snapshot.begin_timestamp),
            acknowledged_owner
        );
        guard.finish().unwrap();
    }

    #[test]
    fn authority_incarnation_reconciliation_requires_quiescence_and_checked_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let authority = open(&dir.path().join("coordination"));
        let mut guard = authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        let original = authority.load(EPOCH_OFFSET);
        assert!(!guard.incarnation_ready().unwrap());
        assert!(matches!(
            guard.reconcile_incarnation(u64::MAX),
            Err(LimboError::DatabaseFull)
        ));
        assert_eq!(authority.load(EPOCH_OFFSET), original);
        assert!(!guard.incarnation_ready().unwrap());
        guard.reconcile_incarnation(40).unwrap();
        assert_eq!(authority.load(EPOCH_OFFSET), 41);
        assert!(guard.incarnation_ready().unwrap());
        assert!(guard.reconcile_incarnation(50).is_err());
        let snapshot = guard.register_snapshot().unwrap();
        authority.store(INCARNATION_READY_OFFSET, 0);
        assert!(guard.reconcile_incarnation(50).is_err());
        assert_eq!(snapshot.epoch, authority.load(EPOCH_OFFSET));
        authority.store(INCARNATION_READY_OFFSET, 1);
        guard.finish().unwrap();
    }

    #[test]
    fn authority_existing_only_does_not_recreate_unlinked_live_mapping() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        let old_id = authority.file.file_id().unwrap();
        std::fs::remove_file(&path).unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let reopened = if std::env::var_os("TURSO_AUTHORITY_LEGACY_CREATE_CONTROL").is_some() {
            SharedMvccAuthority::open(&io, path.to_str().unwrap(), DATABASE_ID, 1024)
        } else {
            SharedMvccAuthority::open_existing(&io, path.to_str().unwrap(), DATABASE_ID, 1024)
        };
        assert!(
            reopened.is_err(),
            "recreated coordination while the old mapping remained live"
        );
        assert!(!path.exists());
        assert_eq!(authority.file.file_id().unwrap(), old_id);
        let guard = authority.try_acquire().unwrap();
        assert_eq!(guard.published().unwrap(), PublishedPrefix::default());
        guard.finish().unwrap();
    }

    #[test]
    fn authority_existing_only_rejects_missing_short_or_uninitialized_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        assert!(
            SharedMvccAuthority::open_existing(&io, path.to_str().unwrap(), DATABASE_ID, 1024)
                .is_err()
        );
        assert!(!path.exists());
        for size in [0, 1, MAP_LEN - 1, MAP_LEN, MAP_LEN + 1] {
            let bytes = vec![0; size];
            std::fs::write(&path, &bytes).unwrap();
            let before_id = io.file_id(path.to_str().unwrap()).unwrap();
            assert!(SharedMvccAuthority::open_existing(
                &io,
                path.to_str().unwrap(),
                DATABASE_ID,
                1024
            )
            .is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            assert_eq!(io.file_id(path.to_str().unwrap()).unwrap(), before_id);
            std::fs::remove_file(&path).unwrap();
        }
    }

    #[test]
    fn authority_existing_only_rejects_incompatible_header_without_mutation() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        drop(authority);
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let original = std::fs::read(&path).unwrap();
        for (id, limit) in [(FileId { dev: 1, ino: 3 }, 1024), (DATABASE_ID, 2048)] {
            assert!(
                SharedMvccAuthority::open_existing(&io, path.to_str().unwrap(), id, limit).is_err()
            );
            assert_eq!(std::fs::read(&path).unwrap(), original);
        }
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&999u64.to_le_bytes(), VERSION_OFFSET as u64)
            .unwrap();
        let incompatible = std::fs::read(&path).unwrap();
        assert!(
            SharedMvccAuthority::open_existing(&io, path.to_str().unwrap(), DATABASE_ID, 1024)
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), incompatible);
    }

    #[test]
    fn authority_existing_only_preserves_live_state_and_requires_recovery_after_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let existing =
            SharedMvccAuthority::open_existing(&io, path.to_str().unwrap(), DATABASE_ID, 1024)
                .unwrap();
        assert_eq!(
            existing.file.file_id().unwrap(),
            authority.file.file_id().unwrap()
        );
        let epoch = existing.load(EPOCH_OFFSET);
        let guard = authority.try_acquire().unwrap();
        assert!(matches!(existing.try_acquire(), Err(LimboError::Busy)));
        guard.finish().unwrap();
        let guard = existing.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        guard.finish().unwrap();
        drop(existing);
        drop(authority);
        let reopened =
            SharedMvccAuthority::open_existing(&io, path.to_str().unwrap(), DATABASE_ID, 1024)
                .unwrap();
        assert!(reopened.load(EPOCH_OFFSET) > epoch);
        assert!(reopened.try_acquire().unwrap().requires_recovery());
    }

    #[test]
    fn authority_recovers_after_independent_owner_is_killed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        let mut child = spawn(&path, "guard");
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        child.kill();
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let reopened =
            SharedMvccAuthority::open_existing(&io, path.to_str().unwrap(), DATABASE_ID, 1024)
                .unwrap();
        let mut guard = reopened.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        assert!(guard.published().is_err());
        guard.ensure_no_snapshot_locks().unwrap();
        assert!(guard.requires_recovery());
        assert!(guard.published().is_err());
        let recovered = PublishedPrefix {
            committed_timestamp: 9,
            end_offset: 128,
            running_crc: 17,
        };
        guard.recover(recovered).unwrap();
        assert_eq!(guard.published().unwrap(), recovered);
        guard.finish().unwrap();
        let guard = authority.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        assert_eq!(guard.published().unwrap(), recovered);
        guard.finish().unwrap();
    }

    #[test]
    fn authority_retains_live_snapshots_and_reclaims_dead_process_slots() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        let mut child = spawn(&path, "snapshot");
        let mut guard = authority.try_acquire().unwrap();
        assert!(matches!(
            guard.ensure_no_snapshot_locks(),
            Err(LimboError::Busy)
        ));
        let child_timestamp = guard.oldest_snapshot().unwrap().unwrap();
        let parent = guard.register_snapshot().unwrap();
        assert!(parent.begin_timestamp > child_timestamp);
        assert_ne!(parent.epoch, 0);
        assert_eq!(parent.prefix, PublishedPrefix::default());
        assert_eq!(guard.oldest_snapshot().unwrap(), Some(child_timestamp));
        let mut live = guard.live_registrations().unwrap();
        live.sort_unstable();
        assert_eq!(
            live,
            vec![
                (parent.epoch, child_timestamp),
                (parent.epoch, parent.begin_timestamp)
            ]
        );
        guard.finish().unwrap();
        child.kill();
        let guard = authority.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        assert!(matches!(
            guard.ensure_no_snapshot_locks(),
            Err(LimboError::Busy)
        ));
        assert_eq!(
            guard.oldest_snapshot().unwrap(),
            Some(parent.begin_timestamp)
        );
        assert_eq!(
            guard.live_registrations().unwrap(),
            vec![(parent.epoch, parent.begin_timestamp)]
        );
        drop(parent);
        guard.ensure_no_snapshot_locks().unwrap();
        assert_eq!(guard.oldest_snapshot().unwrap(), None);
        assert!(guard.live_registrations().unwrap().is_empty());
        guard.finish().unwrap();
    }

    #[test]
    fn authority_normal_publication_requires_recovery_and_advances_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let authority = open(&dir.path().join("coordination"));
        let mut guard = authority.try_acquire().unwrap();
        let previous = PublishedPrefix {
            committed_timestamp: 10,
            end_offset: 128,
            running_crc: 3,
        };
        let next = PublishedPrefix {
            committed_timestamp: 20,
            end_offset: 256,
            running_crc: 4,
        };
        assert!(guard.advance_committed(next).is_err());
        guard.recover(previous).unwrap();
        for invalid in [
            previous,
            PublishedPrefix {
                committed_timestamp: 9,
                ..next
            },
            PublishedPrefix {
                end_offset: 128,
                ..next
            },
            PublishedPrefix {
                committed_timestamp: MAX_TIMESTAMP,
                ..next
            },
        ] {
            assert!(guard.advance_committed(invalid).is_err());
            assert_eq!(guard.published().unwrap(), previous);
        }
        guard.advance_committed(next).unwrap();
        assert_eq!(guard.published().unwrap(), next);
        assert!(guard.allocate_timestamp().unwrap() > next.committed_timestamp);
        guard.finish().unwrap();
        assert_eq!(authority.try_acquire().unwrap().published().unwrap(), next);
    }

    #[test]
    fn authority_reopen_requires_recovery_and_local_abandonment_is_visible() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        drop(authority.try_acquire().unwrap());
        let mut guard = authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.finish().unwrap();
        drop(authority);
        let reopened = open(&path);
        assert!(reopened.try_acquire().unwrap().requires_recovery());
    }

    #[test]
    fn authority_slots_and_append_limits_fail_without_changing_publication() {
        let dir = tempfile::tempdir().unwrap();
        let authority = open(&dir.path().join("coordination"));
        let mut guard = authority.try_acquire().unwrap();
        let prefix = PublishedPrefix {
            committed_timestamp: 1,
            end_offset: 1000,
            running_crc: 17,
        };
        guard.recover(prefix).unwrap();
        guard.reconcile_incarnation(0).unwrap();
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        assert!(guard.check_append_size(24).is_ok());
        assert!(matches!(guard.check_append_size(25), Err(LimboError::Busy)));
        assert!(matches!(
            guard.check_append_size(1025),
            Err(LimboError::TooBig)
        ));
        let mut snapshots = Vec::new();
        for _ in 0..SLOT_COUNT {
            snapshots.push(guard.register_snapshot().unwrap());
        }
        assert!(matches!(guard.register_snapshot(), Err(LimboError::Busy)));
        let released = snapshots.remove(0).begin_timestamp;
        let replacement = guard.register_snapshot().unwrap();
        assert!(replacement.begin_timestamp > released);
        assert_eq!(guard.published().unwrap(), prefix);
        guard.finish().unwrap();
    }

    #[test]
    fn authority_rejects_incompatible_live_participants_without_rewriting() {
        use std::os::unix::fs::FileExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let before = std::fs::read(&path).unwrap();
        assert!(SharedMvccAuthority::open(
            &io,
            path.to_str().unwrap(),
            FileId { dev: 1, ino: 3 },
            1024,
        )
        .is_err());
        assert!(SharedMvccAuthority::open(&io, path.to_str().unwrap(), DATABASE_ID, 2048).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&999u64.to_le_bytes(), VERSION_OFFSET as u64)
            .unwrap();
        let incompatible = std::fs::read(&path).unwrap();
        assert!(SharedMvccAuthority::open(&io, path.to_str().unwrap(), DATABASE_ID, 1024).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), incompatible);
    }

    #[test]
    fn authority_child() {
        let Ok(path) = std::env::var("TURSO_MVCC_AUTHORITY_TEST_PATH") else {
            return;
        };
        let authority = open(std::path::Path::new(&path));
        let mut guard = authority.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        let action = std::env::var("TURSO_MVCC_AUTHORITY_TEST_ACTION").unwrap();
        let snapshot = if action == "snapshot" {
            let snapshot = guard.register_snapshot().unwrap();
            guard.finish().unwrap();
            Some(snapshot)
        } else {
            println!("AUTHORITY_READY");
            std::io::stdout().flush().unwrap();
            let mut command = String::new();
            std::io::stdin().read_line(&mut command).unwrap();
            guard.finish().unwrap();
            return;
        };
        println!("AUTHORITY_READY");
        std::io::stdout().flush().unwrap();
        let mut command = String::new();
        std::io::stdin().read_line(&mut command).unwrap();
        drop(snapshot);
    }

    fn open(path: &std::path::Path) -> Arc<SharedMvccAuthority> {
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        SharedMvccAuthority::open(&io, path.to_str().unwrap(), DATABASE_ID, 1024).unwrap()
    }

    fn initialize(authority: &Arc<SharedMvccAuthority>) {
        let mut guard = authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.reconcile_incarnation(0).unwrap();
        guard.finish().unwrap();
    }

    fn spawn(path: &std::path::Path, action: &str) -> ChildProcess {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env("TURSO_MVCC_AUTHORITY_TEST_PATH", path)
            .env("TURSO_MVCC_AUTHORITY_TEST_ACTION", action)
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
                if line.unwrap().contains("AUTHORITY_READY") {
                    let _ = sender.send(());
                    return;
                }
            }
        });
        receiver.recv_timeout(Duration::from_secs(20)).unwrap();
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

#[cfg(all(test, not(shuttle)))]
pub(super) fn authority_for_test(file: Arc<dyn File>) -> Arc<SharedMvccAuthority> {
    let authority = authority_for_test_unreconciled(file);
    authority.store(INCARNATION_READY_OFFSET, 1);
    authority
}

#[cfg(all(test, not(shuttle)))]
pub(super) fn authority_for_test_unreconciled(file: Arc<dyn File>) -> Arc<SharedMvccAuthority> {
    struct Mapping(Box<[MappedAtomic; MAP_LEN / 8]>);
    impl SharedWalMappedRegion for Mapping {
        fn ptr(&self) -> std::ptr::NonNull<u8> {
            std::ptr::NonNull::new(self.0.as_ptr().cast_mut().cast()).unwrap()
        }
        fn len(&self) -> usize {
            MAP_LEN
        }
    }
    let authority = Arc::new(SharedMvccAuthority {
        file,
        mapping: Box::new(Mapping(Box::new(std::array::from_fn(|_| {
            MappedAtomic::new(0)
        })))),
        publication_owned: AtomicBool::new(false),
        poisoned: AtomicBool::new(false),
        local_slots: Mutex::new([false; SLOT_COUNT]),
    });
    authority.initialize(FileId { dev: 1, ino: 2 }, 1_000_000);
    authority.store(EPOCH_OFFSET, 1);
    authority
}

#[cfg(all(test, not(shuttle)))]
#[test]
fn shared_authority_rejects_pre_reservation_protocol_without_reinitialization() {
    use crate::IO;
    let io = crate::MemoryIO::new();
    let file = io
        .open_file("old-authority", crate::io::OpenFlags::Create, false)
        .unwrap();
    let authority = authority_for_test(file);
    for version in [1, 2] {
        authority.store(VERSION_OFFSET, version);
        assert!(authority
            .validate_header(FileId { dev: 1, ino: 2 }, 1_000_000)
            .is_err());
        assert_eq!(authority.load(VERSION_OFFSET), version);
    }
}

fn check_combined_history(
    log: u64,
    bytes: u64,
    metadata: u64,
    minimum: u64,
    limit: u64,
) -> Result<()> {
    if metadata < minimum {
        return Err(LimboError::InvalidArgument(
            "history metadata budget is below its compact minimum".into(),
        ));
    }
    if bytes.checked_add(minimum).is_none_or(|size| size > limit) {
        return Err(LimboError::TooBig);
    }
    if log
        .checked_add(bytes)
        .and_then(|size| size.checked_add(metadata))
        .is_none_or(|size| size > limit)
    {
        return Err(LimboError::Busy);
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn shared_history_budget_counts_metadata_and_compaction_headroom() {
    assert!(check_combined_history(100, 50, 40, 20, 190).is_ok());
    assert!(matches!(
        check_combined_history(100, 51, 40, 20, 190),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        check_combined_history(0, 171, 40, 20, 190),
        Err(LimboError::TooBig)
    ));
    assert!(matches!(
        check_combined_history(u64::MAX, 1, 20, 20, u64::MAX),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        check_combined_history(0, u64::MAX, 20, 20, u64::MAX),
        Err(LimboError::TooBig)
    ));
    assert!(check_combined_history(0, 1, 19, 20, 190).is_err());
}
