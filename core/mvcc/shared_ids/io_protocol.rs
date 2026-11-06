#[path = "witness_mirror.rs"]
pub(in crate::mvcc) mod witness_mirror;

#[path = "compaction.rs"]
pub(in crate::mvcc) mod compaction;

use super::*;
use crate::mvcc::guarded_file::{FileBinding, GuardedFile};
use crate::mvcc::shared_authority::{AuthorityGuard, PublicationLease, PublishedPrefix};
use crate::sync::{Arc, Mutex};
use crate::types::IOCompletions;
use crate::{Buffer, Completion, IOResult};
use std::sync::atomic::{AtomicI32, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mvcc) struct RecoveryIdentity {
    pub database: DatabaseGeneration,
    pub prefix: PublishedPrefix,
}

pub(in crate::mvcc) struct SharedIds {
    banks: [Arc<GuardedFile>; 2],
    selector: Arc<GuardedFile>,
    database_id: FileId,
    history_budget: u64,
}

impl SharedIds {
    /// The caller supplies files opened existing-only in the validated database identity
    /// namespace, all bound to that same authority, and the retained main file ID.
    /// Guard bindings verify authority association; they do not establish path provenance.
    pub(in crate::mvcc) fn open_existing(
        banks: [Arc<GuardedFile>; 2],
        selector: Arc<GuardedFile>,
        database_id: FileId,
        history_budget: u64,
    ) -> Result<Arc<Self>> {
        use crate::io::File;
        if history_budget == 0 {
            return Err(invalid("ID journal history budget must be positive"));
        }
        let ids = [
            banks[0].file_id()?,
            banks[1].file_id()?,
            selector.file_id()?,
        ];
        if ids.iter().any(Option::is_none)
            || ids[0] == ids[1]
            || ids[0] == ids[2]
            || ids[1] == ids[2]
        {
            return Err(invalid(
                "ID journal needs three distinct retained native files",
            ));
        }
        Ok(Arc::new(Self {
            banks,
            selector,
            database_id,
            history_budget,
        }))
    }

    fn file(&self, index: usize) -> &Arc<GuardedFile> {
        match index {
            0 => &self.selector,
            1 => &self.banks[0],
            2 => &self.banks[1],
            _ => unreachable!(),
        }
    }

    pub(in crate::mvcc) fn load(
        self: &Arc<Self>,
        guard: AuthorityGuard,
        identity: RecoveryIdentity,
    ) -> Result<IdLoad> {
        use crate::io::File;
        if guard.published()? != identity.prefix {
            return Err(invalid(
                "ID journal oracle prefix differs from publication authority",
            ));
        }
        if identity.database.generation == 0
            || identity.database.checkpoint_timestamp > identity.prefix.committed_timestamp
        {
            return Err(invalid("ID journal recovery identity is inconsistent"));
        }
        if self.selector.size()? != (2 * PAGE_SIZE) as u64 {
            return Err(corrupt("invalid published ID selector length"));
        }
        let bindings = [
            self.selector.bind(&guard)?,
            self.banks[0].bind(&guard)?,
            self.banks[1].bind(&guard)?,
        ];
        let mut data = std::array::from_fn(|_| Vec::new());
        data[0].try_reserve_exact(2 * PAGE_SIZE)?;
        Ok(IdLoad {
            files: self.clone(),
            identity,
            ownership: Arc::new(Mutex::new(Some(guard))),
            bindings: Some(bindings),
            data,
            expected: [2 * PAGE_SIZE, 0, 0],
            index: 0,
            pending: None,
            selected: None,
            replayed: None,
            finished: false,
        })
    }
}

struct PendingRead {
    completion: Completion,
    buffer: Arc<Buffer>,
    count: Arc<AtomicI32>,
}

pub(in crate::mvcc) struct IdLoad {
    files: Arc<SharedIds>,
    identity: RecoveryIdentity,
    ownership: Arc<Mutex<Option<AuthorityGuard>>>,
    bindings: Option<[FileBinding; 3]>,
    data: [Vec<u8>; 3],
    expected: [usize; 3],
    index: usize,
    pending: Option<PendingRead>,
    selected: Option<SelectedIds>,
    replayed: Option<journal::State>,
    finished: bool,
}

pub(in crate::mvcc) struct LoadedIds {
    files: Arc<SharedIds>,
    pub(super) guard: AuthorityGuard,
    bindings: [FileBinding; 3],
    pub(super) identity: RecoveryIdentity,
    pub(super) selection: SelectedIds,
    pub(super) state: journal::State,
    banks: [Vec<u8>; 2],
}

impl LoadedIds {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        &self.guard
    }

    pub(in crate::mvcc) fn has_pending_checkpoint(&self) -> bool {
        self.selection.selector.prepared.is_some()
    }

    fn max_owner_epoch(&self) -> Result<Option<u64>> {
        let mut maximum = self.state.max_owner_epoch();
        if let Some(prepared) = self.selection.selector.prepared {
            let other = if self.selection.selected.bank == prepared.bank {
                self.selection.selector.active
            } else {
                prepared
            };
            let state = replay_selected(
                SelectedIds {
                    selected: other,
                    ..self.selection
                },
                [&self.banks[0], &self.banks[1]],
            )?;
            maximum = maximum.into_iter().chain(state.max_owner_epoch()).max();
        }
        Ok(maximum)
    }

    pub(in crate::mvcc) fn retained_bytes(&self) -> Result<u64> {
        use crate::io::File;
        self.guard.published()?;
        self.files
            .banks
            .iter()
            .chain(std::iter::once(&self.files.selector))
            .try_fold(0u64, |total, file| {
                total.checked_add(file.size()?).ok_or(LimboError::TooBig)
            })
    }

    /// Includes room for a floor-only replacement bank before the selected bank can be reclaimed.
    pub(in crate::mvcc) fn required_budget_bytes(&self) -> Result<u64> {
        use crate::io::File;
        let inactive = 1 - usize::from(self.selection.selected.bank);
        let headroom = ((2 * PAGE_SIZE) as u64).saturating_sub(self.files.banks[inactive].size()?);
        self.retained_bytes()?
            .checked_add(headroom)
            .ok_or(LimboError::TooBig)
    }

    pub(in crate::mvcc) fn into_guard(self) -> AuthorityGuard {
        self.guard
    }

    /// Persists a change already checked against live registrations and the committed oracle.
    /// This layer validates replay and durability; it does not choose or authorize an ID.
    pub(super) fn append(mut self, change: journal::Change) -> Result<IdAppend> {
        use crate::io::File;
        if self.guard.published()? != self.identity.prefix
            || self.selection.selector.prepared.is_some()
            || self.selection.selected != self.selection.selector.active
        {
            return Err(invalid(
                "ID append requires the current finalized generation",
            ));
        }
        let record = journal::Record {
            sequence: self
                .state
                .last_record
                .checked_add(1)
                .ok_or(LimboError::DatabaseFull)?,
            change,
        }
        .encode()?;
        let mut selector = self.selection.selector;
        selector.sequence = selector
            .sequence
            .checked_add(1)
            .ok_or(LimboError::DatabaseFull)?;
        let old_end = selector.active.end;
        selector.active.end = old_end
            .checked_add(PAGE_SIZE as u64)
            .ok_or(LimboError::TooBig)?;
        let bank = usize::from(selector.active.bank);
        let physical_active = self.files.banks[bank].size()?;
        let required = self
            .required_budget_bytes()?
            .checked_add(self.identity.prefix.end_offset)
            .and_then(|bytes| {
                bytes.checked_add(selector.active.end.saturating_sub(physical_active))
            })
            .ok_or(LimboError::TooBig)?;
        if required > self.files.history_budget {
            return Err(LimboError::Busy);
        }
        let bytes = &mut self.banks[bank];
        bytes.try_reserve_exact(PAGE_SIZE)?;
        bytes.extend_from_slice(&record);
        selector.active.running_crc = crc32c::crc32c(bytes);
        // Validate a private replay before any I/O; failed mutations never escape as live state.
        self.state = journal::State::replay_snapshot(
            &bytes[PAGE_SIZE..],
            self.identity.database.generation,
            (selector.active.snapshot_end as usize) - PAGE_SIZE,
        )?;
        let selector_bytes = selector.encode()?;
        let record_buffer = Arc::new(Buffer::new_temporary(PAGE_SIZE));
        record_buffer.as_mut_slice().copy_from_slice(&record);
        let selector_buffer = Arc::new(Buffer::new_temporary(PAGE_SIZE));
        selector_buffer
            .as_mut_slice()
            .copy_from_slice(&selector_bytes);
        self.selection = SelectedIds {
            selector,
            selected: selector.active,
            slots_agree: true,
        };
        Ok(IdAppend {
            loaded: Some(self),
            record_buffer,
            selector_buffer,
            old_end,
            step: 0,
            pending: None,
            finished: false,
        })
    }
}

pub(super) struct IdAppend {
    loaded: Option<LoadedIds>,
    record_buffer: Arc<Buffer>,
    selector_buffer: Arc<Buffer>,
    old_end: u64,
    step: u8,
    pending: Option<Completion>,
    finished: bool,
}
impl IdAppend {
    pub(super) fn poll(&mut self) -> Result<IOResult<LoadedIds>> {
        use crate::io::{File, FileSyncType};
        if self.finished {
            return Err(invalid("ID append already terminated"));
        }
        self.finished = true;
        loop {
            if let Some(completion) = self.pending.take() {
                if !completion.finished() {
                    self.pending = Some(completion.clone());
                    self.finished = false;
                    return Ok(IOResult::IO(IOCompletions(completion)));
                }
                if let Some(error) = completion.get_error() {
                    return Err(error.into());
                }
            }
            let loaded = self.loaded.as_ref().unwrap();
            let bank = &loaded.files.banks[usize::from(loaded.selection.selected.bank)];
            let selector = &loaded.files.selector;
            let completion = match self.step {
                0 => bank.truncate(self.old_end, Completion::new_trunc(|_| {}))?,
                1 => bank.pwrite(
                    self.old_end,
                    self.record_buffer.clone(),
                    Completion::new_write(|_| {}),
                )?,
                2 => bank.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                3 => selector.pwrite(
                    0,
                    self.selector_buffer.clone(),
                    Completion::new_write(|_| {}),
                )?,
                4 => selector.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                5 => selector.pwrite(
                    PAGE_SIZE as u64,
                    self.selector_buffer.clone(),
                    Completion::new_write(|_| {}),
                )?,
                6 => selector.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                7 => {
                    if !loaded.guard.io_retired() {
                        let completion = loaded.guard.retirement_completion();
                        self.finished = false;
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    return Ok(IOResult::Done(self.loaded.take().unwrap()));
                }
                _ => unreachable!(),
            };
            self.step += 1;
            self.pending = Some(completion);
        }
    }
}

impl IdLoad {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<LoadedIds>> {
        use crate::io::File;
        if self.finished {
            return Err(invalid("ID journal load already terminated"));
        }
        self.finished = true;
        loop {
            if let Some(read) = self.pending.take() {
                if !read.completion.finished() {
                    let completion = read.completion.clone();
                    self.pending = Some(read);
                    self.finished = false;
                    return Ok(IOResult::IO(IOCompletions(completion)));
                }
                if let Some(error) = read.completion.get_error() {
                    return Err(error.into());
                }
                let count = read.count.load(Ordering::Acquire);
                if count <= 0 || count as usize > read.buffer.len() {
                    return Err(corrupt("short or invalid ID journal read"));
                }
                self.data[self.index].extend_from_slice(&read.buffer.as_slice()[..count as usize]);
            }
            if self.index == 3 {
                if self.selected.is_none() {
                    let selected = select(
                        [&self.data[0][..PAGE_SIZE], &self.data[0][PAGE_SIZE..]],
                        [&self.data[1], &self.data[2]],
                        self.files.database_id,
                        self.identity.database,
                    )?;
                    self.replayed =
                        Some(replay_selected(selected, [&self.data[1], &self.data[2]])?);
                    self.selected = Some(selected);
                }
                if !self.ownership.lock().as_ref().unwrap().io_retired() {
                    self.finished = false;
                    let completion = self
                        .ownership
                        .lock()
                        .as_ref()
                        .unwrap()
                        .retirement_completion();
                    return Ok(IOResult::IO(IOCompletions(completion)));
                }
                return Ok(IOResult::Done(LoadedIds {
                    files: self.files.clone(),
                    guard: self.ownership.lock().take().unwrap(),
                    bindings: self.bindings.take().unwrap(),
                    identity: self.identity,
                    selection: self.selected.take().unwrap(),
                    state: self.replayed.take().unwrap(),
                    banks: [
                        std::mem::take(&mut self.data[1]),
                        std::mem::take(&mut self.data[2]),
                    ],
                }));
            }
            if self.data[self.index].len() == self.expected[self.index] {
                if self.index == 0 {
                    self.prepare_bank_reads()?;
                }
                self.index += 1;
                continue;
            }
            let filled = self.data[self.index].len();
            let remaining = self.expected[self.index] - filled;
            let buffer = Arc::new(Buffer::new_temporary(remaining.min(PAGE_SIZE)));
            let count = Arc::new(AtomicI32::new(-1));
            let callback_count = count.clone();
            let ownership = self.ownership.clone();
            let pin = PublicationLease::pin(&ownership.lock().as_ref().unwrap().io_lease())?;
            let completion = Completion::new_read(buffer.clone(), move |result| {
                let _keep_alive = (&ownership, &pin);
                if let Ok((_, bytes)) = result {
                    callback_count.store(bytes, Ordering::Release);
                }
                None
            });
            let completion = self
                .files
                .file(self.index)
                .pread(filled as u64, completion)?;
            self.pending = Some(PendingRead {
                completion,
                buffer,
                count,
            });
        }
    }

    fn prepare_bank_reads(&mut self) -> Result<()> {
        use crate::io::File;
        let (selector, _) = select_witness(
            [&self.data[0][..PAGE_SIZE], &self.data[0][PAGE_SIZE..]],
            self.files.database_id,
        )?;
        for reference in [Some(selector.active), selector.prepared]
            .into_iter()
            .flatten()
        {
            let index = usize::from(reference.bank) + 1;
            if self.files.file(index).size()? < reference.end {
                return Err(corrupt("selected ID bank is truncated"));
            }
            self.expected[index] = reference
                .end
                .try_into()
                .map_err(|_| corrupt("ID bank exceeds address space"))?;
        }
        let required = self
            .expected
            .iter()
            .try_fold(self.identity.prefix.end_offset, |total, size| {
                total.checked_add(*size as u64)
            })
            .ok_or(LimboError::TooBig)?;
        if required > self.files.history_budget {
            return Err(LimboError::TooBig);
        }
        for index in 1..=2 {
            self.data[index].try_reserve_exact(self.expected[index])?;
        }
        Ok(())
    }
}

fn invalid(message: &str) -> LimboError {
    LimboError::InvalidArgument(message.into())
}

#[cfg(all(test, not(shuttle)))]
mod tests {
    use super::*;
    use crate::io::{File, FileSyncType, SharedWalLockKind};
    use crate::mvcc::shared_authority::{
        authority_for_test, authority_for_test_unreconciled, SharedMvccAuthority,
    };
    use crate::sync::atomic::{AtomicBool, AtomicUsize};

    enum Mutation {
        Write(u64, Arc<Buffer>),
        Truncate(u64),
        Sync,
    }

    struct TestFile {
        id: FileId,
        bytes: Mutex<Vec<u8>>,
        publication: AtomicBool,
        delayed: AtomicBool,
        limit: AtomicUsize,
        pending: Mutex<Option<(u64, Completion)>>,
        retained: Mutex<Vec<Completion>>,
        mutations: AtomicUsize,
        writable: AtomicBool,
        durable: Mutex<Vec<u8>>,
        fault: Mutex<Option<(Arc<AtomicUsize>, usize)>>,
        delay_mutations: AtomicBool,
        pending_mutation: Mutex<Option<(Mutation, Completion)>>,
    }
    impl TestFile {
        fn new(ino: u64, bytes: Vec<u8>) -> Arc<Self> {
            Arc::new(Self {
                id: FileId { dev: 1, ino },
                fault: Mutex::new(None),
                delay_mutations: AtomicBool::new(false),
                pending_mutation: Mutex::new(None),
                durable: Mutex::new(bytes.clone()),
                bytes: Mutex::new(bytes),
                publication: AtomicBool::new(false),
                delayed: AtomicBool::new(false),
                limit: AtomicUsize::new(usize::MAX),
                pending: Mutex::new(None),
                retained: Mutex::new(Vec::new()),
                mutations: AtomicUsize::new(0),
                writable: AtomicBool::new(false),
            })
        }
        fn mutate(&self, action: Mutation, completion: Completion) -> Result<Completion> {
            self.mutations.fetch_add(1, Ordering::AcqRel);
            if !self.writable.load(Ordering::Acquire) {
                return Err(invalid("unexpected loader mutation"));
            }
            if let Some((step, fail_at)) = &*self.fault.lock() {
                if step.fetch_add(1, Ordering::AcqRel) == *fail_at {
                    return Err(invalid("injected ID journal mutation failure"));
                }
            }
            if self.delay_mutations.load(Ordering::Acquire) {
                assert!(self
                    .pending_mutation
                    .lock()
                    .replace((action, completion.clone()))
                    .is_none());
            } else {
                self.perform_mutation(action, &completion);
            }
            Ok(completion)
        }
        fn perform_mutation(&self, action: Mutation, completion: &Completion) {
            let count = match action {
                Mutation::Write(pos, buffer) => {
                    let mut bytes = self.bytes.lock();
                    let start = pos as usize;
                    let len = bytes.len().max(start + buffer.len());
                    bytes.resize(len, 0);
                    bytes[start..start + buffer.len()].copy_from_slice(buffer.as_slice());
                    buffer.len() as i32
                }
                Mutation::Truncate(size) => {
                    self.bytes.lock().resize(size as usize, 0);
                    0
                }
                Mutation::Sync => {
                    self.durable.lock().clone_from(&self.bytes.lock());
                    0
                }
            };
            completion.complete(count);
        }
        fn complete_mutation(&self, retain: bool) {
            let (action, completion) = self.pending_mutation.lock().take().unwrap();
            self.perform_mutation(action, &completion);
            if retain {
                self.retained.lock().push(completion);
            }
        }
        fn complete_read(&self, pos: u64, completion: &Completion) {
            let bytes = self.bytes.lock();
            let start = pos as usize;
            let buffer = completion.as_read().buf();
            let count = buffer
                .len()
                .min(bytes.len().saturating_sub(start))
                .min(self.limit.load(Ordering::Acquire));
            if count != 0 {
                buffer.as_mut_slice()[..count].copy_from_slice(&bytes[start..start + count]);
            }
            completion.complete(count as i32);
        }
        fn complete_pending(&self, retain: bool) {
            let (pos, completion) = self.pending.lock().take().unwrap();
            self.complete_read(pos, &completion);
            if retain {
                self.retained.lock().push(completion);
            }
        }
        fn retire(&self) {
            self.retained.lock().clear();
        }
    }
    impl File for TestFile {
        fn file_id(&self) -> Result<Option<FileId>> {
            Ok(Some(self.id))
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
        fn pread(&self, pos: u64, completion: Completion) -> Result<Completion> {
            if self.delayed.load(Ordering::Acquire) {
                assert!(self
                    .pending
                    .lock()
                    .replace((pos, completion.clone()))
                    .is_none());
            } else {
                self.complete_read(pos, &completion);
            }
            Ok(completion)
        }
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.mutate(Mutation::Write(pos, buffer), c)
        }
        fn truncate(&self, size: u64, c: Completion) -> Result<Completion> {
            self.mutate(Mutation::Truncate(size), c)
        }
        fn sync(&self, c: Completion, kind: FileSyncType) -> Result<Completion> {
            assert!(matches!(kind, FileSyncType::FullFsync));
            self.mutate(Mutation::Sync, c)
        }
        fn shared_wal_try_lock_byte(
            &self,
            offset: u64,
            _: bool,
            _: SharedWalLockKind,
        ) -> Result<bool> {
            if offset == 1 {
                return Ok(self
                    .publication
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok());
            }
            Ok(true)
        }
        fn shared_wal_unlock_byte(&self, offset: u64, _: SharedWalLockKind) -> Result<()> {
            if offset == 1 {
                self.publication.store(false, Ordering::Release);
            }
            Ok(())
        }
    }

    struct Fixture {
        authority: Arc<SharedMvccAuthority>,
        files: Arc<SharedIds>,
        selector: Arc<TestFile>,
        bank: Arc<TestFile>,
        unused: Arc<TestFile>,
        identity: RecoveryIdentity,
    }
    fn fixture() -> Fixture {
        fixture_with_authority(authority_for_test(TestFile::new(100, Vec::new())))
    }
    fn fixture_with_authority(authority: Arc<SharedMvccAuthority>) -> Fixture {
        let id = FileId { dev: 1, ino: 2 };
        let database = DatabaseGeneration {
            generation: 1,
            checkpoint_timestamp: 0,
        };
        let mut bytes = BankHeader {
            database_id: id,
            bank_generation: 1,
            database,
        }
        .encode()
        .unwrap()
        .to_vec();
        bytes.extend_from_slice(
            &journal::Record {
                sequence: 1,
                change: journal::Change::LogicalFloor(-100),
            }
            .encode()
            .unwrap(),
        );
        let active = BankRef {
            bank: 0,
            bank_generation: 1,
            database,
            end: bytes.len() as u64,
            snapshot_end: bytes.len() as u64,
            running_crc: crc32c::crc32c(&bytes),
        };
        let record = IdSelector {
            sequence: 1,
            database_id: id,
            active,
            prepared: None,
        }
        .encode()
        .unwrap();
        let mut selector_bytes = record.to_vec();
        selector_bytes.extend_from_slice(&record);
        let selector = TestFile::new(10, selector_bytes);
        let bank = TestFile::new(11, bytes);
        let unused = TestFile::new(12, Vec::new());
        let files = SharedIds::open_existing(
            [
                GuardedFile::new(bank.clone(), &authority),
                GuardedFile::new(unused.clone(), &authority),
            ],
            GuardedFile::new(selector.clone(), &authority),
            id,
            1_000_000,
        )
        .unwrap();
        let mut guard = authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.finish().unwrap();
        Fixture {
            authority,
            files,
            selector,
            bank,
            unused,
            identity: RecoveryIdentity {
                database,
                prefix: PublishedPrefix::default(),
            },
        }
    }

    #[test]
    fn id_load_handles_short_reads_without_mutating_content() {
        let f = fixture();
        f.selector.limit.store(73, Ordering::Release);
        f.bank.limit.store(31, Ordering::Release);
        let mut load = f
            .files
            .load(f.authority.try_acquire().unwrap(), f.identity)
            .unwrap();
        let IOResult::Done(loaded) = load.poll().unwrap() else {
            panic!("synchronous short-read backend");
        };
        assert_eq!(loaded.state.logical_floor, -100);
        assert_eq!(loaded.state.last_record, 1);
        assert_eq!(f.selector.mutations.load(Ordering::Acquire), 0);
        assert_eq!(f.bank.mutations.load(Ordering::Acquire), 0);
        loaded.guard.finish().unwrap();
    }

    #[test]
    fn id_load_cancel_keeps_authority_until_read_backend_retires() {
        let f = fixture();
        f.selector.delayed.store(true, Ordering::Release);
        let mut load = f
            .files
            .load(f.authority.try_acquire().unwrap(), f.identity)
            .unwrap();
        assert!(matches!(load.poll().unwrap(), IOResult::IO(_)));
        drop(load);
        assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
        f.selector.complete_pending(true);
        assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
        f.selector.retire();
        let mut guard = f.authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.finish().unwrap();
    }

    #[test]
    fn id_load_does_not_report_done_before_read_retirement() {
        let f = fixture();
        f.selector.delayed.store(true, Ordering::Release);
        let mut load = f
            .files
            .load(f.authority.try_acquire().unwrap(), f.identity)
            .unwrap();
        assert!(matches!(load.poll().unwrap(), IOResult::IO(_)));
        f.selector.complete_pending(true);
        assert!(matches!(load.poll().unwrap(), IOResult::IO(_)));
        f.selector.complete_pending(true);
        assert!(matches!(load.poll().unwrap(), IOResult::IO(_)));
        f.selector.retire();
        let IOResult::Done(loaded) = load.poll().unwrap() else {
            panic!("backend reads retired");
        };
        loaded.guard.finish().unwrap();
    }

    #[test]
    fn id_load_rejects_stale_prefix_and_corrupt_selected_data() {
        let f = fixture();
        let stale = RecoveryIdentity {
            prefix: PublishedPrefix {
                committed_timestamp: 1,
                ..PublishedPrefix::default()
            },
            ..f.identity
        };
        assert!(f
            .files
            .load(f.authority.try_acquire().unwrap(), stale)
            .is_err());
        let mut guard = f.authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        f.bank.bytes.lock()[0] ^= 1;
        let mut load = f.files.load(guard, f.identity).unwrap();
        assert!(load.poll().is_err());
        assert_eq!(f.selector.mutations.load(Ordering::Acquire), 0);
        assert_eq!(f.bank.mutations.load(Ordering::Acquire), 0);
    }
    #[test]
    fn id_load_aborted_read_retains_authority_until_backend_retirement() {
        let f = fixture();
        f.selector.delayed.store(true, Ordering::Release);
        let mut load = f
            .files
            .load(f.authority.try_acquire().unwrap(), f.identity)
            .unwrap();
        let IOResult::IO(waiter) = load.poll().unwrap() else {
            panic!("delayed read");
        };
        waiter.0.abort();
        assert!(load.poll().is_err());
        drop(load);
        drop(waiter);
        assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
        f.selector.complete_pending(true);
        assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
        f.selector.retire();
        let mut guard = f.authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.finish().unwrap();
    }

    #[test]
    fn id_load_rejects_guard_from_another_authority() {
        let f = fixture();
        let other = fixture();
        assert!(f
            .files
            .load(other.authority.try_acquire().unwrap(), f.identity)
            .is_err());
        assert!(f.selector.pending.lock().is_none());
        let guard = f.authority.try_acquire().unwrap();
        guard.finish().unwrap();
    }

    #[test]
    fn id_load_eof_and_read_error_are_terminal() {
        for fail in [false, true] {
            let f = fixture();
            f.selector.delayed.store(true, Ordering::Release);
            let mut load = f
                .files
                .load(f.authority.try_acquire().unwrap(), f.identity)
                .unwrap();
            assert!(matches!(load.poll().unwrap(), IOResult::IO(_)));
            if fail {
                let (_, completion) = f.selector.pending.lock().take().unwrap();
                completion.error(crate::error::CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "test ID read",
                ));
            } else {
                f.selector.bytes.lock().clear();
                f.selector.complete_pending(false);
            }
            assert!(load.poll().is_err());
            assert!(load.poll().is_err());
            assert_eq!(f.selector.mutations.load(Ordering::Acquire), 0);
        }
    }

    fn loaded(f: &Fixture) -> LoadedIds {
        let mut load = f
            .files
            .load(f.authority.try_acquire().unwrap(), f.identity)
            .unwrap();
        let IOResult::Done(loaded) = load.poll().unwrap() else {
            panic!("synchronous fixture");
        };
        loaded
    }

    #[test]
    fn id_append_acknowledges_only_durable_mirrored_floor() {
        let f = fixture();
        f.bank.writable.store(true, Ordering::Release);
        f.selector.writable.store(true, Ordering::Release);
        let mut append = loaded(&f)
            .append(journal::Change::LogicalFloor(-200))
            .unwrap();
        let IOResult::Done(result) = append.poll().unwrap() else {
            panic!("synchronous mutation fixture");
        };
        assert_eq!(result.state.logical_floor, -200);
        let slots = f.selector.durable.lock();
        let bank = f.bank.durable.lock();
        let selected = select(
            [&slots[..PAGE_SIZE], &slots[PAGE_SIZE..]],
            [&bank, &[]],
            f.files.database_id,
            f.identity.database,
        )
        .unwrap();
        assert!(selected.slots_agree);
        assert_eq!(
            replay_selected(selected, [&bank, &[]])
                .unwrap()
                .logical_floor,
            -200
        );
        assert_eq!(f.bank.mutations.load(Ordering::Acquire), 3);
        assert_eq!(f.selector.mutations.load(Ordering::Acquire), 4);
        result.guard.finish().unwrap();
    }

    #[test]
    fn id_append_failure_boundaries_recover_a_complete_selected_floor() {
        for fail_at in 0..7 {
            let f = fixture();
            let step = Arc::new(AtomicUsize::new(0));
            for file in [&f.bank, &f.selector] {
                file.writable.store(true, Ordering::Release);
                *file.fault.lock() = Some((step.clone(), fail_at));
            }
            let mut append = loaded(&f)
                .append(journal::Change::LogicalFloor(-200))
                .unwrap();
            assert!(append.poll().is_err(), "failure boundary {fail_at}");
            assert!(append.poll().is_err(), "error must be terminal");
            drop(append);
            let slots = f.selector.durable.lock();
            let bank = f.bank.durable.lock();
            let selected = select(
                [&slots[..PAGE_SIZE], &slots[PAGE_SIZE..]],
                [&bank, &[]],
                f.files.database_id,
                f.identity.database,
            )
            .unwrap();
            let floor = replay_selected(selected, [&bank, &[]])
                .unwrap()
                .logical_floor;
            assert_eq!(
                floor,
                if fail_at >= 5 { -200 } else { -100 },
                "failure boundary {fail_at}"
            );
            let mut guard = f.authority.try_acquire().unwrap();
            assert!(guard.requires_recovery());
            guard.recover(PublishedPrefix::default()).unwrap();
            guard.finish().unwrap();
        }
    }

    #[test]
    fn id_append_preflight_failure_does_not_mutate_files() {
        for case in 0..3 {
            let mut f = fixture();
            let pressure = case != 0;
            if case == 1 {
                Arc::get_mut(&mut f.files).unwrap().history_budget = (4 * PAGE_SIZE) as u64;
            } else if case == 2 {
                f.unused.bytes.lock().resize(2 * PAGE_SIZE, 0);
                Arc::get_mut(&mut f.files).unwrap().history_budget = (6 * PAGE_SIZE) as u64;
            }
            let change = journal::Change::LogicalFloor(if pressure { -200 } else { -1 });
            let result = loaded(&f).append(change);
            if pressure {
                assert!(matches!(result, Err(LimboError::Busy)));
            } else {
                assert!(result.is_err());
            }
            assert_eq!(f.bank.mutations.load(Ordering::Acquire), 0);
            assert_eq!(f.selector.mutations.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn id_append_waits_for_each_mutation_and_final_backend_retirement() {
        let f = fixture();
        for file in [&f.bank, &f.selector] {
            file.writable.store(true, Ordering::Release);
            file.delay_mutations.store(true, Ordering::Release);
        }
        let mut append = loaded(&f)
            .append(journal::Change::LogicalFloor(-200))
            .unwrap();
        for step in 0..7 {
            let IOResult::IO(waiter) = append.poll().unwrap() else {
                panic!("mutation {step} must wait");
            };
            assert!(!waiter.0.finished());
            assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
            let file = if step < 3 { &f.bank } else { &f.selector };
            file.complete_mutation(true);
        }
        let IOResult::IO(waiter) = append.poll().unwrap() else {
            panic!("retirement must wait");
        };
        assert!(!waiter.0.finished());
        f.bank.retire();
        assert!(!waiter.0.finished());
        f.selector.retire();
        assert!(waiter.0.finished());
        let IOResult::Done(result) = append.poll().unwrap() else {
            panic!("all writes retired");
        };
        assert_eq!(result.state.logical_floor, -200);
        result.guard.finish().unwrap();
    }

    #[test]
    fn id_append_abort_at_every_mutation_keeps_authority_until_retirement() {
        for cancel_at in 0..7 {
            let f = fixture();
            for file in [&f.bank, &f.selector] {
                file.writable.store(true, Ordering::Release);
                file.delay_mutations.store(true, Ordering::Release);
            }
            let mut append = loaded(&f)
                .append(journal::Change::LogicalFloor(-200))
                .unwrap();
            for step in 0..=cancel_at {
                let IOResult::IO(waiter) = append.poll().unwrap() else {
                    panic!("mutation {step} must wait");
                };
                let file = if step < 3 { &f.bank } else { &f.selector };
                if step == cancel_at {
                    waiter.0.abort();
                    assert!(append.poll().is_err());
                    drop(append);
                    assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
                    file.complete_mutation(true);
                    assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
                    file.retire();
                    let mut guard = f.authority.try_acquire().unwrap();
                    assert!(guard.requires_recovery());
                    guard.recover(PublishedPrefix::default()).unwrap();
                    guard.finish().unwrap();
                    break;
                }
                file.complete_mutation(false);
            }
        }
    }

    #[test]
    fn id_append_discards_only_unacknowledged_tail_and_reuses_held_bindings() {
        let f = fixture();
        f.bank.bytes.lock().extend_from_slice(&[0x55; PAGE_SIZE]);
        f.bank.writable.store(true, Ordering::Release);
        f.selector.writable.store(true, Ordering::Release);
        let mut current = loaded(&f);
        for floor in [-200, -300] {
            let mut append = current
                .append(journal::Change::LogicalFloor(floor))
                .unwrap();
            let IOResult::Done(result) = append.poll().unwrap() else {
                panic!("synchronous fixture");
            };
            assert_eq!(result.state.logical_floor, floor);
            assert_eq!(
                result.selection.selected.end,
                f.bank.bytes.lock().len() as u64
            );
            current = result;
        }
        assert_eq!(current.state.last_record, 3);
        current.guard.finish().unwrap();
    }

    struct Oracle {
        identity: RecoveryIdentity,
        maximum: Option<i64>,
    }
    impl super::super::policy::CommittedIdOracle for Oracle {
        fn identity(&self) -> RecoveryIdentity {
            self.identity
        }
        fn poll(
            &mut self,
            _: &AuthorityGuard,
            query: &super::super::policy::IdQuery,
        ) -> Result<IOResult<super::super::policy::IdFact>> {
            use super::super::policy::{IdFact, IdQuery};
            Ok(IOResult::Done(match query {
                IdQuery::RowFacts { .. } => IdFact::RowFacts {
                    maximum: self.maximum,
                    autoincrement_floor: 0,
                },
                IdQuery::RowExists { value, .. } => IdFact::RowExists(self.maximum == Some(*value)),
                IdQuery::LogicalFloor => IdFact::LogicalFloor(-1),
            }))
        }
    }
    fn registration(f: &Fixture) -> crate::mvcc::shared_authority::SnapshotRegistration {
        let mut guard = f.authority.try_acquire().unwrap();
        let registration = guard.register_snapshot().unwrap();
        guard.finish().unwrap();
        registration
    }
    fn policy(
        f: &Fixture,
        action: super::super::policy::IdAction,
        maximum: Option<i64>,
    ) -> super::super::policy::IdResult {
        let mut operation = f
            .files
            .start(
                f.authority.try_acquire().unwrap(),
                f.identity,
                Box::new(Oracle {
                    identity: f.identity,
                    maximum,
                }),
                Arc::new(crate::MemoryIO::new()),
                action,
            )
            .unwrap();
        let IOResult::Done(outcome) = operation.poll().unwrap() else {
            panic!("synchronous oracle");
        };
        assert_eq!(
            outcome.required_budget_bytes,
            outcome.retained_bytes
                + ((2 * PAGE_SIZE) as u64).saturating_sub(f.unused.size().unwrap())
        );
        assert_eq!(
            outcome.retained_bytes,
            f.selector.size().unwrap() + f.bank.size().unwrap() + f.unused.size().unwrap()
        );
        outcome.guard.finish().unwrap();
        outcome.result
    }
    #[test]
    fn id_policy_reserves_distinct_rows_before_either_owner_commits() {
        use super::super::policy::{IdAction, IdResult};
        let f = fixture();
        f.bank.writable.store(true, Ordering::Release);
        f.selector.writable.store(true, Ordering::Release);
        let a = registration(&f);
        let b = registration(&f);
        let domain = journal::Domain {
            generation: 1,
            table: -2,
            sequence: false,
        };
        for (registration, expected) in [(&a, 1), (&b, 2)] {
            let owner = journal::Owner {
                epoch: registration.epoch,
                begin: registration.begin_timestamp,
            };
            let result = policy(
                &f,
                IdAction::ReserveRow {
                    owner,
                    domain,
                    autoincrement: false,
                },
                None,
            );
            let IdResult::Reserved { id, value } = result else {
                panic!("reservation");
            };
            assert_eq!(id.owner, owner);
            assert_eq!(id.ordinal, 1);
            assert_eq!(value, expected);
        }
    }

    fn owner(registration: &crate::mvcc::shared_authority::SnapshotRegistration) -> journal::Owner {
        journal::Owner {
            epoch: registration.epoch,
            begin: registration.begin_timestamp,
        }
    }
    fn writable_fixture() -> Fixture {
        let f = fixture();
        f.unused.writable.store(true, Ordering::Release);
        f.bank.writable.store(true, Ordering::Release);
        f.selector.writable.store(true, Ordering::Release);
        f
    }
    fn reserved(result: super::super::policy::IdResult) -> (u64, i64) {
        let super::super::policy::IdResult::Reserved { id, value } = result else {
            panic!("reservation");
        };
        (id.ordinal, value)
    }
    #[test]
    fn id_policy_savepoints_do_not_reuse_logical_ids_or_ordinals() {
        use super::super::policy::{IdAction, IdResult};
        let f = writable_fixture();
        let a = registration(&f);
        let a_owner = owner(&a);
        let IdResult::Marked(empty) = policy(&f, IdAction::Savepoint { owner: a_owner }, None)
        else {
            panic!("mark");
        };
        assert_eq!(empty.next_ordinal, 1);
        assert_eq!(
            policy(&f, IdAction::ReleaseAfter { mark: empty }, None),
            IdResult::Released
        );
        assert_eq!(f.bank.mutations.load(Ordering::Acquire), 0);
        assert_eq!(
            reserved(policy(
                &f,
                IdAction::ReserveLogicalTable { owner: a_owner },
                None
            )),
            (1, -101)
        );
        let IdResult::Marked(mark) = policy(&f, IdAction::Savepoint { owner: a_owner }, None)
        else {
            panic!("mark");
        };
        assert_eq!(
            reserved(policy(
                &f,
                IdAction::ReserveLogicalTable { owner: a_owner },
                None
            )),
            (2, -102)
        );
        policy(&f, IdAction::ReleaseAfter { mark }, None);
        assert_eq!(
            reserved(policy(
                &f,
                IdAction::ReserveLogicalTable { owner: a_owner },
                None
            )),
            (3, -103)
        );
        drop(a);
        let b = registration(&f);
        assert_eq!(
            reserved(policy(
                &f,
                IdAction::ReserveLogicalTable { owner: owner(&b) },
                None
            )),
            (1, -104)
        );
    }

    #[test]
    fn id_policy_reclaims_dead_row_claims_and_reconciles_committed_maximum() {
        use super::super::policy::IdAction;
        for committed in [None, Some(100)] {
            let f = writable_fixture();
            let a = registration(&f);
            let domain = journal::Domain {
                generation: 1,
                table: -2,
                sequence: false,
            };
            assert_eq!(
                reserved(policy(
                    &f,
                    IdAction::ReserveRow {
                        owner: owner(&a),
                        domain,
                        autoincrement: false
                    },
                    None
                )),
                (1, 1)
            );
            drop(a);
            let b = registration(&f);
            assert_eq!(
                reserved(policy(
                    &f,
                    IdAction::ReserveRow {
                        owner: owner(&b),
                        domain,
                        autoincrement: false
                    },
                    committed
                )),
                (1, committed.unwrap_or(0) + 1)
            );
        }
    }

    #[test]
    fn id_policy_sequence_claims_preserve_supplied_cycle_descending_and_boundary_values() {
        use super::super::policy::{IdAction, IdResult, SequenceDurability};
        for durability in [
            SequenceDurability::OuterTransaction,
            SequenceDurability::AutonomousInner,
        ] {
            let f = writable_fixture();
            let a = registration(&f);
            let domain = journal::Domain {
                generation: 1,
                table: -3,
                sequence: true,
            };
            for value in [9, 1, 2, 1, 0, -1, -2, i64::MIN, i64::MAX] {
                let (_, recorded) = reserved(policy(
                    &f,
                    IdAction::ClaimSequence {
                        owner: owner(&a),
                        domain,
                        value,
                        durability,
                    },
                    None,
                ));
                assert_eq!(recorded, value);
            }
            assert_eq!(
                policy(&f, IdAction::LowestLiveSequenceClaim { domain }, None),
                IdResult::LowestClaim(Some(i64::MIN))
            );
            let mutations = f.bank.mutations.load(Ordering::Acquire);
            drop(a);
            assert_eq!(
                policy(&f, IdAction::LowestLiveSequenceClaim { domain }, None),
                IdResult::LowestClaim(None)
            );
            assert_eq!(
                f.bank.mutations.load(Ordering::Acquire),
                mutations,
                "read-only watermark never journals cleanup"
            );
        }
    }

    #[test]
    fn id_policy_preserves_negative_row_start_and_autoincrement_exhaustion() {
        use super::super::policy::IdAction;
        let f = writable_fixture();
        let a = registration(&f);
        let domain = journal::Domain {
            generation: 1,
            table: -2,
            sequence: false,
        };
        assert_eq!(
            reserved(policy(
                &f,
                IdAction::ReserveRow {
                    owner: owner(&a),
                    domain,
                    autoincrement: false
                },
                Some(-3)
            )),
            (1, -2)
        );
        let before = f.bank.mutations.load(Ordering::Acquire);
        let mut operation = f
            .files
            .start(
                f.authority.try_acquire().unwrap(),
                f.identity,
                Box::new(Oracle {
                    identity: f.identity,
                    maximum: Some(i64::MAX),
                }),
                Arc::new(crate::MemoryIO::new()),
                IdAction::ReserveRow {
                    owner: owner(&a),
                    domain,
                    autoincrement: true,
                },
            )
            .unwrap();
        assert!(matches!(operation.poll(), Err(LimboError::DatabaseFull)));
        assert_eq!(f.bank.mutations.load(Ordering::Acquire), before);
    }

    #[test]
    fn id_policy_rejects_stale_oracle_and_unregistered_owner_before_mutation() {
        use super::super::policy::IdAction;
        for stale in [false, true] {
            let f = writable_fixture();
            let a = registration(&f);
            let action = IdAction::ReserveLogicalTable { owner: owner(&a) };
            let mut identity = f.identity;
            if stale {
                identity.database.generation += 1;
            } else {
                drop(a);
            }
            let result = f.files.start(
                f.authority.try_acquire().unwrap(),
                f.identity,
                Box::new(Oracle {
                    identity,
                    maximum: None,
                }),
                Arc::new(crate::MemoryIO::new()),
                action,
            );
            if stale {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().poll().is_err());
            }
            assert_eq!(f.bank.mutations.load(Ordering::Acquire), 0);
        }
    }

    struct FloorsOracle(RecoveryIdentity);
    impl super::super::policy::CommittedIdOracle for FloorsOracle {
        fn identity(&self) -> RecoveryIdentity {
            self.0
        }
        fn poll(
            &mut self,
            _: &AuthorityGuard,
            query: &super::super::policy::IdQuery,
        ) -> Result<IOResult<super::super::policy::IdFact>> {
            use super::super::policy::{IdFact, IdQuery};
            Ok(IOResult::Done(match query {
                IdQuery::RowFacts { .. } => IdFact::RowFacts {
                    maximum: Some(10),
                    autoincrement_floor: 100,
                },
                IdQuery::LogicalFloor => IdFact::LogicalFloor(-1000),
                IdQuery::RowExists { .. } => panic!("no random fallback below exhaustion"),
            }))
        }
    }
    #[test]
    fn id_policy_committed_autoincrement_and_logical_floors_override_smaller_live_state() {
        use super::super::policy::IdAction;
        let f = writable_fixture();
        let a = registration(&f);
        let domain = journal::Domain {
            generation: 1,
            table: -2,
            sequence: false,
        };
        for (action, expected) in [
            (
                IdAction::ReserveRow {
                    owner: owner(&a),
                    domain,
                    autoincrement: true,
                },
                (1, 101),
            ),
            (
                IdAction::ReserveLogicalTable { owner: owner(&a) },
                (2, -1001),
            ),
        ] {
            let mut operation = f
                .files
                .start(
                    f.authority.try_acquire().unwrap(),
                    f.identity,
                    Box::new(FloorsOracle(f.identity)),
                    Arc::new(crate::MemoryIO::new()),
                    action,
                )
                .unwrap();
            let IOResult::Done(result) = operation.poll().unwrap() else {
                panic!("synchronous floor oracle");
            };
            assert_eq!(reserved(result.result), expected);
            result.guard.finish().unwrap();
        }
    }

    struct FixedRandomIo {
        inner: crate::MemoryIO,
        calls: AtomicUsize,
    }
    impl crate::io::Clock for FixedRandomIo {
        fn current_time_monotonic(&self) -> crate::MonotonicInstant {
            self.inner.current_time_monotonic()
        }
        fn current_time_wall_clock(&self) -> crate::WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }
    impl crate::IO for FixedRandomIo {
        fn open_file(
            &self,
            path: &str,
            flags: crate::OpenFlags,
            direct: bool,
        ) -> Result<Arc<dyn File>> {
            self.inner.open_file(path, flags, direct)
        }
        fn remove_file(&self, path: &str) -> Result<()> {
            self.inner.remove_file(path)
        }
        fn step(&self) -> Result<()> {
            self.inner.step()
        }
        fn generate_random_number(&self) -> i64 {
            self.calls.fetch_add(1, Ordering::AcqRel);
            0
        }
    }
    #[test]
    fn id_policy_random_fallback_checks_live_claims_and_stops_after_100_attempts() {
        use super::super::policy::IdAction;
        for collision in [false, true] {
            let f = writable_fixture();
            let a = registration(&f);
            let b = registration(&f);
            let domain = journal::Domain {
                generation: 1,
                table: -2,
                sequence: false,
            };
            if collision {
                policy(
                    &f,
                    IdAction::ClaimExplicitRow {
                        owner: owner(&a),
                        domain,
                        value: 1,
                    },
                    None,
                );
            }
            let io = Arc::new(FixedRandomIo {
                inner: crate::MemoryIO::new(),
                calls: AtomicUsize::new(0),
            });
            let mut operation = f
                .files
                .start(
                    f.authority.try_acquire().unwrap(),
                    f.identity,
                    Box::new(Oracle {
                        identity: f.identity,
                        maximum: Some(i64::MAX),
                    }),
                    io.clone(),
                    IdAction::ReserveRow {
                        owner: owner(&b),
                        domain,
                        autoincrement: false,
                    },
                )
                .unwrap();
            let result = operation.poll();
            if collision {
                assert!(matches!(result, Err(LimboError::DatabaseFull)));
                assert_eq!(io.calls.load(Ordering::Acquire), 100);
            } else {
                let IOResult::Done(result) = result.unwrap() else {
                    panic!("synchronous fallback");
                };
                assert_eq!(reserved(result.result), (1, 1));
                assert_eq!(io.calls.load(Ordering::Acquire), 1);
                result.guard.finish().unwrap();
            }
        }
    }

    struct DelayedOracle {
        identity: RecoveryIdentity,
        file: Arc<TestFile>,
        completion: Option<Completion>,
    }
    impl super::super::policy::CommittedIdOracle for DelayedOracle {
        fn identity(&self) -> RecoveryIdentity {
            self.identity
        }
        fn poll(
            &mut self,
            guard: &AuthorityGuard,
            _: &super::super::policy::IdQuery,
        ) -> Result<IOResult<super::super::policy::IdFact>> {
            if self.completion.is_none() {
                let pin = PublicationLease::pin(&guard.io_lease())?;
                let completion =
                    Completion::new_read(Arc::new(Buffer::new_temporary(1)), move |_| {
                        let _pin = &pin;
                        None
                    });
                self.completion = Some(self.file.pread(0, completion)?);
            }
            let completion = self.completion.as_ref().unwrap();
            if !completion.finished() {
                return Ok(IOResult::IO(IOCompletions(completion.clone())));
            }
            if let Some(error) = completion.get_error() {
                return Err(error.into());
            }
            // Drop the caller's completed reference; the backend can still retain its callback.
            self.completion.take();
            Ok(IOResult::Done(super::super::policy::IdFact::RowFacts {
                maximum: None,
                autoincrement_floor: 0,
            }))
        }
    }
    #[test]
    fn id_policy_oracle_reads_hold_authority_until_backend_retirement() {
        use super::super::policy::IdAction;
        for cancel in [false, true] {
            let f = writable_fixture();
            let a = registration(&f);
            let oracle_file = TestFile::new(90, vec![1]);
            oracle_file.delayed.store(true, Ordering::Release);
            let domain = journal::Domain {
                generation: 1,
                table: -2,
                sequence: false,
            };
            let mut operation = f
                .files
                .start(
                    f.authority.try_acquire().unwrap(),
                    f.identity,
                    Box::new(DelayedOracle {
                        identity: f.identity,
                        file: oracle_file.clone(),
                        completion: None,
                    }),
                    Arc::new(crate::MemoryIO::new()),
                    IdAction::ReserveRow {
                        owner: owner(&a),
                        domain,
                        autoincrement: false,
                    },
                )
                .unwrap();
            let IOResult::IO(waiter) = operation.poll().unwrap() else {
                panic!("oracle read pending");
            };
            if cancel {
                waiter.0.abort();
                assert!(operation.poll().is_err());
                drop(operation);
                drop(waiter);
                assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
                oracle_file.complete_pending(true);
                assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
                oracle_file.retire();
                let mut guard = f.authority.try_acquire().unwrap();
                guard.recover(PublishedPrefix::default()).unwrap();
                guard.finish().unwrap();
            } else {
                oracle_file.complete_pending(true);
                drop(waiter);
                let IOResult::IO(retirement) = operation.poll().unwrap() else {
                    panic!("oracle backend still owns callback");
                };
                assert!(!retirement.0.finished());
                assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
                oracle_file.retire();
                let IOResult::Done(result) = operation.poll().unwrap() else {
                    panic!("oracle retired");
                };
                assert_eq!(reserved(result.result), (1, 1));
                result.guard.finish().unwrap();
            }
        }
    }

    #[test]
    fn retained_id_bytes_include_both_physical_tails_under_owned_guard() {
        let f = fixture();
        f.bank.bytes.lock().extend_from_slice(&[0; PAGE_SIZE]);
        f.unused.bytes.lock().resize(2 * PAGE_SIZE, 0);
        let loaded = loaded(&f);
        assert_eq!(loaded.retained_bytes().unwrap(), (7 * PAGE_SIZE) as u64);
        assert_eq!(
            loaded.required_budget_bytes().unwrap(),
            (7 * PAGE_SIZE) as u64
        );
        loaded.into_guard().finish().unwrap();
    }

    #[test]
    fn id_append_preserves_floor_snapshot_headroom_at_a_fixed_budget() {
        let mut f = writable_fixture();
        Arc::get_mut(&mut f.files).unwrap().history_budget = (8 * PAGE_SIZE) as u64;
        let mut current = loaded(&f);
        for floor in [-200, -300] {
            let mut append = current
                .append(journal::Change::LogicalFloor(floor))
                .unwrap();
            let IOResult::Done(next) = append.poll().unwrap() else {
                panic!("synchronous fixture");
            };
            current = next;
        }
        let before = f.bank.mutations.load(Ordering::Acquire);
        assert!(
            matches!(
                current.append(journal::Change::LogicalFloor(-400)),
                Err(LimboError::Busy)
            ),
            "append must leave capacity for the replacement floor snapshot"
        );
        assert_eq!(f.bank.mutations.load(Ordering::Acquire), before);
        let physical =
            f.bank.size().unwrap() + f.unused.size().unwrap() + f.selector.size().unwrap();
        assert_eq!(f.files.history_budget - physical, (2 * PAGE_SIZE) as u64);
    }

    #[test]
    fn id_compaction_reclaims_space_without_reusing_the_permanent_floor() {
        let mut f = writable_fixture();
        Arc::get_mut(&mut f.files).unwrap().history_budget = (8 * PAGE_SIZE) as u64;
        let mut current = loaded(&f);
        for floor in [-200, -300] {
            let mut append = current
                .append(journal::Change::LogicalFloor(floor))
                .unwrap();
            let IOResult::Done(next) = append.poll().unwrap() else {
                panic!("synchronous fixture");
            };
            current = next;
        }
        let mut compact = current.compact_floor_only(f.identity, -250).unwrap();
        let IOResult::Done(current) = compact.poll().unwrap() else {
            panic!("synchronous compactor");
        };
        assert_eq!(current.state.logical_floor, -300);
        assert_eq!(current.state.last_record, 1);
        assert_eq!(current.retained_bytes().unwrap(), INITIAL_RETAINED_BYTES);
        assert_eq!(
            current.required_budget_bytes().unwrap(),
            MIN_ID_BUDGET_BYTES
        );
        assert_eq!(f.bank.size().unwrap(), 0);
        let mut append = current.append(journal::Change::LogicalFloor(-400)).unwrap();
        let IOResult::Done(current) = append.poll().unwrap() else {
            panic!("capacity reclaimed");
        };
        current.into_guard().finish().unwrap();
        let current = loaded(&f);
        assert_eq!(current.state.logical_floor, -400);
        current.into_guard().finish().unwrap();
    }

    fn compaction_file(f: &Fixture, step: usize) -> &Arc<TestFile> {
        match step {
            0..=3 | 8..=11 => &f.selector,
            4 | 12..=13 => &f.bank,
            5..=7 => &f.unused,
            _ => unreachable!(),
        }
    }
    #[test]
    fn id_compaction_rejects_live_readers_stale_identity_and_invalid_floor_before_writes() {
        for case in 0..4 {
            let f = writable_fixture();
            let _reader = (case == 0).then(|| registration(&f));
            if case == 3 {
                let mut selector = IdSelector::decode(&f.selector.bytes.lock()[..PAGE_SIZE])
                    .unwrap()
                    .unwrap();
                selector.sequence = u64::MAX;
                let page = selector.encode().unwrap();
                let mut bytes = f.selector.bytes.lock();
                bytes[..PAGE_SIZE].copy_from_slice(&page);
                bytes[PAGE_SIZE..].copy_from_slice(&page);
            }
            let mut identity = f.identity;
            if case == 1 {
                identity.database.generation += 1;
            }
            let result = loaded(&f).compact_floor_only(identity, if case == 2 { 0 } else { -200 });
            if case == 0 {
                assert!(matches!(result, Err(LimboError::Busy)));
            } else {
                assert!(result.is_err());
            }
            for file in [&f.bank, &f.unused, &f.selector] {
                assert_eq!(file.mutations.load(Ordering::Acquire), 0);
            }
        }
    }
    #[test]
    fn id_compaction_failure_boundaries_preserve_a_complete_durable_floor() {
        for fail_at in 0..14 {
            let f = writable_fixture();
            let step = Arc::new(AtomicUsize::new(0));
            for file in [&f.bank, &f.unused, &f.selector] {
                *file.fault.lock() = Some((step.clone(), fail_at));
            }
            let mut compact = loaded(&f).compact_floor_only(f.identity, -200).unwrap();
            assert!(compact.poll().is_err(), "failure boundary {fail_at}");
            assert!(compact.poll().is_err());
            drop(compact);
            let slots = f.selector.durable.lock();
            let old = f.bank.durable.lock();
            let next = f.unused.durable.lock();
            let selected = select(
                [&slots[..PAGE_SIZE], &slots[PAGE_SIZE..]],
                [&old, &next],
                f.files.database_id,
                f.identity.database,
            )
            .unwrap();
            assert_eq!(
                replay_selected(selected, [&old, &next])
                    .unwrap()
                    .logical_floor,
                if fail_at >= 10 { -200 } else { -100 },
                "failure boundary {fail_at}"
            );
        }
    }
    #[test]
    fn id_compaction_waits_for_every_mutation_and_all_backend_callbacks() {
        let f = writable_fixture();
        for file in [&f.bank, &f.unused, &f.selector] {
            file.delay_mutations.store(true, Ordering::Release);
        }
        let mut compact = loaded(&f).compact_floor_only(f.identity, -200).unwrap();
        for step in 0..14 {
            let IOResult::IO(waiter) = compact.poll().unwrap() else {
                panic!("step {step} must wait");
            };
            assert!(!waiter.0.finished());
            assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
            compaction_file(&f, step).complete_mutation(true);
        }
        let IOResult::IO(waiter) = compact.poll().unwrap() else {
            panic!("retirement must wait");
        };
        assert!(!waiter.0.finished());
        f.bank.retire();
        f.unused.retire();
        assert!(!waiter.0.finished());
        f.selector.retire();
        assert!(waiter.0.finished());
        let IOResult::Done(result) = compact.poll().unwrap() else {
            panic!("all callbacks retired");
        };
        result.into_guard().finish().unwrap();
    }
    #[test]
    fn id_compaction_abort_at_each_mutation_keeps_authority_until_retirement() {
        for cancel_at in 0..14 {
            let f = writable_fixture();
            for file in [&f.bank, &f.unused, &f.selector] {
                file.delay_mutations.store(true, Ordering::Release);
            }
            let mut compact = loaded(&f).compact_floor_only(f.identity, -200).unwrap();
            for step in 0..=cancel_at {
                let IOResult::IO(waiter) = compact.poll().unwrap() else {
                    panic!("step {step} must wait");
                };
                let file = compaction_file(&f, step);
                if step == cancel_at {
                    waiter.0.abort();
                    assert!(compact.poll().is_err());
                    drop(compact);
                    assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
                    file.complete_mutation(true);
                    assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
                    file.retire();
                    let mut guard = f.authority.try_acquire().unwrap();
                    assert!(guard.requires_recovery());
                    guard.recover(PublishedPrefix::default()).unwrap();
                    guard.finish().unwrap();
                    break;
                }
                file.complete_mutation(false);
            }
        }
    }
    #[test]
    fn id_compaction_repeatedly_bounds_bytes_and_retains_inode_identities() {
        let mut f = writable_fixture();
        Arc::get_mut(&mut f.files).unwrap().history_budget = (8 * PAGE_SIZE) as u64;
        let ids = [
            f.bank.file_id().unwrap(),
            f.unused.file_id().unwrap(),
            f.selector.file_id().unwrap(),
        ];
        let mut current = loaded(&f);
        let mut floor = -100;
        for _ in 0..4 {
            for _ in 0..2 {
                floor -= 1;
                let mut append = current
                    .append(journal::Change::LogicalFloor(floor))
                    .unwrap();
                let IOResult::Done(next) = append.poll().unwrap() else {
                    panic!("synchronous append");
                };
                current = next;
            }
            let mut compact = current.compact_floor_only(f.identity, -1).unwrap();
            let IOResult::Done(next) = compact.poll().unwrap() else {
                panic!("synchronous compactor");
            };
            current = next;
            assert_eq!(current.state.logical_floor, floor);
            assert_eq!(current.retained_bytes().unwrap(), INITIAL_RETAINED_BYTES);
            assert_eq!(
                ids,
                [
                    f.bank.file_id().unwrap(),
                    f.unused.file_id().unwrap(),
                    f.selector.file_id().unwrap()
                ]
            );
        }
        current.into_guard().finish().unwrap();
    }

    #[test]
    fn id_compaction_syncs_both_current_witnesses_before_reusing_the_other_bank() {
        let f = writable_fixture();
        let previous = f.bank.bytes.lock().clone();
        *f.unused.bytes.lock() = previous.clone();
        *f.unused.durable.lock() = previous;
        let mut current = IdSelector::decode(&f.selector.bytes.lock()[..PAGE_SIZE])
            .unwrap()
            .unwrap();
        let mut old = current;
        old.active.bank = 1;
        current.sequence = 2;
        current.active.bank_generation = 2;
        f.bank.bytes.lock()[..PAGE_SIZE].copy_from_slice(
            &BankHeader {
                database_id: f.files.database_id,
                bank_generation: 2,
                database: f.identity.database,
            }
            .encode()
            .unwrap(),
        );
        current.active.running_crc = crc32c::crc32c(&f.bank.bytes.lock());
        *f.bank.durable.lock() = f.bank.bytes.lock().clone();
        {
            let mut slots = f.selector.bytes.lock();
            slots[..PAGE_SIZE].copy_from_slice(&current.encode().unwrap());
            slots[PAGE_SIZE..].copy_from_slice(&old.encode().unwrap());
            *f.selector.durable.lock() = slots.clone();
        }
        for file in [&f.bank, &f.unused, &f.selector] {
            file.delay_mutations.store(true, Ordering::Release);
        }
        let mut compact = loaded(&f).compact_floor_only(f.identity, -200).unwrap();
        let mut stopped_before_successor = false;
        for _ in 0..20 {
            assert!(matches!(compact.poll().unwrap(), IOResult::IO(_)));
            let file = [&f.bank, &f.unused, &f.selector]
                .into_iter()
                .find(|file| file.pending_mutation.lock().is_some())
                .unwrap();
            let successor_write = match file.pending_mutation.lock().as_ref().unwrap() {
                (Mutation::Write(0, bytes), _) if Arc::ptr_eq(file, &f.selector) => {
                    IdSelector::decode(bytes.as_slice())
                        .unwrap()
                        .unwrap()
                        .sequence
                        == 3
                }
                _ => false,
            };
            if successor_write {
                let (_, completion) = file.pending_mutation.lock().take().unwrap();
                completion.error(crate::error::CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "stop before successor selector",
                ));
                assert!(compact.poll().is_err());
                stopped_before_successor = true;
                break;
            }
            file.complete_mutation(false);
        }
        assert!(stopped_before_successor);
        drop(compact);
        let mut slots = f.selector.durable.lock().clone();
        slots[0] ^= 1;
        let a = f.bank.durable.lock();
        let b = f.unused.durable.lock();
        let selected = select(
            [&slots[..PAGE_SIZE], &slots[PAGE_SIZE..]],
            [&a, &b],
            f.files.database_id,
            f.identity.database,
        )
        .unwrap();
        assert_eq!(selected.selector.sequence, 2);
        assert_eq!(
            replay_selected(selected, [&a, &b]).unwrap().logical_floor,
            -100
        );
    }

    #[test]
    fn id_witness_proof_waits_for_both_syncs_and_backend_retirement() {
        let f = writable_fixture();
        let owner = journal::Owner {
            epoch: 91,
            begin: 3,
        };
        let mut current = loaded(&f);
        for change in [
            journal::Change::Claim {
                owner,
                ordinal: 1,
                claim: journal::Claim {
                    key: journal::ClaimKey::LogicalTable,
                    value: -200,
                },
            },
            journal::Change::ReleaseOwner { owner },
        ] {
            let mut append = current.append(change).unwrap();
            let IOResult::Done(next) = append.poll().unwrap() else {
                panic!("synchronous append");
            };
            current = next;
        }
        let bank_mutations = f.bank.mutations.load(Ordering::Acquire);
        f.selector.delay_mutations.store(true, Ordering::Release);
        let mut mirror = current.mirror_current_witness().unwrap();
        for _ in 0..4 {
            let IOResult::IO(waiter) = mirror.poll().unwrap() else {
                panic!("mirror mutation must wait");
            };
            assert!(!waiter.0.finished());
            f.selector.complete_mutation(true);
        }
        let IOResult::IO(waiter) = mirror.poll().unwrap() else {
            panic!("mirror callbacks remain live");
        };
        assert!(!waiter.0.finished());
        assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
        f.selector.retire();
        let IOResult::Done(proof) = mirror.poll().unwrap() else {
            panic!("mirror callbacks retired");
        };
        let (guard, max_epoch) = proof.into_guard_and_max_owner_epoch();
        assert_eq!(max_epoch, Some(91));
        assert_eq!(f.bank.mutations.load(Ordering::Acquire), bank_mutations);
        guard.finish().unwrap();
    }

    fn retired_epoch_journal(f: &Fixture) -> LoadedIds {
        let owner = journal::Owner {
            epoch: 91,
            begin: 3,
        };
        let mut current = loaded(f);
        for change in [
            journal::Change::Claim {
                owner,
                ordinal: 1,
                claim: journal::Claim {
                    key: journal::ClaimKey::LogicalTable,
                    value: -200,
                },
            },
            journal::Change::ReleaseOwner { owner },
        ] {
            let mut append = current.append(change).unwrap();
            let IOResult::Done(next) = append.poll().unwrap() else {
                panic!("synchronous append");
            };
            current = next;
        }
        current
    }
    #[test]
    fn id_witness_proof_covers_owner_epochs_in_both_active_and_prepared_banks() {
        let mut f = writable_fixture();
        retired_epoch_journal(&f).into_guard().finish().unwrap();
        let target = DatabaseGeneration {
            generation: 2,
            checkpoint_timestamp: 0,
        };
        let mut bytes = InitialIds::new(f.files.database_id, target, -500)
            .unwrap()
            .bank_a;
        bytes[..PAGE_SIZE].copy_from_slice(
            &BankHeader {
                database_id: f.files.database_id,
                database: target,
                bank_generation: 2,
            }
            .encode()
            .unwrap(),
        );
        let prepared = BankRef {
            bank: 1,
            bank_generation: 2,
            database: target,
            end: bytes.len() as u64,
            snapshot_end: bytes.len() as u64,
            running_crc: crc32c::crc32c(&bytes),
        };
        *f.unused.bytes.lock() = bytes;
        let mut selector = IdSelector::decode(&f.selector.bytes.lock()[..PAGE_SIZE])
            .unwrap()
            .unwrap();
        selector.sequence += 1;
        selector.prepared = Some(prepared);
        let encoded = selector.encode().unwrap();
        f.selector.bytes.lock()[..PAGE_SIZE].copy_from_slice(&encoded);
        f.selector.bytes.lock()[PAGE_SIZE..].copy_from_slice(&encoded);
        f.identity.database = target;
        let current = loaded(&f);
        assert_eq!(current.state.max_owner_epoch(), None);
        let mut mirror = current.mirror_current_witness().unwrap();
        let IOResult::Done(proof) = mirror.poll().unwrap() else {
            panic!("synchronous witness mirror");
        };
        let (guard, epoch) = proof.into_guard_and_max_owner_epoch();
        assert_eq!(
            epoch,
            Some(91),
            "still-referenced active bank retains retired epochs"
        );
        assert_eq!(
            IdSelector::decode(&f.selector.durable.lock()[..PAGE_SIZE])
                .unwrap()
                .unwrap(),
            selector
        );
        guard.finish().unwrap();
    }
    #[test]
    fn id_witness_proof_prevents_fallback_to_forgotten_owner_epochs() {
        let f = writable_fixture();
        let current = retired_epoch_journal(&f);
        let step = Arc::new(AtomicUsize::new(0));
        for file in [&f.bank, &f.unused, &f.selector] {
            *file.fault.lock() = Some((step.clone(), 10));
        }
        let mut compact = current.compact_floor_only(f.identity, -1).unwrap();
        assert!(compact.poll().is_err());
        drop(compact);
        for file in [&f.bank, &f.unused, &f.selector] {
            *file.fault.lock() = None;
        }
        let mut guard = f.authority.try_acquire().unwrap();
        guard.recover(f.identity.prefix).unwrap();
        let mut load = f.files.load(guard, f.identity).unwrap();
        let IOResult::Done(current) = load.poll().unwrap() else {
            panic!("synchronous load");
        };
        assert_eq!(current.state.max_owner_epoch(), None);
        let mut mirror = current.mirror_current_witness().unwrap();
        let IOResult::Done(proof) = mirror.poll().unwrap() else {
            panic!("synchronous witness mirror");
        };
        let (guard, epoch) = proof.into_guard_and_max_owner_epoch();
        assert_eq!(epoch, None);
        let mut slots = f.selector.durable.lock().clone();
        slots[0] ^= 1;
        let a = f.bank.durable.lock();
        let b = f.unused.durable.lock();
        let selected = select(
            [&slots[..PAGE_SIZE], &slots[PAGE_SIZE..]],
            [&a, &b],
            f.files.database_id,
            f.identity.database,
        )
        .unwrap();
        let state = replay_selected(selected, [&a, &b]).unwrap();
        assert_eq!(
            state.max_owner_epoch(),
            None,
            "older retired owner cannot become selectable after readiness"
        );
        assert_eq!(state.logical_floor, -200);
        guard.finish().unwrap();
    }
    #[test]
    fn id_witness_abort_never_releases_authority_before_backend_retirement() {
        for cancel_at in 0..4 {
            let f = fixture_with_authority(authority_for_test_unreconciled(TestFile::new(
                100,
                Vec::new(),
            )));
            f.selector.writable.store(true, Ordering::Release);
            let guard = f.authority.try_acquire().unwrap();
            assert!(!guard.incarnation_ready().unwrap());
            guard.finish().unwrap();
            f.selector.delay_mutations.store(true, Ordering::Release);
            let mut mirror = loaded(&f).mirror_current_witness().unwrap();
            for step in 0..=cancel_at {
                let IOResult::IO(waiter) = mirror.poll().unwrap() else {
                    panic!("mirror mutation pending");
                };
                if step == cancel_at {
                    waiter.0.abort();
                    assert!(mirror.poll().is_err());
                    drop(mirror);
                    assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
                    f.selector.complete_mutation(true);
                    assert!(matches!(f.authority.try_acquire(), Err(LimboError::Busy)));
                    f.selector.retire();
                    let mut guard = f.authority.try_acquire().unwrap();
                    guard.recover(f.identity.prefix).unwrap();
                    assert!(!guard.incarnation_ready().unwrap());
                    assert!(guard.register_snapshot().is_err());
                    guard.finish().unwrap();
                    break;
                }
                f.selector.complete_mutation(false);
            }
        }
    }

    struct RetirementIo {
        inner: crate::MemoryIO,
        backend: Arc<TestFile>,
        steps: AtomicUsize,
    }
    impl crate::io::Clock for RetirementIo {
        fn current_time_monotonic(&self) -> crate::MonotonicInstant {
            self.inner.current_time_monotonic()
        }
        fn current_time_wall_clock(&self) -> crate::WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }
    impl crate::IO for RetirementIo {
        fn open_file(
            &self,
            path: &str,
            flags: crate::OpenFlags,
            direct: bool,
        ) -> Result<Arc<dyn File>> {
            self.inner.open_file(path, flags, direct)
        }
        fn remove_file(&self, path: &str) -> Result<()> {
            self.inner.remove_file(path)
        }
        fn step(&self) -> Result<()> {
            self.steps.fetch_add(1, Ordering::AcqRel);
            self.backend.retire();
            Ok(())
        }
    }
    #[test]
    fn id_load_retirement_wait_drives_step_only_backend() {
        use crate::IO;
        let f = fixture();
        f.selector.delayed.store(true, Ordering::Release);
        let mut load = f
            .files
            .load(f.authority.try_acquire().unwrap(), f.identity)
            .unwrap();
        assert!(matches!(load.poll().unwrap(), IOResult::IO(_)));
        f.selector.complete_pending(true);
        assert!(matches!(load.poll().unwrap(), IOResult::IO(_)));
        f.selector.complete_pending(true);
        let IOResult::IO(waiter) = load.poll().unwrap() else {
            panic!("retirement must wait");
        };
        assert!(
            !waiter.0.finished(),
            "finished yields cannot drive backend retirement"
        );
        let io = RetirementIo {
            inner: crate::MemoryIO::new(),
            backend: f.selector,
            steps: AtomicUsize::new(0),
        };
        io.wait_for_completion(waiter.0).unwrap();
        assert_eq!(io.steps.load(Ordering::Acquire), 1);
        let IOResult::Done(loaded) = load.poll().unwrap() else {
            panic!("backend retirement must unblock load");
        };
        loaded.guard.finish().unwrap();
    }
}
