use super::*;
use crate::mvcc::guarded_file::{FileBinding, GuardedFile};
use crate::mvcc::shared_authority::{AuthorityGuard, PublicationLease, PublishedPrefix};
use crate::sync::{Arc, Mutex};
use crate::types::IOCompletions;
use crate::{Buffer, Completion, IOResult};
use std::sync::atomic::{AtomicI32, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RecoveryIdentity {
    pub database: DatabaseGeneration,
    pub prefix: PublishedPrefix,
}

pub(super) struct SharedIds {
    banks: [Arc<GuardedFile>; 2],
    selector: Arc<GuardedFile>,
    database_id: FileId,
    history_budget: u64,
}

impl SharedIds {
    /// The caller supplies files opened existing-only in the validated database identity
    /// namespace, all bound to that same authority, and the retained main file ID.
    /// Guard bindings verify authority association; they do not establish path provenance.
    pub(super) fn open_existing(
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

    pub(super) fn load(
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

pub(super) struct IdLoad {
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

pub(super) struct LoadedIds {
    files: Arc<SharedIds>,
    guard: AuthorityGuard,
    bindings: [FileBinding; 3],
    identity: RecoveryIdentity,
    selection: SelectedIds,
    state: journal::State,
    banks: [Vec<u8>; 2],
}

impl LoadedIds {
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
        let mut required = self
            .identity
            .prefix
            .end_offset
            .checked_add(self.files.selector.size()?)
            .ok_or(LimboError::TooBig)?;
        for index in 0..2 {
            let physical = self.files.banks[index].size()?;
            let size = if index == bank {
                physical.max(selector.active.end)
            } else {
                physical
            };
            required = required.checked_add(size).ok_or(LimboError::TooBig)?;
        }
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
    pub(super) fn poll(&mut self) -> Result<IOResult<LoadedIds>> {
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
    use crate::mvcc::shared_authority::{authority_for_test, SharedMvccAuthority};
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
                    *self.durable.lock() = self.bytes.lock().clone();
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
        let authority = authority_for_test(TestFile::new(100, Vec::new()));
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
            backend: f.selector.clone(),
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
