use super::*;

impl LoadedIds {
    /// The coordinator pauses admission while waiting for quiescence. This operation
    /// continuously owns publication authority, so no new registration can enter during I/O.
    /// The supplied floor must come from the committed oracle at exactly `identity`.
    pub(in crate::mvcc) fn compact_floor_only(
        mut self,
        identity: RecoveryIdentity,
        committed_floor: i64,
    ) -> Result<IdCompaction> {
        use crate::io::File;
        if identity != self.identity
            || self.guard.published()? != identity.prefix
            || self.selection.selector.prepared.is_some()
            || self.selection.selected != self.selection.selector.active
        {
            return Err(invalid(
                "ID compaction requires the current finalized generation",
            ));
        }
        if !self.guard.live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        if committed_floor >= 0 || committed_floor == i64::MIN {
            return Err(invalid("invalid committed logical ID floor"));
        }
        let old = self.selection.selected;
        let old_bank = usize::from(old.bank);
        let next_bank = 1 - old_bank;
        let bank_generation = old
            .bank_generation
            .checked_add(1)
            .ok_or(LimboError::DatabaseFull)?;
        let sequence = self
            .selection
            .selector
            .sequence
            .checked_add(1)
            .ok_or(LimboError::DatabaseFull)?;
        let floor = self.state.logical_floor.min(committed_floor);
        let header = BankHeader {
            database_id: self.files.database_id,
            bank_generation,
            database: identity.database,
        }
        .encode()?;
        let record = journal::Record {
            sequence: 1,
            change: journal::Change::LogicalFloor(floor),
        }
        .encode()?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(2 * PAGE_SIZE)?;
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&record);
        let peak = old
            .end
            .checked_add(bytes.len() as u64)
            .and_then(|total| total.checked_add(identity.prefix.end_offset))
            .and_then(|total| total.checked_add((2 * PAGE_SIZE) as u64))
            .ok_or(LimboError::TooBig)?;
        if self.files.history_budget < MIN_ID_BUDGET_BYTES {
            return Err(LimboError::TooBig);
        }
        if peak > self.files.history_budget {
            return Err(LimboError::Busy);
        }
        if self.files.banks[old_bank].size()? < old.end {
            return Err(corrupt("selected ID bank became truncated"));
        }
        let selected = BankRef {
            bank: next_bank as u8,
            bank_generation,
            database: identity.database,
            end: bytes.len() as u64,
            snapshot_end: bytes.len() as u64,
            running_crc: crc32c::crc32c(&bytes),
        };
        let next = IdSelector {
            sequence,
            database_id: self.files.database_id,
            active: selected,
            prepared: None,
        };
        let current_selector = encoded_buffer(&self.selection.selector.encode()?);
        let next_selector = encoded_buffer(&next.encode()?);
        let snapshot = encoded_buffer(&bytes);
        self.state = journal::State::replay_snapshot(
            &bytes[PAGE_SIZE..],
            identity.database.generation,
            PAGE_SIZE,
        )?;
        self.banks[next_bank] = bytes;
        self.selection = SelectedIds {
            selector: next,
            selected,
            slots_agree: true,
        };
        Ok(IdCompaction {
            loaded: Some(self),
            current_selector,
            next_selector,
            snapshot,
            old_bank,
            old_end: old.end,
            step: 0,
            pending: None,
            finished: false,
        })
    }
}

pub(in crate::mvcc) struct IdCompaction {
    loaded: Option<LoadedIds>,
    current_selector: Arc<Buffer>,
    next_selector: Arc<Buffer>,
    snapshot: Arc<Buffer>,
    old_bank: usize,
    old_end: u64,
    step: u8,
    pending: Option<Completion>,
    finished: bool,
}

impl IdCompaction {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<LoadedIds>> {
        use crate::io::{File, FileSyncType};
        if self.finished {
            return Err(invalid("ID compaction already terminated"));
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
            let old = &loaded.files.banks[self.old_bank];
            let next = &loaded.files.banks[1 - self.old_bank];
            let selector = &loaded.files.selector;
            let completion = match self.step {
                // Matching read bytes do not prove completed synchronization. Establish it afresh
                // before overwriting a bank that an older selector copy could still reference.
                0 => selector.pwrite(
                    0,
                    self.current_selector.clone(),
                    Completion::new_write(|_| {}),
                )?,
                1 => selector.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                2 => selector.pwrite(
                    PAGE_SIZE as u64,
                    self.current_selector.clone(),
                    Completion::new_write(|_| {}),
                )?,
                3 => selector.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                4 => old.truncate(self.old_end, Completion::new_trunc(|_| {}))?,
                5 => next.truncate(0, Completion::new_trunc(|_| {}))?,
                6 => next.pwrite(0, self.snapshot.clone(), Completion::new_write(|_| {}))?,
                7 => next.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                8 => {
                    selector.pwrite(0, self.next_selector.clone(), Completion::new_write(|_| {}))?
                }
                9 => selector.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                10 => selector.pwrite(
                    PAGE_SIZE as u64,
                    self.next_selector.clone(),
                    Completion::new_write(|_| {}),
                )?,
                11 => selector.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                // Both durable selector copies now name the successor, so the old bank is reclaimable.
                12 => old.truncate(0, Completion::new_trunc(|_| {}))?,
                13 => old.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                14 => {
                    if !loaded.guard.io_retired() {
                        let completion = loaded.guard.retirement_completion();
                        self.finished = false;
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    let mut loaded = self.loaded.take().unwrap();
                    loaded.banks[self.old_bank] = Vec::new();
                    return Ok(IOResult::Done(loaded));
                }
                _ => unreachable!(),
            };
            self.step += 1;
            self.pending = Some(completion);
        }
    }
}

fn encoded_buffer(bytes: &[u8]) -> Arc<Buffer> {
    let buffer = Arc::new(Buffer::new_temporary(bytes.len()));
    buffer.as_mut_slice().copy_from_slice(bytes);
    buffer
}
