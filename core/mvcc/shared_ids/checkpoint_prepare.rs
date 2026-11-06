use super::*;

impl LoadedIds {
    /// The coordinator validates the materialized domain mapping and pauses admission.
    /// The floor comes from its committed oracle at this operation's held identity.
    /// Preparation does not prove materialization or authorize old-bank reclamation.
    pub(in crate::mvcc) fn prepare_checkpoint(
        mut self,
        target: DatabaseGeneration,
        committed_floor: i64,
    ) -> Result<IdCheckpointPrepare> {
        use crate::io::File;
        let identity = self.identity;
        if self.guard.published()? != identity.prefix
            || self.selection.selector.prepared.is_some()
            || self.selection.selected != self.selection.selector.active
        {
            return Err(invalid(
                "ID preparation requires the current finalized generation",
            ));
        }
        if target.generation <= identity.database.generation
            || target.checkpoint_timestamp != identity.prefix.committed_timestamp
            || target.checkpoint_timestamp < identity.database.checkpoint_timestamp
        {
            return Err(invalid(
                "ID checkpoint target differs from the selected commit boundary",
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
            database: target,
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
            database: target,
            end: bytes.len() as u64,
            snapshot_end: bytes.len() as u64,
            running_crc: crc32c::crc32c(&bytes),
        };
        let next = IdSelector {
            sequence,
            database_id: self.files.database_id,
            active: old,
            prepared: Some(selected),
        };
        let current_selector = encoded_buffer(&self.selection.selector.encode()?);
        let next_selector = encoded_buffer(&next.encode()?);
        let snapshot = encoded_buffer(&bytes);
        self.banks[next_bank] = bytes;
        self.selection = SelectedIds {
            selector: next,
            selected: old,
            slots_agree: true,
        };
        Ok(IdCheckpointPrepare {
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

pub(in crate::mvcc) struct IdCheckpointPrepare {
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

impl IdCheckpointPrepare {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<PreparedIds>> {
        use crate::io::{File, FileSyncType};
        if self.finished {
            return Err(invalid("ID checkpoint preparation already terminated"));
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
                12 => {
                    if !loaded.guard.io_retired() {
                        let completion = loaded.guard.retirement_completion();
                        self.finished = false;
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    return Ok(IOResult::Done(PreparedIds {
                        loaded: self.loaded.take().unwrap(),
                    }));
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

/// Holds both generation references and the continuous authority lease. A coordinator-owned
/// completed-checkpoint proof is required before a future finalizer may reclaim either bank.
pub(in crate::mvcc) struct PreparedIds {
    loaded: LoadedIds,
}
impl PreparedIds {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        &self.loaded.guard
    }

    pub(in crate::mvcc) fn source_identity(&self) -> RecoveryIdentity {
        self.loaded.identity
    }

    pub(in crate::mvcc) fn target(&self) -> DatabaseGeneration {
        self.loaded.selection.selector.prepared.unwrap().database
    }
}
