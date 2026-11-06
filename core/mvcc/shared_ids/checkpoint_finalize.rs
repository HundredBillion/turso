use super::*;
use crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof;

impl SuspendedPreparedIds {
    pub(in crate::mvcc) fn finalize(
        self,
        guard: AuthorityGuard,
        proof: CompletedCheckpointProof,
    ) -> Result<IdCheckpointFinalize> {
        let suspended = self.loaded;
        let target = proof.target_identity();
        let old = suspended.selection.selector.active;
        let prepared = suspended
            .selection
            .selector
            .prepared
            .ok_or_else(|| invalid("ID finalization requires a prepared generation"))?;
        let target_prefix = PublishedPrefix {
            committed_timestamp: target.database.checkpoint_timestamp,
            end_offset: 0,
            running_crc: 0,
        };
        if !crate::sync::Weak::ptr_eq(&suspended.lease, &guard.io_lease())
            || !proof.matches_lease(&guard)
            || proof.source_database() != suspended.identity.database
            || proof.source_database() != old.database
            || proof
                .source_identity()
                .is_some_and(|source| source != suspended.identity)
            || prepared.database != target.database
            || target.database.generation <= old.database.generation
            || target.prefix != target_prefix
            || guard.published()? != target.prefix
        {
            return Err(invalid(
                "ID finalization requires the completed target and original authority lease",
            ));
        }
        PublicationLease::pin(&guard.io_lease())?;
        if !guard.live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        let state = replay_selected(
            SelectedIds {
                selected: prepared,
                ..suspended.selection
            },
            [&suspended.banks[0], &suspended.banks[1]],
        )?;
        if proof.materialized_floor() >= 0
            || proof.materialized_floor() == i64::MIN
            || state.logical_floor > proof.materialized_floor()
            || state.logical_floor > suspended.state.logical_floor
        {
            return Err(invalid(
                "prepared ID floor does not cover the completed checkpoint",
            ));
        }
        let selector = IdSelector {
            sequence: suspended
                .selection
                .selector
                .sequence
                .checked_add(1)
                .ok_or(LimboError::DatabaseFull)?,
            database_id: suspended.files.database_id,
            active: prepared,
            prepared: None,
        };
        let bytes = encoded_buffer(&selector.encode()?);
        let loaded = LoadedIds {
            files: suspended.files,
            guard,
            bindings: suspended.bindings,
            identity: target,
            selection: SelectedIds {
                selector,
                selected: prepared,
                slots_agree: true,
            },
            state,
            banks: suspended.banks,
        };
        Ok(IdCheckpointFinalize {
            loaded: Some(loaded),
            proof: Some(proof),
            selector: bytes,
            old_bank: usize::from(old.bank),
            step: 0,
            pending: None,
            finished: false,
        })
    }
}

pub(in crate::mvcc) struct IdCheckpointFinalize {
    loaded: Option<LoadedIds>,
    proof: Option<CompletedCheckpointProof>,
    selector: Arc<Buffer>,
    old_bank: usize,
    step: u8,
    pending: Option<Completion>,
    finished: bool,
}

pub(in crate::mvcc) struct FinalizedIds {
    loaded: LoadedIds,
    proof: CompletedCheckpointProof,
}

impl FinalizedIds {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        &self.loaded.guard
    }
    pub(in crate::mvcc) fn proof(&self) -> &CompletedCheckpointProof {
        &self.proof
    }
    pub(in crate::mvcc) fn into_parts(self) -> (LoadedIds, CompletedCheckpointProof) {
        (self.loaded, self.proof)
    }
}

impl IdCheckpointFinalize {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<FinalizedIds>> {
        use crate::io::{File, FileSyncType};
        if self.finished {
            return Err(invalid("ID finalization already terminated"));
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
            let loaded = self.loaded.as_mut().unwrap();
            let completion = match self.step {
                0 => loaded.files.selector.pwrite(
                    0,
                    self.selector.clone(),
                    Completion::new_write(|_| {}),
                )?,
                1 => loaded
                    .files
                    .selector
                    .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                2 => loaded.files.selector.pwrite(
                    PAGE_SIZE as u64,
                    self.selector.clone(),
                    Completion::new_write(|_| {}),
                )?,
                3 => loaded
                    .files
                    .selector
                    .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                4 => {
                    loaded.files.banks[self.old_bank].truncate(0, Completion::new_trunc(|_| {}))?
                }
                5 => loaded.files.banks[self.old_bank]
                    .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                6 => {
                    if !loaded.guard.io_retired() {
                        let waiter = loaded.guard.retirement_completion();
                        self.finished = false;
                        return Ok(IOResult::IO(IOCompletions(waiter)));
                    }
                    loaded.banks[self.old_bank].clear();
                    return Ok(IOResult::Done(FinalizedIds {
                        loaded: self.loaded.take().unwrap(),
                        proof: self.proof.take().unwrap(),
                    }));
                }
                _ => unreachable!(),
            };
            self.step += 1;
            self.pending = Some(completion);
        }
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "checkpoint_finalize_tests.rs"]
mod tests;
