use super::*;
use crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof;

impl SuspendedPreparedIds {
    pub(in crate::mvcc) fn finalize(
        self,
        guard: AuthorityGuard,
        proof: CompletedCheckpointProof,
    ) -> Result<IdCheckpointFinalize> {
        let suspended = self.loaded;
        if !crate::sync::Weak::ptr_eq(&suspended.lease, &guard.io_lease()) {
            return Err(invalid("prepared IDs lost their original authority lease"));
        }
        let source = suspended.identity;
        let loaded = LoadedIds {
            files: suspended.files,
            guard,
            bindings: suspended.bindings,
            identity: suspended.identity,
            selection: suspended.selection,
            state: suspended.state,
            banks: suspended.banks,
        };
        begin_finalization(loaded, proof, Some(source))
    }
}

impl LoadedIds {
    pub(in crate::mvcc) fn finalize_recovered_checkpoint(
        self,
        proof: CompletedCheckpointProof,
    ) -> Result<IdCheckpointFinalize> {
        begin_finalization(self, proof, None)
    }
}

fn begin_finalization(
    mut loaded: LoadedIds,
    proof: CompletedCheckpointProof,
    live_source: Option<RecoveryIdentity>,
) -> Result<IdCheckpointFinalize> {
    let target = proof.target_identity();
    let old = loaded.selection.selector.active;
    let prepared = loaded.selection.selector.prepared;
    let selected = prepared.unwrap_or(old);
    let loaded_matches = match (live_source, prepared) {
        (Some(source), Some(_)) => {
            source == loaded.identity
                && source.database == old.database
                && loaded.selection.selected == old
        }
        (None, _) => loaded.identity == target && loaded.selection.selected == selected,
        (Some(_), None) => false,
    };
    let expected_source = prepared.map(|_| old.database);
    let target_prefix = PublishedPrefix {
        committed_timestamp: target.database.checkpoint_timestamp,
        end_offset: 0,
        running_crc: 0,
    };
    if !loaded_matches
        || proof.source_identity() != live_source
        || !proof.matches_lease(&loaded.guard)
        || proof.source_database() != expected_source
        || selected.database != target.database
        || prepared.is_some_and(|_| {
            target.database.generation <= old.database.generation
                || target.database.checkpoint_timestamp < old.database.checkpoint_timestamp
        })
        || target.prefix != target_prefix
        || loaded.guard.published()? != target.prefix
    {
        return Err(invalid(
            "ID finalization requires the completed target and original authority lease",
        ));
    }
    PublicationLease::pin(&loaded.guard.io_lease())?;
    if !loaded.guard.live_registrations()?.is_empty() {
        return Err(LimboError::Busy);
    }
    let old_floor = if prepared.is_some() {
        Some(
            replay_selected(
                SelectedIds {
                    selected: old,
                    ..loaded.selection
                },
                [&loaded.banks[0], &loaded.banks[1]],
            )?
            .logical_floor,
        )
    } else {
        None
    };
    let state = replay_selected(
        SelectedIds {
            selected,
            ..loaded.selection
        },
        [&loaded.banks[0], &loaded.banks[1]],
    )?;
    if proof.materialized_floor() >= 0
        || proof.materialized_floor() == i64::MIN
        || state.logical_floor > proof.materialized_floor()
        || old_floor.is_some_and(|floor| state.logical_floor > floor)
    {
        return Err(invalid(
            "prepared ID floor does not cover the completed checkpoint",
        ));
    }
    let sequence = if prepared.is_some() {
        loaded
            .selection
            .selector
            .sequence
            .checked_add(1)
            .ok_or(LimboError::DatabaseFull)?
    } else {
        loaded.selection.selector.sequence
    };
    let old_bank = if prepared.is_some() {
        usize::from(old.bank)
    } else {
        1 - usize::from(selected.bank)
    };
    let selector = IdSelector {
        sequence,
        database_id: loaded.files.database_id,
        active: selected,
        prepared: None,
    };
    let bytes = encoded_buffer(&selector.encode()?);
    loaded.identity = target;
    loaded.selection = SelectedIds {
        selector,
        selected,
        slots_agree: true,
    };
    loaded.state = state;
    Ok(IdCheckpointFinalize {
        loaded: Some(loaded),
        proof: Some(proof),
        selector: bytes,
        old_bank,
        step: 0,
        pending: None,
        finished: false,
    })
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
