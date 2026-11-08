use super::*;

#[allow(dead_code)]
pub(in crate::mvcc) struct UnrecoveredIds {
    loaded: LoadedIds,
    max_owner_epoch: Option<u64>,
}

#[allow(dead_code)]
pub(in crate::mvcc) struct SuspendedRecoveryIds {
    ids: super::checkpoint_prepare::SuspendedIds,
    identity: RecoveryIdentity,
    source_database: Option<DatabaseGeneration>,
}

#[allow(dead_code)]
impl UnrecoveredIds {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.loaded.guard()
    }

    pub(in crate::mvcc) fn identity(&self) -> RecoveryIdentity {
        self.loaded.identity()
    }

    pub(in crate::mvcc) fn logical_floor(&self) -> i64 {
        self.loaded.selected_logical_floor()
    }

    pub(in crate::mvcc) fn source_database(&self) -> Option<DatabaseGeneration> {
        self.loaded.pending_checkpoint_source()
    }

    pub(in crate::mvcc) fn has_pending_checkpoint(&self) -> bool {
        self.loaded.has_pending_checkpoint()
    }

    pub(in crate::mvcc) fn max_owner_epoch(&self) -> Option<u64> {
        self.max_owner_epoch
    }

    pub(in crate::mvcc) fn suspend(self) -> (SuspendedRecoveryIds, AuthorityGuard) {
        let identity = self.identity();
        let source_database = self.source_database();
        let (ids, guard) = self.loaded.suspend();
        (
            SuspendedRecoveryIds {
                ids,
                identity,
                source_database,
            },
            guard,
        )
    }
}

pub(in crate::mvcc) struct RecoveryTargetIdLoad {
    load: Option<IdLoad>,
    physical_floor: i64,
}

impl SharedIds {
    pub(in crate::mvcc) fn inspect_completed_target(
        self: &Arc<Self>,
        guard: AuthorityGuard,
        identity: RecoveryIdentity,
        physical_floor: i64,
    ) -> Result<RecoveryTargetIdLoad> {
        guard.ensure_no_snapshot_locks()?;
        if identity.database.generation == 0
            || identity.database.checkpoint_timestamp != identity.prefix.committed_timestamp
            || identity.prefix.end_offset != 0
            || identity.prefix.running_crc != 0
            || physical_floor >= 0
            || physical_floor == i64::MIN
        {
            return Err(invalid(
                "ID inspection requires a completed physical target",
            ));
        }
        Ok(RecoveryTargetIdLoad {
            load: Some(self.start_load(guard, IdLoadTarget::Known(identity))?),
            physical_floor,
        })
    }
}

impl RecoveryTargetIdLoad {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<UnrecoveredIds>> {
        let result = self.poll_inner();
        if result.is_err() {
            self.load = None;
        }
        result
    }

    fn poll_inner(&mut self) -> Result<IOResult<UnrecoveredIds>> {
        let load = self
            .load
            .as_mut()
            .ok_or_else(|| invalid("ID inspection has ended"))?;
        let loaded = match load.poll()? {
            IOResult::IO(waiter) => return Ok(IOResult::IO(waiter)),
            IOResult::Done(loaded) => loaded,
        };
        self.load = None;
        loaded.guard.ensure_no_snapshot_locks()?;
        let old_floor = if loaded.selection.selector.prepared.is_some() {
            Some(
                replay_selected(
                    SelectedIds {
                        selected: loaded.selection.selector.active,
                        ..loaded.selection
                    },
                    [&loaded.banks[0], &loaded.banks[1]],
                )?
                .logical_floor,
            )
        } else {
            None
        };
        if loaded.state.logical_floor > self.physical_floor
            || old_floor.is_some_and(|floor| loaded.state.logical_floor > floor)
        {
            return Err(corrupt(
                "completed target ID floor lost retained physical or logical IDs",
            ));
        }
        let max_owner_epoch = loaded.max_owner_epoch()?;
        Ok(IOResult::Done(UnrecoveredIds {
            loaded,
            max_owner_epoch,
        }))
    }
}

use super::checkpoint_prepare::finalize::{FinalizedIds, IdCheckpointFinalize};
use super::witness_mirror::IdWitnessMirror;
use crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof;

pub(in crate::mvcc) struct RecoveredIdFinalization {
    mirror: Option<IdWitnessMirror>,
    finalizer: Option<IdCheckpointFinalize>,
    proof: Option<CompletedCheckpointProof>,
    captured_maximum: Option<u64>,
    finished: bool,
}

impl SuspendedRecoveryIds {
    pub(in crate::mvcc) fn begin_finalize(
        self,
        guard: AuthorityGuard,
        proof: CompletedCheckpointProof,
        captured_maximum: Option<u64>,
    ) -> Result<RecoveredIdFinalization> {
        if proof.source_identity().is_some()
            || proof.target_identity() != self.identity
            || proof.source_database() != self.source_database
            || !proof.matches_lease(&guard)
            || guard.published()? != self.identity.prefix
            || proof.materialized_floor() >= 0
            || proof.materialized_floor() == i64::MIN
        {
            return Err(invalid("recovered ID finalization proof differs"));
        }
        guard.ensure_no_snapshot_locks()?;
        if !guard.io_retired() {
            return Err(LimboError::Busy);
        }
        let loaded = self.ids.rejoin_source(guard)?;
        if loaded.max_owner_epoch()? != captured_maximum {
            return Err(invalid("recovered ID epoch witness differs"));
        }
        if loaded.selected_logical_floor() > proof.materialized_floor() {
            return Err(invalid("recovered ID floor does not cover physical target"));
        }
        let ready = loaded.guard.incarnation_ready()?;
        Ok(if ready {
            RecoveredIdFinalization {
                mirror: None,
                finalizer: Some(loaded.finalize_recovered_checkpoint(proof)?),
                proof: None,
                captured_maximum,
                finished: false,
            }
        } else {
            RecoveredIdFinalization {
                mirror: Some(loaded.mirror_current_witness()?),
                finalizer: None,
                proof: Some(proof),
                captured_maximum,
                finished: false,
            }
        })
    }
}
impl RecoveredIdFinalization {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<FinalizedIds>> {
        let result = self.poll_inner();
        if result.is_err() {
            self.mirror = None;
            self.finalizer = None;
            self.proof = None;
            self.finished = true;
        }
        result
    }
    fn poll_inner(&mut self) -> Result<IOResult<FinalizedIds>> {
        if self.finished {
            return Err(invalid("recovered ID finalization has ended"));
        }
        if let Some(mirror) = self.mirror.as_mut() {
            let mirrored = match mirror.poll()? {
                IOResult::IO(waiter) => return Ok(IOResult::IO(waiter)),
                IOResult::Done(mirrored) => mirrored,
            };
            self.mirror = None;
            self.finalizer = Some(mirrored.finalize_recovered_checkpoint(
                self.proof.take().unwrap(),
                self.captured_maximum,
            )?);
        }
        let finalized = match self.finalizer.as_mut().unwrap().poll()? {
            IOResult::IO(waiter) => return Ok(IOResult::IO(waiter)),
            IOResult::Done(finalized) => finalized,
        };
        self.finalizer = None;
        self.finished = true;
        Ok(IOResult::Done(finalized))
    }
}
