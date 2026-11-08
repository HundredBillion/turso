#[cfg(all(host_shared_wal, unix))]
#[path = "checkpoint_recovery_confirmation.rs"]
pub(in crate::mvcc) mod checkpoint_recovery_confirmation;

use super::*;

pub(in crate::mvcc) struct CheckpointIntentPublication {
    file: Arc<dyn File>,
    publication: ManifestPublication,
    mirrored: bool,
    durable: Option<DurableManifest>,
    terminated: bool,
}

pub(in crate::mvcc) struct MirroredCheckpointIntent {
    file: Arc<dyn File>,
    record: ManifestRecord,
    guard: AuthorityGuard,
}

#[cfg(all(host_shared_wal, unix))]
impl LoadedManifest {
    pub(in crate::mvcc) fn mirror_recovered_checkpoint_intent(
        mut self,
        evidence: &crate::mvcc::database::RecoveryIntentEvidence,
    ) -> Result<CheckpointIntentPublication> {
        evidence.validate(&self.file, self.record, self.guard()?)?;
        if !self.guard()?.io_retired() {
            return Err(LimboError::Busy);
        }
        let file = self.file.clone();
        let publication = ManifestPublication {
            file: file.clone(),
            ownership: Arc::new(Mutex::new(self.guard.take())),
            slot: 1 - self.slot,
            record: self.record,
            stage: PublicationStage::Start,
        };
        Ok(CheckpointIntentPublication {
            file,
            publication,
            mirrored: false,
            durable: None,
            terminated: false,
        })
    }
}

impl LoadedManifest {
    pub(in crate::mvcc) fn begin_checkpoint_intent(
        self,
        source_checkpoint_timestamp: u64,
    ) -> Result<CheckpointIntentPublication> {
        let guard = self.guard()?;
        if guard.published()? != self.record.prefix() || !guard.incarnation_ready()? {
            return Err(LimboError::InvalidArgument(
                "checkpoint requires recovered shared authority".into(),
            ));
        }
        if !guard.live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        let intent = self.record.begin_checkpoint(source_checkpoint_timestamp)?;
        let file = self.file.clone();
        Ok(CheckpointIntentPublication {
            file,
            publication: self.publish(intent)?,
            mirrored: false,
            durable: None,
            terminated: false,
        })
    }
}

impl CheckpointIntentPublication {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<MirroredCheckpointIntent>> {
        if self.terminated {
            return Err(LimboError::InvalidArgument(
                "checkpoint publication terminated".into(),
            ));
        }
        self.terminated = true;
        loop {
            if let Some(durable) = self.durable.as_mut() {
                let guard = durable.guard.as_ref().ok_or_else(|| {
                    LimboError::InvalidArgument("checkpoint intent lost authority".into())
                })?;
                if !guard.io_retired() {
                    let waiter = guard.retirement_completion();
                    self.terminated = false;
                    return Ok(IOResult::IO(IOCompletions(waiter)));
                }
                return Ok(IOResult::Done(MirroredCheckpointIntent {
                    file: self.file.clone(),
                    record: durable.record,
                    guard: durable.guard.take().unwrap(),
                }));
            }
            let mut durable = match self.publication.poll()? {
                IOResult::IO(waiter) => {
                    self.terminated = false;
                    return Ok(IOResult::IO(waiter));
                }
                IOResult::Done(durable) => durable,
            };
            if self.mirrored {
                self.durable = Some(durable);
            } else {
                // Both slots must independently survive before physical materialization starts.
                self.publication = ManifestPublication {
                    file: self.file.clone(),
                    ownership: Arc::new(Mutex::new(durable.guard.take())),
                    slot: 1 - durable.slot,
                    record: durable.record,
                    stage: PublicationStage::Start,
                };
                self.mirrored = true;
            }
        }
    }
}

impl MirroredCheckpointIntent {
    pub(in crate::mvcc) fn record(&self) -> ManifestRecord {
        self.record
    }
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        &self.guard
    }
    pub(in crate::mvcc) fn into_guard(self) -> AuthorityGuard {
        self.guard
    }
}

/// Evidence of a fully mirrored completed generation, bound to its continuous authority lease.
/// Only completed publication may construct this value; selecting an intent is insufficient.
pub(in crate::mvcc) struct CompletedCheckpointProof {
    source: Option<crate::mvcc::shared_ids::io_protocol::RecoveryIdentity>,
    source_database: Option<crate::mvcc::shared_ids::DatabaseGeneration>,
    target: crate::mvcc::shared_ids::io_protocol::RecoveryIdentity,
    lease: crate::sync::Weak<PublicationLease>,
    materialized_floor: i64,
}

impl CompletedCheckpointProof {
    pub(in crate::mvcc) fn source_identity(
        &self,
    ) -> Option<crate::mvcc::shared_ids::io_protocol::RecoveryIdentity> {
        self.source
    }
    pub(in crate::mvcc) fn source_database(
        &self,
    ) -> Option<crate::mvcc::shared_ids::DatabaseGeneration> {
        self.source_database
    }
    pub(in crate::mvcc) fn target_identity(
        &self,
    ) -> crate::mvcc::shared_ids::io_protocol::RecoveryIdentity {
        self.target
    }
    pub(in crate::mvcc) fn matches_lease(&self, guard: &AuthorityGuard) -> bool {
        crate::sync::Weak::ptr_eq(&self.lease, &guard.io_lease())
    }
    pub(in crate::mvcc) fn materialized_floor(&self) -> i64 {
        self.materialized_floor
    }
}

pub(in crate::mvcc) struct CheckpointIntentToken {
    file: Arc<dyn File>,
    record: ManifestRecord,
    lease: crate::sync::Weak<PublicationLease>,
}

impl MirroredCheckpointIntent {
    pub(in crate::mvcc) fn suspend(self) -> (CheckpointIntentToken, AuthorityGuard) {
        let token = CheckpointIntentToken {
            file: self.file,
            record: self.record,
            lease: self.guard.io_lease(),
        };
        (token, self.guard)
    }
}

pub(in crate::mvcc) struct CheckpointCompletionPublication {
    token: CheckpointIntentToken,
    publication: ManifestPublication,
    ids: Option<crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::SuspendedPreparedIds>,
    source: crate::mvcc::shared_ids::io_protocol::RecoveryIdentity,
    target: crate::mvcc::shared_ids::io_protocol::RecoveryIdentity,
    materialized_floor: i64,
    mirrored: bool,
    durable: Option<DurableManifest>,
    terminated: bool,
}

pub(in crate::mvcc) struct MirroredCheckpointCompletion {
    proof: CompletedCheckpointProof,
    guard: AuthorityGuard,
    ids: crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::SuspendedPreparedIds,
}

impl MirroredCheckpointCompletion {
    pub(in crate::mvcc) fn into_parts(
        self,
    ) -> (
        CompletedCheckpointProof,
        AuthorityGuard,
        crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::SuspendedPreparedIds,
    ) {
        (self.proof, self.guard, self.ids)
    }
}

impl CheckpointIntentToken {
    #[cfg(all(host_shared_wal, unix))]
    pub(in crate::mvcc) fn begin_completion(
        self,
        materialized: crate::mvcc::database::checkpoint_state_machine::MaterializedCheckpointProof,
        prepared: crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::PreparedIds,
    ) -> Result<CheckpointCompletionPublication> {
        use crate::mvcc::shared_ids::{io_protocol::RecoveryIdentity, DatabaseGeneration};
        let intent = self
            .record
            .checkpoint_intent()
            .ok_or_else(|| LimboError::InvalidArgument("checkpoint token has no intent".into()))?;
        let source = RecoveryIdentity {
            database: DatabaseGeneration {
                generation: self.record.log_generation(),
                checkpoint_timestamp: intent.source_checkpoint_timestamp,
            },
            prefix: self.record.prefix(),
        };
        let target = RecoveryIdentity {
            database: DatabaseGeneration {
                generation: intent.target_generation,
                checkpoint_timestamp: source.prefix.committed_timestamp,
            },
            prefix: PublishedPrefix {
                committed_timestamp: source.prefix.committed_timestamp,
                end_offset: 0,
                running_crc: 0,
            },
        };
        if !crate::sync::Weak::ptr_eq(&self.lease, &prepared.guard().io_lease())
            || prepared.guard().published()? != source.prefix
            || prepared.source_identity() != source
            || prepared.target() != target.database
            || !materialized.matches_manifest(self.record, prepared.guard())
            || prepared.prepared_floor()? > materialized.physical_floor()
        {
            return Err(LimboError::InvalidArgument(
                "checkpoint completion evidence differs".into(),
            ));
        }
        if !prepared.guard().live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        let next = ManifestRecord::new(
            self.record
                .sequence()
                .checked_add(1)
                .ok_or(LimboError::DatabaseFull)?,
            self.record.database_id(),
            target.database.generation,
            0,
            target.prefix,
        )?;
        let (ids, guard) = prepared.suspend();
        let publication = LoadedManifest {
            file: self.file.clone(),
            guard: Some(guard),
            slot: 0,
            record: self.record,
        }
        .publish(next)?;
        Ok(CheckpointCompletionPublication {
            token: self,
            publication,
            ids: Some(ids),
            source,
            target,
            materialized_floor: materialized.physical_floor(),
            mirrored: false,
            durable: None,
            terminated: false,
        })
    }
}

impl CheckpointCompletionPublication {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<MirroredCheckpointCompletion>> {
        if self.terminated {
            return Err(LimboError::InvalidArgument(
                "checkpoint completion terminated".into(),
            ));
        }
        self.terminated = true;
        loop {
            if let Some(durable) = self.durable.as_mut() {
                let guard = durable.guard.as_ref().ok_or_else(|| {
                    LimboError::InvalidArgument("checkpoint completion lost authority".into())
                })?;
                if !guard.io_retired() {
                    let waiter = guard.retirement_completion();
                    self.terminated = false;
                    return Ok(IOResult::IO(IOCompletions(waiter)));
                }
                return Ok(IOResult::Done(MirroredCheckpointCompletion {
                    proof: CompletedCheckpointProof {
                        source: Some(self.source),
                        source_database: Some(self.source.database),
                        target: self.target,
                        lease: self.token.lease.clone(),
                        materialized_floor: self.materialized_floor,
                    },
                    guard: durable.guard.take().unwrap(),
                    ids: self.ids.take().unwrap(),
                }));
            }
            let mut durable = match self.publication.poll()? {
                IOResult::IO(waiter) => {
                    self.terminated = false;
                    return Ok(IOResult::IO(waiter));
                }
                IOResult::Done(durable) => durable,
            };
            if self.mirrored {
                self.durable = Some(durable);
            } else {
                self.publication = ManifestPublication {
                    file: self.token.file.clone(),
                    ownership: Arc::new(Mutex::new(durable.guard.take())),
                    slot: 1 - durable.slot,
                    record: durable.record,
                    stage: PublicationStage::Start,
                };
                self.mirrored = true;
            }
        }
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "checkpoint_completion_tests.rs"]
mod native_tests;
