use super::*;
use crate::mvcc::database::RecoveryCompletionEvidence;

pub(in crate::mvcc) struct RecoveryCompletionMirror {
    evidence: RecoveryCompletionEvidence,
    file: Arc<dyn File>,
    publication: ManifestPublication,
    mirrored: bool,
    durable: Option<DurableManifest>,
    terminated: bool,
}

pub(in crate::mvcc) struct MirroredRecoveryCompletion {
    proof: CompletedCheckpointProof,
    guard: AuthorityGuard,
}

impl MirroredRecoveryCompletion {
    pub(in crate::mvcc) fn into_parts(self) -> (CompletedCheckpointProof, AuthorityGuard) {
        (self.proof, self.guard)
    }
}

impl LoadedManifest {
    pub(in crate::mvcc) fn mirror_recovered_completion(
        mut self,
        evidence: RecoveryCompletionEvidence,
    ) -> Result<RecoveryCompletionMirror> {
        let guard = self.guard()?;
        evidence.validate(&self.file, self.record, guard)?;
        if !guard.io_retired() {
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
        Ok(RecoveryCompletionMirror {
            evidence,
            file,
            publication,
            mirrored: false,
            durable: None,
            terminated: false,
        })
    }
}

impl RecoveryCompletionMirror {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<MirroredRecoveryCompletion>> {
        if self.terminated {
            return Err(LimboError::InvalidArgument(
                "recovered completion mirror has ended".into(),
            ));
        }
        self.terminated = true;
        loop {
            if let Some(durable) = self.durable.as_mut() {
                let guard = durable.guard.as_ref().ok_or_else(|| {
                    LimboError::InvalidArgument("recovered completion lost authority".into())
                })?;
                if !guard.io_retired() {
                    self.terminated = false;
                    return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
                }
                self.evidence.validate(&self.file, durable.record, guard)?;
                let proof = CompletedCheckpointProof {
                    source: None,
                    source_database: self.evidence.source_database(),
                    target: self.evidence.target(),
                    lease: guard.io_lease(),
                    materialized_floor: self.evidence.physical_floor(),
                };
                return Ok(IOResult::Done(MirroredRecoveryCompletion {
                    proof,
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
