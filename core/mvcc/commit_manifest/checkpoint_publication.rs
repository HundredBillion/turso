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
