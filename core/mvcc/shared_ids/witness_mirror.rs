use super::*;

impl LoadedIds {
    /// Establishes durable agreement before an admission decision can forget owner epochs
    /// that occur only behind an older selector copy. Active/prepared references stay intact.
    pub(in crate::mvcc) fn mirror_current_witness(self) -> Result<IdWitnessMirror> {
        if self.guard.published()? != self.identity.prefix {
            return Err(invalid("ID witness prefix differs from held authority"));
        }
        let maximum_owner_epoch = self.max_owner_epoch()?;
        let bytes = self.selection.selector.encode()?;
        let buffer = Arc::new(Buffer::new_temporary(PAGE_SIZE));
        buffer.as_mut_slice().copy_from_slice(&bytes);
        Ok(IdWitnessMirror {
            loaded: Some(self),
            buffer,
            maximum_owner_epoch,
            step: 0,
            pending: None,
            finished: false,
        })
    }
}

pub(in crate::mvcc) struct IdWitnessMirror {
    loaded: Option<LoadedIds>,
    buffer: Arc<Buffer>,
    maximum_owner_epoch: Option<u64>,
    step: u8,
    pending: Option<Completion>,
    finished: bool,
}

/// Constructed only after both selector copies have synchronized and all backend I/O retired.
pub(in crate::mvcc) struct MirroredIds {
    loaded: LoadedIds,
    maximum_owner_epoch: Option<u64>,
}
impl IdWitnessMirror {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<MirroredIds>> {
        use crate::io::{File, FileSyncType};
        if self.finished {
            return Err(invalid("ID witness mirroring already terminated"));
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
            let completion = match self.step {
                0 => loaded.files.selector.pwrite(
                    0,
                    self.buffer.clone(),
                    Completion::new_write(|_| {}),
                )?,
                1 => loaded
                    .files
                    .selector
                    .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                2 => loaded.files.selector.pwrite(
                    PAGE_SIZE as u64,
                    self.buffer.clone(),
                    Completion::new_write(|_| {}),
                )?,
                3 => loaded
                    .files
                    .selector
                    .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                4 => {
                    if !loaded.guard.io_retired() {
                        let completion = loaded.guard.retirement_completion();
                        self.finished = false;
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    let mut loaded = self.loaded.take().unwrap();
                    loaded.selection.slots_agree = true;
                    return Ok(IOResult::Done(MirroredIds {
                        loaded,
                        maximum_owner_epoch: self.maximum_owner_epoch,
                    }));
                }
                _ => unreachable!(),
            };
            self.step += 1;
            self.pending = Some(completion);
        }
    }
}
impl MirroredIds {
    pub(in crate::mvcc) fn into_guard_and_max_owner_epoch(self) -> (AuthorityGuard, Option<u64>) {
        (self.loaded.guard, self.maximum_owner_epoch)
    }
}
