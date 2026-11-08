use super::*;
use crate::io::File;
use crate::mvcc::commit_manifest::{LoadedManifest, ManifestRecord};
use crate::mvcc::read_pinned_file::ReadPinnedFile;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mvcc) enum InitialIdRoute {
    Healthy,
    CompletedCheckpoint,
}

pub(in crate::mvcc) struct InspectedOpenSelector {
    manifest: LoadedManifest,
    route: InitialIdRoute,
}
impl InspectedOpenSelector {
    pub(in crate::mvcc) fn into_parts(self) -> (LoadedManifest, InitialIdRoute) {
        (self.manifest, self.route)
    }
}

pub(in crate::mvcc) struct OpenSelectorProbe {
    pending: Option<PendingRead>,
    error: Option<LimboError>,
    data: Vec<u8>,
    reader: Arc<ReadPinnedFile>,
    files: Arc<SharedIds>,
    manifest: Option<LoadedManifest>,
    finished: bool,
}

impl SharedIds {
    pub(in crate::mvcc) fn inspect_open_selector(
        self: &Arc<Self>,
        manifest: LoadedManifest,
    ) -> Result<OpenSelectorProbe> {
        if manifest.record().database_id() != self.database_id
            || manifest.record().requires_recovery()
        {
            return Err(invalid(
                "initial ID inspection requires its normal manifest",
            ));
        }
        if self.selector.size()? != (2 * PAGE_SIZE) as u64 {
            return Err(corrupt("invalid published ID selector length"));
        }
        let reader = self.selector.selected_read_view(&manifest)?;
        let mut data = Vec::new();
        data.try_reserve_exact(2 * PAGE_SIZE)?;
        Ok(OpenSelectorProbe {
            pending: None,
            error: None,
            data,
            reader,
            files: self.clone(),
            manifest: Some(manifest),
            finished: false,
        })
    }
}

impl OpenSelectorProbe {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<InspectedOpenSelector>> {
        if self.finished {
            return Err(invalid("initial ID inspection has ended"));
        }
        if self.error.is_none() {
            match self.poll_inner() {
                Ok(result) => return Ok(result),
                Err(error) => self.error = Some(error),
            }
        }
        let guard = self.manifest.as_ref().unwrap().guard()?;
        if !guard.io_retired() {
            return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
        }
        self.finished = true;
        Err(self.error.take().unwrap())
    }

    fn poll_inner(&mut self) -> Result<IOResult<InspectedOpenSelector>> {
        loop {
            if let Some(read) = self.pending.take() {
                if !read.completion.finished() {
                    let completion = read.completion.clone();
                    self.pending = Some(read);
                    return Ok(IOResult::IO(IOCompletions(completion)));
                }
                if let Some(error) = read.completion.get_error() {
                    return Err(error.into());
                }
                let count = read.count.load(Ordering::Acquire);
                if count <= 0 || count as usize > read.buffer.len() {
                    return Err(corrupt("short or invalid initial ID selector read"));
                }
                self.data
                    .extend_from_slice(&read.buffer.as_slice()[..count as usize]);
            }
            if self.data.len() == 2 * PAGE_SIZE {
                let (selector, _) = select_witness(
                    [&self.data[..PAGE_SIZE], &self.data[PAGE_SIZE..]],
                    self.files.database_id,
                )?;
                let manifest = self.manifest.as_ref().unwrap();
                let route = classify(selector, manifest.record())?;
                let guard = manifest.guard()?;
                if !guard.io_retired() {
                    return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
                }
                self.finished = true;
                return Ok(IOResult::Done(InspectedOpenSelector {
                    manifest: self.manifest.take().unwrap(),
                    route,
                }));
            }
            let buffer = Arc::new(Buffer::new_temporary(
                (2 * PAGE_SIZE - self.data.len()).min(PAGE_SIZE),
            ));
            let count = Arc::new(AtomicI32::new(-1));
            let callback_count = count.clone();
            let completion = Completion::new_read(buffer.clone(), move |result| {
                if let Ok((_, bytes)) = result {
                    callback_count.store(bytes, Ordering::Release);
                }
                None
            });
            let completion = self.reader.pread(self.data.len() as u64, completion)?;
            self.pending = Some(PendingRead {
                completion,
                buffer,
                count,
            });
        }
    }
}

fn classify(selector: IdSelector, record: ManifestRecord) -> Result<InitialIdRoute> {
    let prefix = record.prefix();
    if let Some(prepared) = selector.prepared {
        if prefix.end_offset != 0
            || prefix.running_crc != 0
            || record.log_salt() != 0
            || prepared.database
                != (DatabaseGeneration {
                    generation: record.log_generation(),
                    checkpoint_timestamp: prefix.committed_timestamp,
                })
        {
            return Err(corrupt(
                "prepared IDs differ from the completed normal target",
            ));
        }
        return Ok(InitialIdRoute::CompletedCheckpoint);
    }
    if selector.active.database.generation != record.log_generation()
        || selector.active.database.checkpoint_timestamp > prefix.committed_timestamp
        || (prefix.end_offset == 0
            && selector.active.database.checkpoint_timestamp != prefix.committed_timestamp)
    {
        return Err(corrupt(
            "active IDs differ from the selected normal generation",
        ));
    }
    Ok(InitialIdRoute::Healthy)
}
