use super::*;
#[cfg(all(host_shared_wal, unix))]
use crate::mvcc::database::InstalledDowngradeSource;

/// Mirrors an intent; it does not certify physical materialization or quiescence.
/// The production constructor consumes the installed, quiescent checkpoint capability.
pub(in crate::mvcc) struct DowngradeIntentPublication {
    file: Arc<dyn File>,
    publication: ManifestPublication,
    mirrored: bool,
    durable: Option<DurableManifest>,
    terminated: bool,
    #[cfg(all(host_shared_wal, unix))]
    source: Option<InstalledDowngradeSource>,
}

pub(in crate::mvcc) struct MirroredDowngradeIntent {
    _file: Arc<dyn File>,
    record: ManifestRecord,
    guard: AuthorityGuard,
    #[cfg(all(host_shared_wal, unix))]
    _source: Option<InstalledDowngradeSource>,
}

impl DowngradeIntentPublication {
    #[cfg(all(host_shared_wal, unix))]
    pub(in crate::mvcc) fn from_installed(
        loaded: LoadedManifest,
        source: InstalledDowngradeSource,
    ) -> Result<Self> {
        source.validate(&loaded)?;
        let intent = loaded.record.begin_downgrade(source.wal_id())?;
        let file = loaded.file.clone();
        Ok(Self {
            file,
            publication: loaded.publish_record(intent)?,
            mirrored: false,
            durable: None,
            terminated: false,
            source: Some(source),
        })
    }

    // Synthetic fixtures certify publication ordering only.
    #[cfg(test)]
    pub(super) fn synthetic(loaded: LoadedManifest, basename_wal_id: FileId) -> Result<Self> {
        let guard = loaded.guard()?;
        if guard.published()? != loaded.record.prefix() || !guard.incarnation_ready()? {
            return Err(LimboError::InvalidArgument(
                "downgrade requires recovered shared authority".into(),
            ));
        }
        if !guard.live_registrations()?.is_empty() || !guard.io_retired() {
            return Err(LimboError::Busy);
        }
        let intent = loaded.record.begin_downgrade(basename_wal_id)?;
        let file = loaded.file.clone();
        Ok(Self {
            file,
            publication: loaded.publish_record(intent)?,
            mirrored: false,
            durable: None,
            terminated: false,
            #[cfg(all(host_shared_wal, unix))]
            source: None,
        })
    }
}

impl DowngradeIntentPublication {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<MirroredDowngradeIntent>> {
        if self.terminated {
            return Err(LimboError::InvalidArgument(
                "downgrade publication terminated".into(),
            ));
        }
        self.terminated = true;
        loop {
            if let Some(durable) = self.durable.as_mut() {
                let guard = durable.guard.as_ref().ok_or_else(|| {
                    LimboError::InvalidArgument("downgrade intent lost authority".into())
                })?;
                if !guard.io_retired() {
                    let waiter = guard.retirement_completion();
                    self.terminated = false;
                    return Ok(IOResult::IO(IOCompletions(waiter)));
                }
                return Ok(IOResult::Done(MirroredDowngradeIntent {
                    _file: self.file.clone(),
                    record: durable.record,
                    guard: durable.guard.take().unwrap(),
                    #[cfg(all(host_shared_wal, unix))]
                    _source: self.source.take(),
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
                // Both slots must survive before any mode-marker transaction can start.
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

impl MirroredDowngradeIntent {
    #[cfg(all(host_shared_wal, unix))]
    pub(in crate::mvcc) fn installed_source(&self) -> Result<&InstalledDowngradeSource> {
        self._source.as_ref().ok_or_else(|| {
            LimboError::InvalidArgument(
                "marker transition requires a genuine installed checkpoint".into(),
            )
        })
    }

    pub(in crate::mvcc) fn record(&self) -> ManifestRecord {
        self.record
    }
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        &self.guard
    }
}

#[cfg(all(test, target_os = "linux", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn commit_manifest_downgrade_native_fences_new_open_and_idle_admission() {
    use crate::mvcc::database::shared_runtime_fixture;
    use crate::mvcc::shared_files::SharedMvccFiles;
    use crate::util::IOExt;
    use crate::{ConnectOpts, Database, EngineDialect, OpenFlags, OpenOptions, IO};
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let options = || OpenOptions::new(Arc::new(EngineDialect)).with_shared_mvcc_for_test(limit);
    let db = Database::open(io.clone(), path.to_str().unwrap(), options()).unwrap();
    let idle = db.connect(ConnectOpts::new()).unwrap();
    idle.prepare("SELECT count(*) FROM preserved")
        .unwrap()
        .run_with_row_callback(|_| Ok(()))
        .unwrap();
    let main = io
        .open_file(path.to_str().unwrap(), OpenFlags::NoLock, false)
        .unwrap();
    let database_id = main.file_id().unwrap().unwrap();
    let files =
        SharedMvccFiles::open_existing(io.clone(), main, database_id, &path, limit, false).unwrap();
    let mut wal_path = path.as_os_str().to_owned();
    wal_path.push("-wal");
    let wal = io
        .open_file(
            std::path::Path::new(&wal_path).to_str().unwrap(),
            OpenFlags::NoLock,
            false,
        )
        .unwrap();
    let guard = files.authority.try_acquire().unwrap();
    let mut load = ManifestLoad::new(files.manifest.clone(), guard, database_id).unwrap();
    let mut loaded = io.block(|| Ok(load.poll()?)).unwrap();
    let prefix = loaded.record().prefix();
    if loaded.guard().unwrap().requires_recovery() {
        loaded.guard_mut().unwrap().recover(prefix).unwrap();
    }
    // This synthetic publication tests the admission fence, not materialization.
    let mut publication =
        DowngradeIntentPublication::synthetic(loaded, wal.file_id().unwrap().unwrap()).unwrap();
    let proof = io.block(|| Ok(publication.poll()?)).unwrap();
    assert!(proof.record().requires_recovery());
    drop(proof);
    let reopened = Database::open(io.clone(), path.to_str().unwrap(), options());
    assert!(
        matches!(reopened, Err(LimboError::InvalidArgument(message)) if message.contains("shared manifest recovery is required"))
    );
    let error = idle
        .prepare("SELECT count(*) FROM preserved")
        .and_then(|mut statement| statement.run_with_row_callback(|_| Ok(())))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("shared manifest recovery is required"),
        "{error}"
    );
}
