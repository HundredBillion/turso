use super::shared_refresh::{SharedBeginOptions, SharedRefresh, SharedReplayContext};
use super::*;
use crate::io::FileId;
use crate::mvcc::checkpoint_gate::{AdmissionGuard, CheckpointAdmissionGate};
use crate::mvcc::commit_manifest::{LoadedManifest, ManifestLoad};
use crate::mvcc::guarded_file::{FileBinding, GuardedFile};
use crate::mvcc::persistent_storage::Storage;
use crate::mvcc::shared_files::SharedMvccFiles;
use crate::storage::shared_wal_coordination::MappedSharedWalCoordination;
use crate::sync::Weak;
use std::path::{Path, PathBuf};

pub(crate) struct SharedMvccRuntime {
    files: SharedMvccFiles,
    gate: Arc<CheckpointAdmissionGate>,
    main: Arc<GuardedFile>,
    wal: Arc<GuardedFile>,
    logical: Arc<GuardedFile>,
    storage: Arc<Storage>,
    replay: Mutex<Option<Arc<SharedReplayContext>>>,
    database_id: FileId,
    wal_path: PathBuf,
}

impl Debug for SharedMvccRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedMvccRuntime")
            .field("database_id", &self.database_id)
            .finish_non_exhaustive()
    }
}

pub(crate) struct SharedOpenState {
    runtime: Arc<SharedMvccRuntime>,
    admission: Option<AdmissionGuard>,
    load: Option<ManifestLoad>,
    loaded: Option<LoadedManifest>,
    refresh: Option<SharedRefresh>,
    failed: bool,
    bindings: Vec<FileBinding>,
}

#[derive(Default)]
pub(crate) struct SharedBeginState {
    open: Option<SharedOpenState>,
    request: Option<BeginRequest>,
}

struct BeginRequest {
    connection: Weak<Connection>,
    pager: Arc<Pager>,
    exclusive: bool,
    expected_schema_generation: Option<u64>,
    database_id: usize,
}

impl Debug for SharedBeginState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedBeginState")
            .field("pending", &self.open.is_some())
            .finish()
    }
}

impl SharedBeginState {
    pub(crate) fn is_pending(&self) -> bool {
        self.request.is_some()
    }
    pub(crate) fn captured_expected_schema_generation(&self) -> Option<Option<u64>> {
        self.request
            .as_ref()
            .map(|request| request.expected_schema_generation)
    }
    pub(crate) fn reset(&mut self) {
        self.open = None;
        self.request = None;
    }
}

impl SharedMvccRuntime {
    pub(crate) fn open_existing(
        io: Arc<dyn crate::IO>,
        main: Arc<dyn File>,
        database_id: FileId,
        canonical_path: &Path,
        history_limit: u64,
        readonly: bool,
    ) -> Result<Arc<Self>> {
        let files = SharedMvccFiles::open_existing(
            io.clone(),
            main,
            database_id,
            canonical_path,
            history_limit,
            readonly,
        )?;
        let gate = CheckpointAdmissionGate::open_existing(
            &io,
            &files.namespace.join("authority"),
            files.authority.file_id()?.ok_or_else(|| {
                LimboError::InvalidArgument("shared authority has no native identity".into())
            })?,
        )?;
        let main = GuardedFile::new(files.main.clone(), &files.authority);
        let wal = GuardedFile::new(files.physical_wal.clone(), &files.authority);
        let logical = GuardedFile::new(files.logical.clone(), &files.authority);
        let storage = Arc::new(Storage::new(logical.clone(), io, None));
        let wal_path = files.namespace.join("physical-wal");
        Ok(Arc::new(Self {
            files,
            gate,
            main,
            wal,
            logical,
            storage,
            replay: Mutex::new(None),
            database_id,
            wal_path,
        }))
    }

    pub(crate) fn main_file(&self) -> Arc<dyn File> {
        self.main.clone()
    }
    pub(crate) fn physical_wal_file(&self) -> Arc<dyn File> {
        self.wal.clone()
    }
    pub(crate) fn storage(&self) -> Arc<Storage> {
        self.storage.clone()
    }
    pub(crate) fn physical_coordination(&self) -> Arc<MappedSharedWalCoordination> {
        self.files.physical_coordination.clone()
    }
    pub(crate) fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    pub(crate) fn begin_open(self: &Arc<Self>) -> Result<SharedOpenState> {
        let admission = self.gate.try_enter_admission()?;
        let guard = self.files.authority.try_acquire()?;
        let load = ManifestLoad::new(self.files.manifest.clone(), guard, self.database_id)?;
        Ok(SharedOpenState {
            runtime: self.clone(),
            admission: Some(admission),
            load: Some(load),
            loaded: None,
            refresh: None,
            failed: false,
            bindings: Vec::new(),
        })
    }
}

impl SharedOpenState {
    pub(crate) fn poll_manifest(&mut self) -> IOResultOr<()> {
        if self.failed {
            return Err(LimboError::InvalidArgument("shared open already failed".into()).into());
        }
        if self.loaded.is_some() || self.refresh.is_some() {
            return Ok(IOResult::Done(()));
        }
        let result = self.load.as_mut().unwrap().poll();
        match result {
            Ok(IOResult::IO(io)) => Ok(IOResult::IO(io)),
            Ok(IOResult::Done(loaded)) => {
                self.load = None;
                let bindings = (|| {
                    let guard = loaded.guard()?;
                    Ok::<_, LimboError>(vec![
                        self.runtime.main.bind(guard)?,
                        self.runtime.wal.bind(guard)?,
                    ])
                })();
                match bindings {
                    Ok(bindings) => self.bindings = bindings,
                    Err(error) => {
                        self.abort();
                        return Err(error.into());
                    }
                }
                self.loaded = Some(loaded);
                Ok(IOResult::Done(()))
            }
            Err(error) => {
                self.abort();
                Err(error.into())
            }
        }
    }

    fn abort(&mut self) {
        self.bindings.clear();
        self.refresh = None;
        self.loaded = None;
        self.load = None;
        self.admission = None;
        self.failed = true;
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> MvStore<C, A> {
    pub(crate) fn attach_shared_runtime(&self, runtime: Arc<SharedMvccRuntime>) -> Result<()> {
        let expected: Arc<dyn crate::mvcc::persistent_storage::DurableStorage> = runtime.storage();
        if !Arc::ptr_eq(&self.storage, &expected) || self.experimental_mvcc_passive_checkpoint {
            return Err(LimboError::InvalidArgument(
                "shared runtime storage or checkpoint mode differs".into(),
            ));
        }
        if let Some(existing) = self.shared_runtime.get() {
            return if Arc::ptr_eq(existing, &runtime) {
                Ok(())
            } else {
                Err(LimboError::InvalidArgument(
                    "MVCC store already has another shared runtime".into(),
                ))
            };
        }
        if !self.txs.is_empty() {
            return Err(LimboError::Busy);
        }
        self.shared_runtime
            .set(runtime)
            .map_err(|_| LimboError::Busy)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_shared_nonblock(
        &self,
        connection: &Arc<Connection>,
        pager: Arc<Pager>,
        exclusive: bool,
        expected_schema_generation: Option<u64>,
        database_id: usize,
        state: &mut SharedBeginState,
    ) -> IOResultOr<TxID> {
        let result = (|| {
            let runtime = self.shared_runtime.get().ok_or_else(|| {
                LimboError::InvalidArgument("MVCC store has no shared runtime".into())
            })?;
            if let Some(request) = &state.request {
                if !request
                    .connection
                    .upgrade()
                    .is_some_and(|saved| Arc::ptr_eq(&saved, connection))
                    || !Arc::ptr_eq(&request.pager, &pager)
                    || request.exclusive != exclusive
                    || request.expected_schema_generation != expected_schema_generation
                    || request.database_id != database_id
                {
                    return Err(LimboError::InvalidArgument(
                        "shared begin changed across I/O".into(),
                    )
                    .into());
                }
            } else {
                state.open = Some(runtime.begin_open()?);
                state.request = Some(BeginRequest {
                    connection: Arc::downgrade(connection),
                    pager: pager.clone(),
                    exclusive,
                    expected_schema_generation,
                    database_id,
                });
            }
            let open = state.open.as_mut().unwrap();
            return_if_io!(open.poll_manifest());
            if open.refresh.is_none() {
                let replay = runtime.replay.lock().clone().ok_or_else(|| {
                    LimboError::InvalidArgument("shared runtime has not completed bootstrap".into())
                })?;
                open.refresh =
                    Some(replay.refresh(open.loaded.take().unwrap(), runtime.logical.clone())?);
            }
            open.refresh.as_mut().unwrap().poll_begin(
                self,
                connection,
                SharedBeginOptions {
                    pager,
                    expected_schema_generation,
                    portable_logical_changes: connection.portable_logical_changes_enabled(),
                    database_id,
                    exclusive,
                },
            )
        })();
        if !matches!(&result, Ok(IOResult::IO(_))) {
            state.reset();
        }
        result
    }
}
