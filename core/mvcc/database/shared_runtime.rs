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
    manifest_ready: bool,
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
            manifest_ready: false,
            bindings: Vec::new(),
        })
    }
}

impl SharedOpenState {
    pub(crate) fn poll_manifest(&mut self) -> IOResultOr<()> {
        if self.failed {
            return Err(LimboError::InvalidArgument("shared open already failed".into()).into());
        }
        if self.manifest_ready {
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
                self.manifest_ready = true;
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

#[derive(Default)]
pub(crate) struct SharedBootstrapState {
    stage: SharedBootstrapStage,
    connection: Option<Weak<Connection>>,
    reparse: crate::connection::ReparseSchemaState,
    base: super::shared_bootstrap::SharedBaseContextState,
    post: Option<BootstrapState>,
    tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
}

#[derive(Default)]
enum SharedBootstrapStage {
    #[default]
    Start,
    Reparse,
    Base,
    Replay,
    Post,
    Done,
    Failed,
}

impl<C: LogicalClock, A: ConcurrentAllocator> MvStore<C, A> {
    pub(crate) fn bootstrap_shared_nonblock(
        &self,
        connection: &Arc<Connection>,
        open: &mut SharedOpenState,
        state: &mut SharedBootstrapState,
    ) -> IOResultOr<()> {
        let result = (|| {
            let runtime = self.shared_runtime.get().ok_or_else(|| {
                LimboError::InvalidArgument("shared bootstrap requires attached runtime".into())
            })?;
            if !Arc::ptr_eq(runtime, &open.runtime)
                || state.connection.as_ref().is_some_and(|saved| {
                    !saved
                        .upgrade()
                        .is_some_and(|saved| Arc::ptr_eq(&saved, connection))
                })
            {
                return Err(
                    LimboError::InvalidArgument("shared bootstrap owner changed".into()).into(),
                );
            }
            loop {
                match state.stage {
                    SharedBootstrapStage::Start => {
                        if !connection.is_mvcc_bootstrap_connection()
                            || !self.txs.is_empty()
                            || runtime.replay.lock().is_some()
                        {
                            return Err(LimboError::InvalidArgument(
                                "shared base requires a fresh bootstrap store".into(),
                            )
                            .into());
                        }
                        return_if_io!(open.poll_manifest());
                        state.connection = Some(Arc::downgrade(connection));
                        state.tvfs =
                            Self::capture_table_valued_functions(&connection.schema.read());
                        state.stage = SharedBootstrapStage::Reparse;
                    }
                    SharedBootstrapStage::Reparse => {
                        return_if_io!(connection.reparse_schema_nonblock(&mut state.reparse));
                        self.rehydrate_connection_table_valued_functions(connection, &state.tvfs)?;
                        *connection.db.schema.lock() = connection.schema.read().clone();
                        let header =
                            return_if_io!(connection.pager.load().with_header(|header| *header));
                        self.global_header.write().replace(header);
                        self.bootstrap_map_root_pages(connection)?;
                        state.stage = SharedBootstrapStage::Base;
                    }
                    SharedBootstrapStage::Base => {
                        let context =
                            return_if_io!(self
                                .prepare_shared_base_context_nonblock(connection, &mut state.base));
                        let persistent = context.persistent_tx_ts_max;
                        let observed = open.loaded.as_ref().unwrap().record();
                        if persistent > observed.prefix().committed_timestamp {
                            return Err(LimboError::Corrupt(
                                "checkpoint is newer than shared manifest".into(),
                            )
                            .into());
                        }
                        self.clock.publish_observed(persistent, || -> Result<()> {
                            self.durable_txid_max.store(persistent, Ordering::Release);
                            self.last_committed_tx_ts
                                .store(persistent, Ordering::Release);
                            self.last_global_header_ts
                                .store(persistent, Ordering::Release);
                            Ok(())
                        })?;
                        let replay = SharedReplayContext::new(context, observed, runtime.storage());
                        *runtime.replay.lock() = Some(replay.clone());
                        open.refresh = Some(
                            replay.refresh(open.loaded.take().unwrap(), runtime.logical.clone())?,
                        );
                        state.stage = SharedBootstrapStage::Replay;
                    }
                    SharedBootstrapStage::Replay => {
                        let loaded =
                            return_if_io!(open.refresh.as_mut().unwrap().poll(self, connection));
                        loaded.into_guard()?.finish()?;
                        open.refresh = None;
                        open.bindings.clear();
                        open.admission = None;
                        connection.promote_to_regular_connection();
                        state.post = Some(BootstrapState::LoadSequences {
                            load_st: crate::connection::LoadSequenceDescriptorsState::default(),
                        });
                        state.stage = SharedBootstrapStage::Post;
                    }
                    SharedBootstrapStage::Post => {
                        return_if_io!(
                            self.bootstrap_nonblock(connection, state.post.as_mut().unwrap())
                        );
                        state.post = None;
                        state.stage = SharedBootstrapStage::Done;
                    }
                    SharedBootstrapStage::Done => return Ok(IOResult::Done(())),
                    SharedBootstrapStage::Failed => {
                        return Err(LimboError::InvalidArgument(
                            "shared bootstrap already failed".into(),
                        )
                        .into())
                    }
                }
            }
        })();
        if result.is_err() {
            open.abort();
            state.stage = SharedBootstrapStage::Failed;
            state.post = None;
        }
        result
    }
}
