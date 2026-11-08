#[path = "shared_pager_generation.rs"]
mod shared_pager_generation;

#[path = "shared_checkpoint.rs"]
mod shared_checkpoint;

#[path = "shared_id_runtime.rs"]
mod shared_id_runtime;
pub(crate) use shared_id_runtime::{SharedIdRequest, SharedIdResult, SharedIdState};

use super::shared_refresh::{SharedBeginOptions, SharedRefresh, SharedReplayContext};
use super::*;
use crate::io::FileId;
use crate::mvcc::checkpoint_gate::{AdmissionGuard, CheckpointAdmissionGate};
use crate::mvcc::commit_manifest::{LoadedManifest, ManifestLoad};
use crate::mvcc::guarded_file::{FileBinding, GuardedFile};
use crate::mvcc::persistent_storage::Storage;
use crate::mvcc::shared_files::SharedMvccFiles;
use crate::mvcc::shared_ids::io_protocol::witness_mirror::IdWitnessMirror;
use crate::mvcc::shared_ids::io_protocol::{IdLoad, RecoveryIdentity, SharedIds};
use crate::mvcc::shared_ids::DatabaseGeneration;
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
    ids: Arc<SharedIds>,
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
    pin_physical_reads: bool,
    connection_refreshed: bool,
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
        let ids = SharedIds::open_existing(
            files
                .id_banks
                .each_ref()
                .map(|file| GuardedFile::new(file.clone(), &files.authority)),
            GuardedFile::new(files.id_selector.clone(), &files.authority),
            database_id,
            history_limit,
        )?;
        let storage = Arc::new(Storage::new(logical.clone(), io, None));
        let wal_path = files.namespace.join("physical-wal");
        Ok(Arc::new(Self {
            files,
            gate,
            main,
            wal,
            logical,
            storage,
            ids,
            replay: Mutex::new(None),
            database_id,
            wal_path,
        }))
    }

    pub(super) fn owns_authority(
        &self,
        guard: &crate::mvcc::shared_authority::AuthorityGuard,
    ) -> bool {
        guard.belongs_to(&Arc::downgrade(&self.files.authority))
    }

    pub(super) fn validate_oracle_identity(
        &self,
        guard: &crate::mvcc::shared_authority::AuthorityGuard,
        identity: RecoveryIdentity,
    ) -> Result<()> {
        if !self.owns_authority(guard)
            || guard.published()? != identity.prefix
            || !guard.incarnation_ready()?
            || !self
                .replay
                .lock()
                .as_ref()
                .is_some_and(|replay| replay.matches_oracle_identity(identity))
        {
            return Err(LimboError::InvalidArgument(
                "oracle requires exact refreshed shared identity".into(),
            ));
        }
        Ok(())
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
        self.begin_open_with_read_policy(false)
    }

    pub(crate) fn begin_read_pinned_open(self: &Arc<Self>) -> Result<SharedOpenState> {
        self.begin_open_with_read_policy(true)
    }

    fn begin_open_with_read_policy(
        self: &Arc<Self>,
        pin_physical_reads: bool,
    ) -> Result<SharedOpenState> {
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
            pin_physical_reads,
            connection_refreshed: false,
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
                if loaded.record().requires_recovery() {
                    self.failed = true;
                    return Err(LimboError::InvalidArgument(
                        "shared manifest recovery is required".into(),
                    )
                    .into());
                }
                let bindings = (|| {
                    let guard = loaded.guard()?;
                    Ok::<_, LimboError>(if self.pin_physical_reads {
                        vec![
                            self.runtime.main.bind_reads(guard)?,
                            self.runtime.wal.bind_reads(guard)?,
                        ]
                    } else {
                        vec![
                            self.runtime.main.bind(guard)?,
                            self.runtime.wal.bind(guard)?,
                        ]
                    })
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

    pub(crate) fn capture_connection_generation(
        &self,
    ) -> Result<crate::storage::pager::SharedPagerGeneration> {
        self.runtime
            .captured_pager_generation(self.loaded.as_ref().ok_or(LimboError::Busy)?.record())
    }

    pub(crate) fn finish_connection<C: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        store: &MvStore<C, A>,
        connection: &Arc<Connection>,
        captured: crate::storage::pager::SharedPagerGeneration,
    ) -> IOResultOr<()> {
        if !self.connection_refreshed {
            if self.refresh.is_none() {
                let replay = self.runtime.replay.lock().clone().ok_or(LimboError::Busy)?;
                self.refresh = Some(
                    replay.refresh(self.loaded.take().unwrap(), self.runtime.logical.clone())?,
                );
            }
            self.loaded = Some(return_if_io!(self
                .refresh
                .as_mut()
                .unwrap()
                .poll(store, connection)));
            self.connection_refreshed = true;
        }
        let guard = self.loaded.as_ref().ok_or(LimboError::Busy)?.guard()?;
        if !guard.io_retired() {
            return Ok(IOResult::IO(crate::types::IOCompletions(
                guard.retirement_completion(),
            )));
        }
        self.runtime
            .certify_current_pager(connection, Some(captured), guard)?;
        self.loaded.take().unwrap().into_guard()?.finish()?;
        self.bindings.clear();
        self.admission = None;
        Ok(IOResult::Done(()))
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
    persistent_timestamp: u64,
    ids_load: Option<IdLoad>,
    ids_mirror: Option<IdWitnessMirror>,
    post: Option<BootstrapState>,
    tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
}

impl SharedBootstrapState {
    pub(crate) fn is_complete(&self) -> bool {
        matches!(self.stage, SharedBootstrapStage::Done)
    }
}

#[derive(Default)]
enum SharedBootstrapStage {
    #[default]
    Start,
    Reparse,
    Base,
    Replay,
    Incarnation,
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
                        state.persistent_timestamp = persistent;
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
                        let record = loaded.record();
                        let guard = loaded.into_guard()?;
                        open.refresh = None;
                        state.ids_load = Some(runtime.ids.load(
                            guard,
                            RecoveryIdentity {
                                database: DatabaseGeneration {
                                    generation: record.log_generation(),
                                    checkpoint_timestamp: state.persistent_timestamp,
                                },
                                prefix: record.prefix(),
                            },
                        )?);
                        state.stage = SharedBootstrapStage::Incarnation;
                    }
                    SharedBootstrapStage::Incarnation => {
                        if let Some(load) = state.ids_load.as_mut() {
                            let loaded = return_if_io!(load.poll());
                            state.ids_load = None;
                            if loaded.has_pending_checkpoint() {
                                return Err(LimboError::InvalidArgument(
                                    "shared checkpoint recovery is required".into(),
                                )
                                .into());
                            }
                            if loaded.guard().incarnation_ready()? {
                                let guard = loaded.into_guard();
                                runtime.certify_current_pager(connection, None, &guard)?;
                                guard.finish()?;
                            } else {
                                state.ids_mirror = Some(loaded.mirror_current_witness()?);
                            }
                        }
                        if let Some(mirror) = state.ids_mirror.as_mut() {
                            let proof = return_if_io!(mirror.poll());
                            state.ids_mirror = None;
                            let (mut guard, maximum) = proof.into_guard_and_max_owner_epoch();
                            guard.reconcile_incarnation(maximum.unwrap_or(0))?;
                            runtime.certify_current_pager(connection, None, &guard)?;
                            guard.finish()?;
                        }
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
            state.ids_load = None;
            state.ids_mirror = None;
        }
        result
    }
}

#[cfg(all(test, not(shuttle)))]
pub(crate) fn materialize_marked_fixture(path: &Path) -> u64 {
    use crate::io::OpenFlags;
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let db = crate::Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        OpenFlags::Create,
        crate::DatabaseOpts::new().with_mvcc(true),
        None,
        Arc::new(crate::EngineDialect),
    )
    .unwrap();
    let connection = db.connect(crate::ConnectOpts::new()).unwrap();
    connection.execute("CREATE TABLE preserved(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO preserved VALUES(1,'durable payload')").unwrap();
    connection
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let timestamp = db
        .get_mv_store()
        .as_ref()
        .unwrap()
        .durable_txid_max
        .load(Ordering::Acquire);
    assert!(timestamp > 0);
    connection.demote_to_mvcc_connection();
    connection.reparse_schema().unwrap();
    *db.schema.lock() = connection.schema.read().clone();
    connection.execute("BEGIN IMMEDIATE").unwrap();
    let pager = connection.pager.load().clone();
    io.block(|| {
        pager.with_header_mut(|header| {
            header.read_version = crate::storage::sqlite3_ondisk::RawVersion(SHARED_MVCC_VERSION);
            header.write_version = crate::storage::sqlite3_ondisk::RawVersion(SHARED_MVCC_VERSION);
        })
    })
    .unwrap();
    connection.execute("COMMIT").unwrap();
    connection
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    connection.close().unwrap();
    drop(connection);
    drop(pager);
    drop(db);
    timestamp
}

#[cfg(all(test, target_os = "linux", not(shuttle)))]
pub(crate) fn shared_runtime_fixture(dir: &Path) -> (PathBuf, u64) {
    let limit = 64 * 1024 * 1024;
    (shared_runtime_fixture_with_limit(dir, limit), limit)
}

#[cfg(all(test, target_os = "linux", not(shuttle)))]
pub(crate) fn shared_runtime_fixture_with_limit(dir: &Path, limit: u64) -> PathBuf {
    let path = dir.join("shared.db");
    let timestamp = materialize_marked_fixture(&path);
    crate::mvcc::mode_transition::prepare_test_namespace(&path, timestamp, limit).unwrap();
    path
}

#[cfg(all(test, not(shuttle)))]
mod tests {
    use super::*;
    #[test]
    fn shared_fixture_materializes_metadata_and_marker_through_pager() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("marked.db");
        let timestamp = materialize_marked_fixture(&path);
        assert!(timestamp > 0);
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[18..20], &[SHARED_MVCC_VERSION; 2]);
        assert!(!bytes.is_empty());
    }
}

pub(super) struct SharedTransactionCommit {
    runtime: Arc<SharedMvccRuntime>,
    load: Option<ManifestLoad>,
    refresh: Option<SharedRefresh>,
    loaded: Option<LoadedManifest>,
    driver: Option<crate::mvcc::shared_commit::SharedCommit>,
    guard: Option<crate::mvcc::shared_authority::AuthorityGuard>,
    timestamp: Option<u64>,
    durable: bool,
    id_load: Option<IdLoad>,
    id_budget: Option<u64>,
}

impl SharedTransactionCommit {
    pub(super) fn new(runtime: Arc<SharedMvccRuntime>) -> Result<Self> {
        let guard = runtime.files.authority.try_acquire()?;
        let load = ManifestLoad::new(runtime.files.manifest.clone(), guard, runtime.database_id)?;
        Ok(Self {
            runtime,
            load: Some(load),
            refresh: None,
            loaded: None,
            driver: None,
            guard: None,
            timestamp: None,
            durable: false,
            id_load: None,
            id_budget: None,
        })
    }

    fn poll_refresh<C: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        store: &MvStore<C, A>,
        connection: &Arc<Connection>,
        local: bool,
    ) -> IOResultOr<()> {
        if self.loaded.is_some() {
            return Ok(IOResult::Done(()));
        }
        if self.refresh.is_none() {
            let loaded = return_if_io!(self.load.as_mut().unwrap().poll());
            self.load = None;
            let replay = self.runtime.replay.lock().clone().ok_or_else(|| {
                LimboError::InvalidArgument("shared commit lacks replay context".into())
            })?;
            let refresh = replay.refresh(loaded, self.runtime.logical.clone())?;
            self.refresh = Some(if local {
                refresh.observe_local_commit(self.timestamp.unwrap())?
            } else {
                refresh
            });
        }
        let loaded = return_if_io!(self.refresh.as_mut().unwrap().poll(store, connection));
        self.refresh = None;
        self.loaded = Some(loaded);
        Ok(IOResult::Done(()))
    }

    pub(super) fn poll_prepare<C: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        store: &MvStore<C, A>,
        connection: &Arc<Connection>,
    ) -> IOResultOr<u64> {
        if let Some(timestamp) = self.timestamp {
            return Ok(IOResult::Done(timestamp));
        }
        if self.id_budget.is_none() {
            if self.id_load.is_none() {
                return_if_io!(self.poll_refresh(store, connection, false));
                let replay = self.runtime.replay.lock().clone().ok_or(LimboError::Busy)?;
                let identity = replay.identity()?;
                let guard = self.loaded.take().unwrap().into_guard()?;
                self.id_load = Some(self.runtime.ids.load(guard, identity)?);
            }
            let ids = return_if_io!(self.id_load.as_mut().unwrap().poll());
            self.id_load = None;
            if ids.has_pending_checkpoint() {
                return Err(LimboError::InvalidArgument(
                    "shared checkpoint recovery is required".into(),
                )
                .into());
            }
            self.id_budget = Some(ids.required_budget_bytes()?);
            self.load = Some(ManifestLoad::new(
                self.runtime.files.manifest.clone(),
                ids.into_guard(),
                self.runtime.database_id,
            )?);
        }
        return_if_io!(self.poll_refresh(store, connection, false));
        let timestamp = self
            .loaded
            .as_mut()
            .unwrap()
            .guard_mut()?
            .allocate_timestamp()?;
        self.timestamp = Some(timestamp);
        Ok(IOResult::Done(timestamp))
    }

    pub(super) fn begin_write(&mut self, record: LogRecord) -> Result<()> {
        self.driver = Some(crate::mvcc::shared_commit::SharedCommit::new(
            self.loaded.take().ok_or_else(|| {
                LimboError::InvalidArgument("shared commit prefix already consumed".into())
            })?,
            self.runtime.storage(),
            self.runtime.logical.clone(),
            record,
            self.id_budget.ok_or_else(|| {
                LimboError::InvalidArgument("shared commit lacks ID budget preflight".into())
            })?,
        )?);
        Ok(())
    }

    pub(super) fn poll_write(&mut self) -> IOResultOr<()> {
        let driver = self.driver.as_mut().unwrap();
        let result = driver.poll();
        self.durable |= driver.is_durable();
        match result? {
            IOResult::IO(io) => Ok(IOResult::IO(io)),
            IOResult::Done(guard) => {
                self.guard = Some(guard);
                self.driver = None;
                Ok(IOResult::Done(()))
            }
        }
    }

    pub(super) fn preflight_rejected(&self) -> bool {
        self.driver
            .as_ref()
            .is_some_and(|driver| driver.preflight_rejected())
    }

    pub(super) fn is_durable(&self) -> bool {
        self.durable
            || self
                .driver
                .as_ref()
                .is_some_and(|driver| driver.is_durable())
    }

    pub(super) fn finish_without_append(mut self) -> Result<()> {
        self.loaded
            .take()
            .ok_or_else(|| LimboError::InvalidArgument("shared read commit lost authority".into()))?
            .into_guard()?
            .finish()
    }

    pub(super) fn poll_finish<C: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        store: &MvStore<C, A>,
        connection: &Arc<Connection>,
    ) -> IOResultOr<()> {
        if let Some(guard) = self.guard.take() {
            self.load = Some(ManifestLoad::new(
                self.runtime.files.manifest.clone(),
                guard,
                self.runtime.database_id,
            )?);
        }
        return_if_io!(self.poll_refresh(store, connection, true));
        self.loaded.take().unwrap().into_guard()?.finish()?;
        Ok(IOResult::Done(()))
    }
}

#[cfg(all(test, target_os = "linux", not(shuttle)))]
#[test]
#[ignore = "exports a fixture for the normal-library diagnostic binary"]
fn shared_runtime_export_fixture_for_release_probe() {
    let directory =
        std::env::var_os("TURSO_SHARED_PROBE_DIR").expect("dedicated fixture directory required");
    let directory = Path::new(&directory);
    assert!(directory.is_dir());
    assert!(!directory.join("shared.db").exists());
    let (path, limit) = shared_runtime_fixture(directory);
    std::fs::write(directory.join("history-limit"), limit.to_string()).unwrap();
    println!("prepared fixture {}", path.display());
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_open_native_validates_ids_when_incarnation_is_already_ready() {
    use crate::{Database, OpenOptions};
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().unwrap();
    let (path, limit) = super::shared_runtime_fixture(dir.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let options = OpenOptions::new(Arc::new(crate::dialect::EngineDialect))
        .flags(crate::OpenFlags::ReadOnly)
        .with_shared_mvcc_for_test(limit);
    let first = Database::open(io.clone(), path.to_str().unwrap(), options.clone()).unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    let namespace = crate::mvcc::mode_transition::namespace_for(
        &path,
        FileId {
            dev: metadata.dev(),
            ino: metadata.ino(),
        },
    )
    .unwrap();
    let selector = namespace.join("ids-selector");
    let before = std::fs::read(&selector).unwrap();
    let contents = std::fs::read(&path).unwrap();
    let mut damaged = before.clone();
    damaged[0] ^= 1;
    damaged[4096] ^= 1;
    std::fs::write(&selector, &damaged).unwrap();
    let reopened = Database::open(io, path.to_str().unwrap(), options);
    std::fs::write(&selector, &before).unwrap();
    assert!(
        reopened.is_err(),
        "live authority readiness bypassed durable ID validation"
    );
    assert_eq!(std::fs::read(&path).unwrap(), contents);
    drop(first);
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_oracle_native_keeps_outer_snapshot_and_rejects_unresolved_state() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let db = crate::Database::open(
        io.clone(),
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let connection = db.connect(crate::ConnectOpts::new()).unwrap();
    let store = db.get_mv_store().as_ref().unwrap().clone();
    let base_timestamp = store.last_committed_tx_ts.load(Ordering::Acquire);
    connection
        .execute("UPDATE preserved SET value='oracle committed' WHERE id=1")
        .unwrap();
    connection.execute("BEGIN CONCURRENT").unwrap();
    connection.execute("SELECT * FROM preserved").unwrap();
    let store = db.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap();
    let outer = connection.get_mv_tx();
    let before = store.txs.iter().count();
    assert!(connection.get_pager().holds_read_lock());
    let guard = runtime.files.authority.try_acquire().unwrap();
    let mut load =
        ManifestLoad::new(runtime.files.manifest.clone(), guard, runtime.database_id).unwrap();
    let loaded = loop {
        match load.poll().unwrap() {
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(loaded) => break loaded,
        }
    };
    let identity = RecoveryIdentity {
        database: DatabaseGeneration {
            generation: loaded.record().log_generation(),
            checkpoint_timestamp: base_timestamp,
        },
        prefix: loaded.record().prefix(),
    };
    assert!(identity.prefix.committed_timestamp > identity.database.checkpoint_timestamp);
    let guard = loaded.into_guard().unwrap();
    let registrations = guard.live_registrations().unwrap().len();
    let mut stale = identity;
    stale.database.generation += 1;
    assert!(store
        .begin_shared_oracle(&connection, &guard, stale)
        .is_err());
    let oracle = store
        .begin_shared_oracle(&connection, &guard, identity)
        .unwrap();
    assert!(oracle.matches_guard(&guard));
    assert_eq!(store.txs.iter().count(), before + 1);
    assert_eq!(guard.live_registrations().unwrap().len(), registrations);
    assert!(!guard.io_retired(), "oracle scope must retain authority");
    assert_eq!(connection.get_mv_tx(), outer);
    let temporary = oracle.tx_id();
    drop(oracle);
    assert!(store.txs.get(&temporary).is_none());
    assert_eq!(store.txs.iter().count(), before);
    assert_eq!(connection.get_mv_tx(), outer);
    assert!(connection.get_pager().holds_read_lock());
    let outer_id = outer.unwrap().0;
    store
        .txs
        .get(&outer_id)
        .unwrap()
        .value()
        .state
        .store(TransactionState::Preparing(
            identity.prefix.committed_timestamp + 1,
        ));
    assert!(matches!(
        store.begin_shared_oracle(&connection, &guard, identity),
        Err(LimboError::Busy)
    ));
    store
        .txs
        .get(&outer_id)
        .unwrap()
        .value()
        .state
        .store(TransactionState::Active);
    guard.finish().unwrap();
    connection.execute("ROLLBACK").unwrap();
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_native_materializes_without_reclaiming_log_before_completion() {
    run_shared_checkpoint_fixture(false, |_| Arc::new(crate::io::PlatformIO::new().unwrap()));
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
pub(in crate::mvcc) struct SharedCompletedCheckpointFixture {
    pub proof: crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof,
    pub guard: crate::mvcc::shared_authority::AuthorityGuard,
    pub ids: crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::SuspendedPreparedIds,
    pub pause: crate::mvcc::checkpoint_gate::CheckpointPause,
    pub database: Arc<crate::Database>,
    pub connection: Arc<Connection>,
    pub io: Arc<dyn crate::IO>,
    pub path: PathBuf,
    pub namespace: PathBuf,
    pub limit: u64,
    pub directory: tempfile::TempDir,
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
pub(in crate::mvcc) struct SharedPreparedCheckpointFixture {
    pub token: crate::mvcc::commit_manifest::checkpoint_publication::CheckpointIntentToken,
    pub materialized: super::checkpoint_state_machine::MaterializedCheckpointProof,
    pub prepared: crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::PreparedIds,
    pub pause: crate::mvcc::checkpoint_gate::CheckpointPause,
    pub database: Arc<crate::Database>,
    pub connection: Arc<Connection>,
    pub io: Arc<dyn crate::IO>,
    pub path: PathBuf,
    pub namespace: PathBuf,
    pub limit: u64,
    pub directory: tempfile::TempDir,
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
pub(in crate::mvcc) fn shared_prepared_checkpoint_fixture(
    io_factory: impl FnOnce(&Path) -> Arc<dyn crate::IO>,
) -> SharedPreparedCheckpointFixture {
    run_shared_checkpoint_fixture(true, io_factory).unwrap()
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
pub(in crate::mvcc) fn shared_completed_checkpoint_fixture(
    io_factory: impl FnOnce(&Path) -> Arc<dyn crate::IO>,
) -> SharedCompletedCheckpointFixture {
    let fixture = shared_prepared_checkpoint_fixture(io_factory);
    let source = fixture.prepared.source_identity();
    let target = fixture.prepared.target();
    let log_path = fixture.namespace.join("logical");
    let log_before = std::fs::read(&log_path).unwrap();
    let mut publication = fixture
        .token
        .begin_completion(fixture.materialized, fixture.prepared)
        .unwrap();
    let completed = loop {
        match publication.poll().unwrap() {
            IOResult::IO(waiter) => fixture.io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(completed) => break completed,
        }
    };
    let (proof, mut guard, ids) = completed.into_parts();
    assert!(proof.matches_lease(&guard));
    assert_eq!(proof.source_identity(), Some(source));
    assert_eq!(proof.target_identity().database, target);
    assert!(ids.prepared_floor().unwrap() <= proof.materialized_floor());
    assert_eq!(guard.published().unwrap(), source.prefix);
    guard.publish_checkpoint(&proof).unwrap();
    assert_eq!(guard.published().unwrap(), proof.target_identity().prefix);
    assert_eq!(std::fs::read(&log_path).unwrap(), log_before);
    SharedCompletedCheckpointFixture {
        proof,
        guard,
        ids,
        pause: fixture.pause,
        database: fixture.database,
        connection: fixture.connection,
        io: fixture.io,
        path: fixture.path,
        namespace: fixture.namespace,
        limit: fixture.limit,
        directory: fixture.directory,
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
fn run_shared_checkpoint_fixture(
    complete: bool,
    io_factory: impl FnOnce(&Path) -> Arc<dyn crate::IO>,
) -> Option<SharedPreparedCheckpointFixture> {
    use crate::state_machine::{StateTransition, TransitionResult};
    use crate::storage::wal::CheckpointMode;
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io = io_factory(&path);
    let db = crate::Database::open(
        io.clone(),
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let connection = db.connect(crate::ConnectOpts::new()).unwrap();
    let store = db.get_mv_store().as_ref().unwrap().clone();
    let base_timestamp = store.last_committed_tx_ts.load(Ordering::Acquire);
    connection
        .execute("UPDATE preserved SET value='materialized shared row' WHERE id=1")
        .unwrap();
    let runtime = store.shared_runtime.get().unwrap();
    let main_before = std::fs::read(&path).unwrap();
    let logical_path = runtime.files.namespace.join("logical");
    let log_before = std::fs::read(&logical_path).unwrap();
    assert!(!log_before.is_empty());
    let mut unauthorized = CheckpointStateMachine::new(
        connection.get_pager(),
        store.clone(),
        connection.clone(),
        true,
        crate::SyncMode::Full,
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    assert!(
        matches!(unauthorized.step(&()), Err(LimboError::InvalidArgument(message)) if message.contains("mirrored intent"))
    );
    assert_eq!(std::fs::read(&path).unwrap(), main_before);
    assert_eq!(std::fs::read(&logical_path).unwrap(), log_before);
    let _pause = runtime.gate.try_pause_admission().unwrap();
    let guard = runtime.files.authority.try_acquire().unwrap();
    assert!(guard.live_registrations().unwrap().is_empty());
    let mut load =
        ManifestLoad::new(runtime.files.manifest.clone(), guard, runtime.database_id).unwrap();
    let loaded = loop {
        match load.poll().unwrap() {
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(loaded) => break loaded,
        }
    };
    let prefix = loaded.record().prefix();
    let initial_record = loaded.record();
    let preflight_source = RecoveryIdentity {
        database: DatabaseGeneration {
            generation: initial_record.log_generation(),
            checkpoint_timestamp: base_timestamp,
        },
        prefix,
    };
    let preflight_target = DatabaseGeneration {
        generation: initial_record.log_generation().checked_add(1).unwrap(),
        checkpoint_timestamp: prefix.committed_timestamp,
    };
    let mut ids_load = runtime
        .ids
        .load(loaded.into_guard().unwrap(), preflight_source)
        .unwrap();
    let loaded_ids = loop {
        match ids_load.poll().unwrap() {
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(loaded) => break loaded,
        }
    };
    loaded_ids.preflight_checkpoint(preflight_target).unwrap();
    let (suspended_ids, guard) = loaded_ids.suspend();
    let mut manifest_reload =
        ManifestLoad::new(runtime.files.manifest.clone(), guard, runtime.database_id).unwrap();
    let loaded = loop {
        match manifest_reload.poll().unwrap() {
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(loaded) => break loaded,
        }
    };
    assert_eq!(loaded.record(), initial_record);

    let mut publication = loaded.begin_checkpoint_intent(base_timestamp).unwrap();
    let intent = loop {
        match publication.poll().unwrap() {
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(intent) => break intent,
        }
    };
    let main_binding = runtime.main.bind(intent.guard()).unwrap();
    let wal_binding = runtime.wal.bind(intent.guard()).unwrap();
    let mut checkpoint = CheckpointStateMachine::new(
        connection.get_pager(),
        store.clone(),
        connection.clone(),
        true,
        crate::SyncMode::Full,
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    checkpoint
        .authorize_shared_materialization(&intent)
        .unwrap();
    assert!(checkpoint.materialized_checkpoint_proof(&intent).is_err());
    loop {
        match checkpoint.step(&()).unwrap() {
            TransitionResult::Continue => {}
            TransitionResult::Io(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            TransitionResult::Done(_) => break,
        }
    }
    let materialized = loop {
        match checkpoint.materialized_checkpoint_proof(&intent).unwrap() {
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(proof) => break proof,
        }
    };
    assert!(materialized.matches_intent(&intent));
    assert!(materialized.physical_floor() < 0);
    assert!(checkpoint.materialized_checkpoint_proof(&intent).is_err());
    let materialized_main = std::fs::read(&path).unwrap();
    assert_ne!(materialized_main, main_before);
    assert_eq!(&materialized_main[18..20], &[SHARED_MVCC_VERSION; 2]);
    assert!(
        materialized_main
            .windows(b"materialized shared row".len())
            .any(|window| window == b"materialized shared row"),
        "committed payload must reach main storage"
    );
    assert_eq!(std::fs::read(&logical_path).unwrap(), log_before);
    assert_eq!(store.storage.logical_log_offset(), prefix.end_offset);
    assert_eq!(
        store.durable_txid_max.load(Ordering::Acquire),
        prefix.committed_timestamp
    );
    assert_eq!(std::fs::metadata(runtime.wal_path()).unwrap().len(), 0);
    assert_eq!(intent.guard().published().unwrap(), prefix);
    while !intent.guard().io_retired() {
        io.wait_for_completion(intent.guard().retirement_completion())
            .unwrap();
    }
    drop(main_binding);
    drop(wal_binding);
    if complete {
        let record = intent.record();
        let target = DatabaseGeneration {
            generation: record.checkpoint_intent().unwrap().target_generation,
            checkpoint_timestamp: prefix.committed_timestamp,
        };
        let (token, guard) = intent.suspend();
        let loaded = suspended_ids.rejoin_source(guard).unwrap();
        let mut preparation = loaded
            .prepare_checkpoint(target, materialized.physical_floor())
            .unwrap();
        let prepared = loop {
            match preparation.poll().unwrap() {
                IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
                IOResult::Done(prepared) => break prepared,
            }
        };
        let namespace = runtime.files.namespace.clone();
        return Some(SharedPreparedCheckpointFixture {
            token,
            materialized,
            prepared,
            pause: _pause,
            database: db,
            connection,
            io,
            path,
            namespace,
            limit,
            directory,
        });
    }
    drop(suspended_ids);
    intent.into_guard().finish().unwrap();
    drop(_pause);
    let reopened = crate::Database::open(
        io,
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    );
    assert!(
        matches!(reopened, Err(LimboError::InvalidArgument(message)) if message.contains("shared manifest recovery is required"))
    );
    None
}

#[cfg(all(test, target_os = "linux", not(shuttle)))]
#[path = "shared_id_oracle_tests.rs"]
mod shared_id_oracle_tests;

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_native_completion_requires_id_finalization_before_admission() {
    let fixture =
        shared_completed_checkpoint_fixture(|_| Arc::new(crate::io::PlatformIO::new().unwrap()));
    let source = fixture.proof.source_identity().unwrap();
    let target = fixture.proof.target_identity();
    assert_eq!(target.database.generation, source.database.generation + 1);
    assert_eq!(target.prefix.end_offset, 0);
    assert_eq!(
        target.prefix.committed_timestamp,
        source.prefix.committed_timestamp
    );
    drop(fixture.ids);
    fixture.guard.finish().unwrap();
    drop(fixture.pause);
    let reopened = crate::Database::open(
        fixture.io,
        fixture.path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect))
            .with_shared_mvcc_for_test(fixture.limit),
    );
    assert!(
        matches!(reopened, Err(LimboError::InvalidArgument(message)) if message.contains("checkpoint recovery"))
    );
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "../../database/shared_physical_native_tests.rs"]
mod shared_physical_native_tests;

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_connection_tests.rs"]
mod shared_connection_tests;
