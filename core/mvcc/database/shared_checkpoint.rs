use super::super::checkpoint_state_machine::MaterializedCheckpointProof;
#[path = "shared_checkpoint_base.rs"]
mod shared_checkpoint_base;
use super::*;
use crate::mvcc::checkpoint_gate::CheckpointPause;
use crate::mvcc::commit_manifest::checkpoint_publication::{
    CheckpointCompletionPublication, CheckpointIntentPublication, CheckpointIntentToken,
    MirroredCheckpointIntent,
};
use crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::{
    finalize::{FinalizedIds, IdCheckpointFinalize},
    IdCheckpointPrepare, SuspendedIds,
};
use crate::state_machine::{StateTransition, TransitionResult};
use crate::storage::wal::CheckpointMode;
pub(in crate::mvcc) use shared_checkpoint_base::InstalledDowngradeSource;

/// The caller must verify and install the new physical generation before releasing admission.
/// Logical history remains intact until that downstream proof authorizes reclamation.
pub(in crate::mvcc) struct PublishedSharedCheckpoint {
    pub finalized: FinalizedIds,
    pub pause: CheckpointPause,
}

pub(crate) fn checkpoint_shared(
    connection: Arc<Connection>,
    mode: CheckpointMode,
) -> Result<CheckpointResult> {
    if matches!(
        mode,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(_)
        } | CheckpointMode::Truncate {
            upper_bound_inclusive: Some(_)
        }
    ) {
        return Err(LimboError::InvalidArgument(
            "shared checkpoint does not support timestamp bounds".into(),
        ));
    }
    let store = connection
        .mv_store()
        .as_ref()
        .cloned()
        .ok_or_else(|| LimboError::InvalidArgument("shared checkpoint requires MVCC".into()))?;
    let io = connection.db.io.clone();
    let mut publication = SharedCheckpointPublication::new(store.clone(), connection.clone())?;
    let published = io.block(|| publication.poll())?;
    let result = publication
        .result
        .take()
        .expect("completed materialization result");
    let mut reading = published.begin_physical_read(store.clone(), connection.clone())?;
    let verified = io.block(|| reading.poll())?;
    let mut installing = verified.begin_local_install(store, connection.clone())?;
    let installed = io.block(|| installing.poll())?;
    installed.publish_ready(connection)?;
    Ok(result)
}

#[derive(Clone, Copy)]
enum Phase {
    Refresh,
    LoadIds,
    ReloadManifest,
    Intent,
    Materialize,
    Materialized,
    PrepareIds,
    Complete,
    FinalizeIds,
    Finished,
}

pub(in crate::mvcc) struct SharedCheckpointPublication<C: LogicalClock, A: ConcurrentAllocator> {
    store: Arc<MvStore<C, A>>,
    connection: Arc<Connection>,
    runtime: Arc<SharedMvccRuntime>,
    pause: Option<CheckpointPause>,
    phase: Phase,
    load: Option<ManifestLoad>,
    refresh: Option<SharedRefresh>,
    source: Option<RecoveryIdentity>,
    record: Option<crate::mvcc::commit_manifest::ManifestRecord>,
    target: Option<DatabaseGeneration>,
    id_load: Option<IdLoad>,
    ids: Option<SuspendedIds>,
    publication: Option<CheckpointIntentPublication>,
    intent: Option<MirroredCheckpointIntent>,
    checkpoint: Option<CheckpointStateMachine<C, A>>,
    result: Option<CheckpointResult>,
    bindings: Vec<FileBinding>,
    materialized: Option<MaterializedCheckpointProof>,
    token: Option<CheckpointIntentToken>,
    preparation: Option<IdCheckpointPrepare>,
    completion: Option<CheckpointCompletionPublication>,
    finalizer: Option<IdCheckpointFinalize>,
}
impl<C: LogicalClock, A: ConcurrentAllocator> SharedCheckpointPublication<C, A> {
    pub(in crate::mvcc) fn new(
        store: Arc<MvStore<C, A>>,
        connection: Arc<Connection>,
    ) -> Result<Self> {
        let runtime =
            store.shared_runtime.get().cloned().ok_or_else(|| {
                LimboError::InvalidArgument("checkpoint requires shared MVCC".into())
            })?;
        if connection.is_readonly(crate::MAIN_DB_ID) {
            return Err(LimboError::ReadOnly);
        }
        if !connection.db.get_mv_store().as_ref().is_some_and(|actual| {
            Arc::as_ptr(actual).cast::<()>() == Arc::as_ptr(&store).cast::<()>()
        }) {
            return Err(LimboError::InvalidArgument(
                "checkpoint connection belongs to another store".into(),
            ));
        }
        let pause = runtime.gate.try_pause_admission()?;
        let guard = runtime.files.authority.try_acquire()?;
        // Quiescence is bounded: active participants produce BUSY, never reader eviction.
        if !store.txs.is_empty() {
            return Err(LimboError::Busy);
        }
        let load = ManifestLoad::new(runtime.files.manifest.clone(), guard, runtime.database_id)?;
        Ok(Self {
            store,
            connection,
            runtime,
            pause: Some(pause),
            phase: Phase::Refresh,
            load: Some(load),
            refresh: None,
            source: None,
            record: None,
            target: None,
            id_load: None,
            ids: None,
            publication: None,
            intent: None,
            checkpoint: None,
            result: None,
            bindings: Vec::new(),
            materialized: None,
            token: None,
            preparation: None,
            completion: None,
            finalizer: None,
        })
    }
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<PublishedSharedCheckpoint> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn poll_inner(&mut self) -> IOResultOr<PublishedSharedCheckpoint> {
        loop {
            match self.phase {
                Phase::Refresh => {
                    if self.refresh.is_none() {
                        let loaded = return_if_io!(self.load.as_mut().unwrap().poll());
                        self.load = None;
                        if !loaded.guard()?.live_registrations()?.is_empty()
                            || !self.store.txs.is_empty()
                        {
                            return Err(LimboError::Busy.into());
                        }
                        let replay = self.runtime.replay.lock().clone().ok_or(LimboError::Busy)?;
                        self.refresh = Some(replay.refresh(loaded, self.runtime.logical.clone())?);
                    }
                    let loaded = return_if_io!(self
                        .refresh
                        .as_mut()
                        .unwrap()
                        .poll(&self.store, &self.connection));
                    self.refresh = None;
                    let source = self
                        .runtime
                        .replay
                        .lock()
                        .as_ref()
                        .ok_or(LimboError::Busy)?
                        .identity()?;
                    let target = DatabaseGeneration {
                        generation: source
                            .database
                            .generation
                            .checked_add(1)
                            .ok_or(LimboError::DatabaseFull)?,
                        checkpoint_timestamp: source.prefix.committed_timestamp,
                    };
                    self.record = Some(loaded.record());
                    self.source = Some(source);
                    self.target = Some(target);
                    self.id_load = Some(self.runtime.ids.load(loaded.into_guard()?, source)?);
                    self.phase = Phase::LoadIds;
                }
                Phase::LoadIds => {
                    let loaded = return_if_io!(self.id_load.as_mut().unwrap().poll());
                    self.id_load = None;
                    loaded.preflight_checkpoint(self.target.unwrap())?;
                    let (ids, guard) = loaded.suspend();
                    self.ids = Some(ids);
                    self.load = Some(ManifestLoad::new(
                        self.runtime.files.manifest.clone(),
                        guard,
                        self.runtime.database_id,
                    )?);
                    self.phase = Phase::ReloadManifest;
                }
                Phase::ReloadManifest => {
                    let loaded = return_if_io!(self.load.as_mut().unwrap().poll());
                    self.load = None;
                    if Some(loaded.record()) != self.record {
                        return Err(LimboError::Corrupt(
                            "checkpoint manifest changed under held authority".into(),
                        )
                        .into());
                    }
                    self.publication = Some(loaded.begin_checkpoint_intent(
                        self.source.unwrap().database.checkpoint_timestamp,
                    )?);
                    self.phase = Phase::Intent;
                }
                Phase::Intent => {
                    let intent = return_if_io!(self.publication.as_mut().unwrap().poll());
                    self.publication = None;
                    self.bindings
                        .push(self.runtime.main.bind_checkpoint(intent.guard())?);
                    self.bindings
                        .push(self.runtime.wal.bind_checkpoint(intent.guard())?);
                    let mut checkpoint = CheckpointStateMachine::new(
                        self.connection.get_pager(),
                        self.store.clone(),
                        self.connection.clone(),
                        true,
                        crate::SyncMode::Full,
                        crate::MAIN_DB_ID,
                        CheckpointMode::Truncate {
                            upper_bound_inclusive: None,
                        },
                    );
                    checkpoint.authorize_shared_materialization(&intent)?;
                    self.intent = Some(intent);
                    self.checkpoint = Some(checkpoint);
                    self.phase = Phase::Materialize;
                }
                Phase::Materialize => match self.checkpoint.as_mut().unwrap().step(&())? {
                    TransitionResult::Io(waiter) => return Ok(IOResult::IO(waiter)),
                    TransitionResult::Continue => {}
                    TransitionResult::Done(mut result) => {
                        result.release_guard();
                        self.result = Some(result);
                        self.phase = Phase::Materialized;
                    }
                },
                Phase::Materialized => {
                    let proof = return_if_io!(self
                        .checkpoint
                        .as_mut()
                        .unwrap()
                        .materialized_checkpoint_proof(self.intent.as_ref().unwrap()));
                    self.checkpoint = None;
                    self.bindings.clear();
                    let (token, guard) = self.intent.take().unwrap().suspend();
                    let ids = self.ids.take().unwrap().rejoin_source(guard)?;
                    self.preparation =
                        Some(ids.prepare_checkpoint(self.target.unwrap(), proof.physical_floor())?);
                    self.materialized = Some(proof);
                    self.token = Some(token);
                    self.phase = Phase::PrepareIds;
                }
                Phase::PrepareIds => {
                    let prepared = return_if_io!(self.preparation.as_mut().unwrap().poll());
                    self.preparation = None;
                    self.completion = Some(
                        self.token
                            .take()
                            .unwrap()
                            .begin_completion(self.materialized.take().unwrap(), prepared)?,
                    );
                    self.phase = Phase::Complete;
                }
                Phase::Complete => {
                    let completed = return_if_io!(self.completion.as_mut().unwrap().poll());
                    self.completion = None;
                    let (proof, mut guard, ids) = completed.into_parts();
                    guard.publish_checkpoint(&proof)?;
                    self.finalizer = Some(ids.finalize(guard, proof)?);
                    self.phase = Phase::FinalizeIds;
                }
                Phase::FinalizeIds => {
                    let finalized = return_if_io!(self.finalizer.as_mut().unwrap().poll());
                    self.finalizer = None;
                    self.phase = Phase::Finished;
                    return Ok(IOResult::Done(PublishedSharedCheckpoint {
                        finalized,
                        pause: self.pause.take().unwrap(),
                    }));
                }
                Phase::Finished => {
                    return Err(LimboError::InvalidArgument(
                        "shared checkpoint coordinator terminated".into(),
                    )
                    .into())
                }
            }
        }
    }
    fn abort(&mut self) {
        // Cleanup runs while the intent still owns authority and physical files remain bound.
        if let Some(mut checkpoint) = self.checkpoint.take() {
            let _ = checkpoint.cleanup_after_external_io_error(LimboError::Interrupt);
        }
        self.bindings.clear();
        self.load = None;
        self.refresh = None;
        self.id_load = None;
        self.ids = None;
        self.publication = None;
        self.intent = None;
        self.preparation = None;
        self.completion = None;
        self.finalizer = None;
        self.pause = None;
        self.phase = Phase::Finished;
    }
}
impl<C: LogicalClock, A: ConcurrentAllocator> Drop for SharedCheckpointPublication<C, A> {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_coordinator_native_publishes_target_without_reclaiming_log() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let options = crate::OpenOptions::new(Arc::new(crate::dialect::EngineDialect))
        .with_shared_mvcc_for_test(limit);
    let database =
        crate::Database::open(io.clone(), path.to_str().unwrap(), options.clone()).unwrap();
    let connection = database.connect(crate::ConnectOpts::new()).unwrap();
    let peer = crate::Database::open(io.clone(), path.to_str().unwrap(), options.clone()).unwrap();
    let writer = peer.connect(crate::ConnectOpts::new()).unwrap();
    writer
        .execute("UPDATE preserved SET value='coordinator committed payload' WHERE id=1")
        .unwrap();
    let store = database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap().clone();
    let before = std::fs::read(runtime.files.namespace.join("logical")).unwrap();
    assert!(!before.is_empty());
    let mut coordinator = SharedCheckpointPublication::new(store, connection).unwrap();
    let published = loop {
        match coordinator.poll().unwrap() {
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(published) => break published,
        }
    };
    assert_eq!(
        published.finalized.guard().published().unwrap(),
        published.finalized.proof().target_identity().prefix
    );
    assert_eq!(
        published
            .finalized
            .proof()
            .target_identity()
            .prefix
            .end_offset,
        0
    );
    assert_eq!(
        std::fs::read(runtime.files.namespace.join("logical")).unwrap(),
        before
    );
    assert!(matches!(
        runtime.gate.try_enter_admission(),
        Err(LimboError::Busy)
    ));
    assert!(coordinator.poll().is_err());
    drop(published);
    drop(coordinator);
    let reopened = crate::Database::open(io, path.to_str().unwrap(), options).unwrap();
    let reader = reopened.connect(crate::ConnectOpts::new()).unwrap();
    let mut values = Vec::new();
    reader
        .prepare("SELECT value FROM preserved WHERE id=1")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(values, ["coordinator committed payload"]);
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_coordinator_native_busy_preserves_reader_and_files() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let options = crate::OpenOptions::new(Arc::new(crate::dialect::EngineDialect))
        .with_shared_mvcc_for_test(limit);
    let db = crate::Database::open(io.clone(), path.to_str().unwrap(), options.clone()).unwrap();
    let connection = db.connect(crate::ConnectOpts::new()).unwrap();
    let peer = crate::Database::open(io.clone(), path.to_str().unwrap(), options).unwrap();
    let reader = peer.connect(crate::ConnectOpts::new()).unwrap();
    reader.execute("BEGIN; SELECT * FROM preserved").unwrap();
    let store = db.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap().clone();
    let manifest = std::fs::read(runtime.files.namespace.join("manifest")).unwrap();
    let main = std::fs::read(&path).unwrap();
    let mut coordinator = SharedCheckpointPublication::new(store, connection).unwrap();
    loop {
        match coordinator.poll() {
            Ok(IOResult::IO(waiter)) => io.wait_for_completion(waiter.0).unwrap(),
            Err(error) => {
                assert!(matches!(*error, LimboError::Busy));
                break;
            }
            Ok(IOResult::Done(_)) => panic!("checkpoint admitted a live reader"),
        }
    }
    assert_eq!(std::fs::read(&path).unwrap(), main);
    assert_eq!(
        std::fs::read(runtime.files.namespace.join("manifest")).unwrap(),
        manifest
    );
    assert!(runtime.gate.try_enter_admission().is_ok());
    reader.execute("SELECT * FROM preserved; COMMIT").unwrap();
    assert!(coordinator.poll().is_err());
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_coordinator_native_empty_prefix_and_readonly_refusal() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let options = crate::OpenOptions::new(Arc::new(crate::dialect::EngineDialect))
        .with_shared_mvcc_for_test(limit);
    let readonly = crate::Database::open(
        io.clone(),
        path.to_str().unwrap(),
        options.clone().flags(crate::OpenFlags::ReadOnly),
    )
    .unwrap();
    let reader = readonly.connect(crate::ConnectOpts::new()).unwrap();
    let readonly_store = readonly.get_mv_store().as_ref().unwrap().clone();
    assert!(matches!(
        SharedCheckpointPublication::new(readonly_store, reader),
        Err(LimboError::ReadOnly)
    ));
    let db = crate::Database::open(io.clone(), path.to_str().unwrap(), options).unwrap();
    let connection = db.connect(crate::ConnectOpts::new()).unwrap();
    let store = db.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap().clone();
    let source = runtime.replay.lock().as_ref().unwrap().identity().unwrap();
    assert_eq!(source.prefix.end_offset, 0);
    let mut coordinator = SharedCheckpointPublication::new(store, connection).unwrap();
    let result = loop {
        match coordinator.poll().unwrap() {
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            IOResult::Done(result) => break result,
        }
    };
    assert_eq!(
        result
            .finalized
            .proof()
            .target_identity()
            .database
            .generation,
        source.database.generation + 1
    );
    assert_eq!(
        result.finalized.proof().target_identity().prefix,
        source.prefix
    );
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_entry_tests.rs"]
mod shared_checkpoint_entry_tests;
