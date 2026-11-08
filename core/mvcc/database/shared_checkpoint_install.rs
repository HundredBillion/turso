#[path = "shared_downgrade_preparation.rs"]
mod shared_downgrade_preparation;
pub(in crate::mvcc) use shared_downgrade_preparation::InstalledDowngradeSource;

use super::*;
use crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof;
use crate::mvcc::database::shared_generation_reload::PreparedGenerationReload;
use crate::mvcc::shared_authority::AuthorityGuard;

#[derive(Clone, Copy)]
struct InstalledPhysicalLayout {
    page_size: u32,
    reserved: u8,
    pages: u32,
    bytes: u64,
}
impl InstalledPhysicalLayout {
    fn validate(&self, header: &DatabaseHeader, bytes: u64) -> Result<()> {
        if header.page_size.get() != self.page_size
            || header.reserved_space != self.reserved
            || header.database_size.get() != self.pages
            || bytes != self.bytes
        {
            return Err(LimboError::Corrupt(
                "installed physical layout changed before downgrade".into(),
            ));
        }
        Ok(())
    }
}

pub(in crate::mvcc) struct InstalledLocalCheckpoint {
    guard: AuthorityGuard,
    proof: CompletedCheckpointProof,
    pause: CheckpointPause,
    replay: Arc<SharedReplayContext>,
    schema: Arc<Schema>,
    original_connection: crate::sync::Weak<Connection>,
    physical_layout: InstalledPhysicalLayout,
}

impl InstalledLocalCheckpoint {
    fn validate_handoff(&self, connection: &Arc<Connection>) -> Result<Arc<SharedMvccRuntime>> {
        if !crate::sync::Weak::ptr_eq(&self.original_connection, &Arc::downgrade(connection)) {
            return Err(LimboError::InvalidArgument(
                "checkpoint handoff requires its original connection".into(),
            ));
        }
        let store = connection
            .db
            .get_mv_store()
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                LimboError::InvalidArgument("checkpoint handoff lost its store".into())
            })?;
        let runtime = store.shared_runtime.get().ok_or_else(|| {
            LimboError::InvalidArgument("checkpoint handoff lost its runtime".into())
        })?;
        let target = self.proof.target_identity();
        if !runtime.owns_authority(&self.guard)
            || !self.proof.matches_lease(&self.guard)
            || self.guard.published()? != target.prefix
            || target.prefix.end_offset != 0
            || target.prefix.running_crc != 0
            || target.database.checkpoint_timestamp != target.prefix.committed_timestamp
            || !self.replay.matches_oracle_identity(target)
            || store.last_committed_tx_ts.load(Ordering::Acquire)
                != target.prefix.committed_timestamp
            || store.durable_txid_max.load(Ordering::Acquire)
                != target.database.checkpoint_timestamp
            || !Arc::ptr_eq(&self.schema, &connection.db.schema.lock())
            || !Arc::ptr_eq(&self.schema, &connection.schema.read())
        {
            return Err(LimboError::InvalidArgument(
                "checkpoint handoff no longer matches installed metadata".into(),
            ));
        }
        let logical: Arc<dyn File> = runtime.logical.clone();
        let storage = runtime.storage.shared_commit_snapshot(&logical)?;
        if storage.offset != 0 || storage.running_crc != 0 || storage.header.is_some() {
            return Err(LimboError::InvalidArgument(
                "checkpoint handoff has nonempty append state".into(),
            ));
        }
        Ok(runtime.clone())
    }

    pub(in crate::mvcc) fn publish_ready(self, connection: Arc<Connection>) -> Result<()> {
        let runtime = self.validate_handoff(&connection)?;
        runtime.certify_completed_pager(&connection, &self.proof, &self.guard)?;
        *runtime.replay.lock() = Some(self.replay);
        let result = self.guard.finish();
        if result.is_err() {
            runtime.replay.lock().take();
        }
        drop(self.pause);
        result
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Manifest,
    Preflight,
    Truncate,
    Sync,
    Retire,
    Install,
    Finished,
}

pub(in crate::mvcc) struct SharedCheckpointLocalInstall<C: LogicalClock, A: ConcurrentAllocator> {
    store: Arc<MvStore<C, A>>,
    connection: Arc<Connection>,
    runtime: Arc<SharedMvccRuntime>,
    phase: Phase,
    load: Option<ManifestLoad>,
    record: Option<crate::mvcc::commit_manifest::ManifestRecord>,
    parts: Option<PhysicalBaseParts>,
    prepared: Option<PreparedGenerationReload<A>>,
    schema: Option<Arc<Schema>>,
    replay: Option<Arc<SharedReplayContext>>,
    pending: Option<Completion>,
    binding: Option<FileBinding>,
    guard: Option<AuthorityGuard>,
    proof: Option<CompletedCheckpointProof>,
    pause: Option<CheckpointPause>,
    prepare_generation: u64,
    physical_layout: Option<InstalledPhysicalLayout>,
}

impl VerifiedPhysicalBase {
    pub(in crate::mvcc) fn begin_local_install<C: LogicalClock, A: ConcurrentAllocator>(
        self,
        store: Arc<MvStore<C, A>>,
        connection: Arc<Connection>,
    ) -> Result<SharedCheckpointLocalInstall<C, A>> {
        let runtime = store.shared_runtime.get().cloned().ok_or_else(|| {
            LimboError::InvalidArgument("checkpoint installation requires shared runtime".into())
        })?;
        let (parts, published) = self.into_parts();
        validate_owner(&store, &connection, &runtime, &published)?;
        let PublishedSharedCheckpoint { finalized, pause } = published;
        let (ids, proof) = finalized.into_parts();
        if ids.identity() != proof.target_identity() || ids.has_pending_checkpoint() {
            return Err(LimboError::InvalidArgument(
                "checkpoint IDs are not finalized".into(),
            ));
        }
        let load = ManifestLoad::new(
            runtime.files.manifest.clone(),
            ids.into_guard(),
            runtime.database_id,
        )?;
        Ok(SharedCheckpointLocalInstall {
            store,
            connection,
            runtime,
            phase: Phase::Manifest,
            load: Some(load),
            record: None,
            parts: Some(parts),
            prepared: None,
            schema: None,
            replay: None,
            pending: None,
            binding: None,
            guard: None,
            proof: Some(proof),
            pause: Some(pause),
            prepare_generation: 0,
            physical_layout: None,
        })
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> SharedCheckpointLocalInstall<C, A> {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<InstalledLocalCheckpoint> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<InstalledLocalCheckpoint> {
        loop {
            if let Some(completion) = self.pending.take() {
                if !completion.finished() {
                    self.pending = Some(completion.clone());
                    return Ok(IOResult::IO(IOCompletions(completion)));
                }
                if let Some(error) = completion.get_error() {
                    return Err(error.into());
                }
            }
            match self.phase {
                Phase::Manifest => {
                    let loaded = return_if_io!(self.load.as_mut().unwrap().poll());
                    self.load = None;
                    let record = loaded.record();
                    let proof = self.proof.as_ref().unwrap();
                    let target = proof.target_identity();
                    if record.requires_recovery()
                        || record.database_id() != self.runtime.database_id
                        || record.log_generation() != target.database.generation
                        || record.prefix() != target.prefix
                        || record.log_salt() != 0
                        || target.prefix.end_offset != 0
                        || target.prefix.running_crc != 0
                        || target.database.checkpoint_timestamp != target.prefix.committed_timestamp
                        || !proof.matches_lease(loaded.guard()?)
                    {
                        return Err(LimboError::Corrupt(
                            "selected manifest is not the completed checkpoint target".into(),
                        )
                        .into());
                    }
                    self.record = Some(record);
                    self.guard = Some(loaded.into_guard()?);
                    self.phase = Phase::Preflight;
                }
                Phase::Preflight => {
                    self.validate_owner()?;
                    let guard = self.guard.as_ref().unwrap();
                    if !guard.io_retired() {
                        return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
                    }
                    let parts = self.parts.take().unwrap();
                    if parts.persistent_timestamp
                        != self
                            .proof
                            .as_ref()
                            .unwrap()
                            .target_identity()
                            .database
                            .checkpoint_timestamp
                    {
                        return Err(LimboError::Corrupt(
                            "physical checkpoint timestamp changed".into(),
                        )
                        .into());
                    }
                    self.prepare_generation = self.connection.prepare_context_generation();
                    self.prepare_generation
                        .checked_add(1)
                        .ok_or(LimboError::DatabaseFull)?;
                    let prepared = self.store.prepare_generation_reload(
                        parts.header,
                        &parts.schema,
                        parts.persistent_timestamp,
                    )?;
                    self.physical_layout = Some(InstalledPhysicalLayout {
                        page_size: parts.header.page_size.get(),
                        reserved: parts.header.reserved_space,
                        pages: parts.header.database_size.get(),
                        bytes: self.runtime.files.main.size()?,
                    });
                    let schema = parts.schema.clone();
                    let context = parts.into_context(&self.store);
                    let replay = SharedReplayContext::new(
                        context,
                        self.record.unwrap(),
                        self.runtime.storage(),
                    );
                    self.binding = Some(self.runtime.logical.bind_checkpoint(guard)?);
                    self.prepared = Some(prepared);
                    self.schema = Some(schema);
                    self.replay = Some(replay);
                    // A failed reclamation or install must never restore old-generation readiness.
                    self.runtime.replay.lock().take();
                    self.phase = Phase::Truncate;
                }
                Phase::Truncate => {
                    self.validate_owner()?;
                    self.pending = Some(
                        self.runtime
                            .logical
                            .truncate(0, Completion::new_trunc(|_| {}))?,
                    );
                    self.phase = Phase::Sync;
                }
                Phase::Sync => {
                    self.pending = Some(
                        self.runtime
                            .logical
                            .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?,
                    );
                    self.phase = Phase::Retire;
                }
                Phase::Retire => {
                    self.validate_owner()?;
                    let guard = self.guard.as_ref().unwrap();
                    if !guard.io_retired() {
                        return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
                    }
                    self.runtime.storage.adopt_shared_prefix(
                        None,
                        0,
                        0,
                        self.proof
                            .as_ref()
                            .unwrap()
                            .target_identity()
                            .database
                            .checkpoint_timestamp,
                    )?;
                    self.phase = Phase::Install;
                }
                Phase::Install => {
                    self.validate_owner()?;
                    if self.connection.prepare_context_generation() != self.prepare_generation {
                        return Err(LimboError::Busy.into());
                    }
                    self.connection.get_pager().reset_shared_generation()?;
                    self.prepared.take().unwrap().install(
                        &self.store,
                        self.proof.as_ref().unwrap(),
                        self.guard.as_ref().unwrap(),
                    )?;
                    let schema = self.schema.take().unwrap();
                    *self.connection.db.schema.lock() = schema.clone();
                    *self.connection.schema.write() = schema.clone();
                    self.connection.bump_prepare_context_generation();
                    self.phase = Phase::Finished;
                    return Ok(IOResult::Done(InstalledLocalCheckpoint {
                        guard: self.guard.take().unwrap(),
                        proof: self.proof.take().unwrap(),
                        pause: self.pause.take().unwrap(),
                        replay: self.replay.take().unwrap(),
                        schema,
                        original_connection: Arc::downgrade(&self.connection),
                        physical_layout: self.physical_layout.take().unwrap(),
                    }));
                }
                Phase::Finished => {
                    return Err(
                        LimboError::InvalidArgument("checkpoint installation ended".into()).into(),
                    )
                }
            }
        }
    }

    fn validate_owner(&self) -> Result<()> {
        let guard = self.guard.as_ref().unwrap();
        let target = self.proof.as_ref().unwrap().target_identity();
        if self
            .connection
            .db
            .get_mv_store()
            .as_ref()
            .is_none_or(|store| {
                Arc::as_ptr(store).cast::<()>() != Arc::as_ptr(&self.store).cast::<()>()
            })
            || !self.runtime.owns_authority(guard)
            || !self.proof.as_ref().unwrap().matches_lease(guard)
            || guard.published()? != target.prefix
            || self.runtime.files.main.file_id()? != Some(self.runtime.database_id)
            || self.runtime.files.physical_wal.size()? != 0
        {
            return Err(LimboError::InvalidArgument(
                "checkpoint installation owner changed".into(),
            ));
        }
        if !self.store.txs.is_empty() || !guard.live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        Ok(())
    }

    fn abort(&mut self) {
        self.parts = None;
        self.prepared = None;
        self.schema = None;
        self.replay = None;
        self.pending = None;
        self.binding = None;
        self.load = None;
        self.guard = None;
        self.proof = None;
        self.pause = None;
        self.phase = Phase::Finished;
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> Drop for SharedCheckpointLocalInstall<C, A> {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_install_fault_tests.rs"]
mod fault_tests;

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_checkpoint_ready_native_resumes_original_connection_and_new_commits() {
        let fixture = shared_completed_checkpoint_fixture(|_| {
            Arc::new(crate::io::PlatformIO::new().unwrap())
        });
        let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap().clone();
        let mut finalizer = fixture.ids.finalize(fixture.guard, fixture.proof).unwrap();
        let finalized = loop {
            match finalizer.poll().unwrap() {
                IOResult::IO(waiter) => fixture.io.wait_for_completion(waiter.0).unwrap(),
                IOResult::Done(finalized) => break finalized,
            }
        };
        let target = finalized.proof().target_identity();
        let published = PublishedSharedCheckpoint {
            finalized,
            pause: fixture.pause,
        };
        let mut reading = published
            .begin_physical_read(store.clone(), fixture.connection.clone())
            .unwrap();
        let verified = fixture.io.block(|| reading.poll()).unwrap();
        let mut installing = verified
            .begin_local_install(store, fixture.connection.clone())
            .unwrap();
        let installed = fixture.io.block(|| installing.poll()).unwrap();
        assert!(runtime.replay.lock().is_none());
        installed.publish_ready(fixture.connection.clone()).unwrap();
        assert_eq!(
            runtime.replay.lock().as_ref().unwrap().identity().unwrap(),
            target
        );
        assert!(runtime.gate.try_enter_admission().is_ok());
        fixture
            .connection
            .execute("UPDATE preserved SET value='committed after ready' WHERE id=1")
            .unwrap();
        assert!(
            runtime
                .replay
                .lock()
                .as_ref()
                .unwrap()
                .identity()
                .unwrap()
                .prefix
                .end_offset
                > 0
        );
        let options = crate::OpenOptions::new(fixture.database.dialect().clone())
            .with_shared_mvcc_for_test(fixture.limit);
        let fresh =
            crate::Database::open(fixture.io.clone(), fixture.path.to_str().unwrap(), options)
                .unwrap();
        let reader = fresh.connect(crate::ConnectOpts::new()).unwrap();
        let mut values = Vec::new();
        reader
            .prepare("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                values.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(values, ["committed after ready"]);
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_checkpoint_install_native_reclaims_then_installs_without_admission_release() {
        let fixture = shared_completed_checkpoint_fixture(|_| {
            Arc::new(crate::io::PlatformIO::new().unwrap())
        });
        let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap().clone();
        let mut finalizer = fixture.ids.finalize(fixture.guard, fixture.proof).unwrap();
        let finalized = loop {
            match finalizer.poll().unwrap() {
                IOResult::IO(waiter) => fixture.io.wait_for_completion(waiter.0).unwrap(),
                IOResult::Done(finalized) => break finalized,
            }
        };
        let target = finalized.proof().target_identity();
        let published = PublishedSharedCheckpoint {
            finalized,
            pause: fixture.pause,
        };
        let old_schema = fixture.database.schema.lock().clone();
        assert!(
            std::fs::metadata(fixture.namespace.join("logical"))
                .unwrap()
                .len()
                > 0
        );
        let mut reading = published
            .begin_physical_read(store.clone(), fixture.connection.clone())
            .unwrap();
        let verified = fixture.io.block(|| reading.poll()).unwrap();
        let mut installing = verified
            .begin_local_install(store.clone(), fixture.connection.clone())
            .unwrap();
        let installed = fixture.io.block(|| installing.poll()).unwrap();
        assert!(
            runtime.replay.lock().is_none(),
            "pager certification and readiness publication remain downstream"
        );
        assert_eq!(
            std::fs::metadata(fixture.namespace.join("logical"))
                .unwrap()
                .len(),
            0
        );
        let logical: Arc<dyn File> = runtime.logical.clone();
        let storage = runtime.storage.shared_commit_snapshot(&logical).unwrap();
        assert_eq!(
            storage.offset, 0,
            "local storage did not adopt the completed empty prefix"
        );
        assert_eq!(storage.running_crc, 0);
        assert!(storage.header.is_none());
        assert_eq!(
            store.last_committed_tx_ts.load(Ordering::Acquire),
            target.database.checkpoint_timestamp
        );
        assert!(!Arc::ptr_eq(&old_schema, &fixture.database.schema.lock()));
        assert!(Arc::ptr_eq(
            &fixture.database.schema.lock(),
            &fixture.connection.schema.read()
        ));
        for table in installed.schema.all_tables() {
            if let crate::schema::Table::BTree(table) = table.as_ref() {
                assert_eq!(
                    store.current_root_page(&MVTableId::from(-table.root_page)),
                    Some(table.root_page as u64)
                );
            }
        }
        assert!(installed.guard.io_retired());
        assert_eq!(installed.proof.target_identity(), target);
        assert!(matches!(
            runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        let durable_paths = [
            fixture.path.clone(),
            fixture.namespace.join("manifest"),
            fixture.namespace.join("logical"),
            fixture.namespace.join("physical-wal"),
            fixture.namespace.join("ids-a"),
            fixture.namespace.join("ids-b"),
            fixture.namespace.join("ids-selector"),
        ];
        let durable_before: Vec<_> = durable_paths
            .iter()
            .map(|path| std::fs::read(path).unwrap())
            .collect();
        assert!(matches!(
            fixture.connection.execute("SELECT value FROM preserved"),
            Err(LimboError::Busy)
        ));
        assert!(matches!(
            fixture.database.connect(crate::ConnectOpts::new()),
            Err(LimboError::Busy)
        ));
        assert!(runtime.replay.lock().is_none());
        assert!(store.txs.is_empty());
        assert!(installing.poll().is_err());
        drop(installing);
        drop(installed);
        assert!(runtime.replay.lock().is_none());
        let mut resumed_values = Vec::new();
        fixture
            .connection
            .prepare("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                resumed_values.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(resumed_values, ["materialized shared row"]);
        assert_eq!(
            runtime.replay.lock().as_ref().unwrap().identity().unwrap(),
            target
        );
        assert_eq!(
            store.last_committed_tx_ts.load(Ordering::Acquire),
            target.prefix.committed_timestamp
        );
        assert_eq!(
            store.durable_txid_max.load(Ordering::Acquire),
            target.database.checkpoint_timestamp
        );
        assert!(Arc::ptr_eq(
            &fixture.database.schema.lock(),
            &fixture.connection.schema.read()
        ));
        assert!(store.txs.is_empty());
        let guard = runtime.files.authority.try_acquire().unwrap();
        assert_eq!(guard.published().unwrap(), target.prefix);
        assert!(guard.live_registrations().unwrap().is_empty());
        guard.finish().unwrap();
        let durable_after: Vec<_> = durable_paths
            .iter()
            .map(|path| std::fs::read(path).unwrap())
            .collect();
        assert_eq!(
            durable_after, durable_before,
            "local readiness reconstruction changed durable files"
        );
        let options = crate::OpenOptions::new(fixture.database.dialect().clone())
            .with_shared_mvcc_for_test(fixture.limit);
        let fresh =
            crate::Database::open(fixture.io.clone(), fixture.path.to_str().unwrap(), options)
                .unwrap();
        let reader = fresh.connect(crate::ConnectOpts::new()).unwrap();
        let mut values = Vec::new();
        reader
            .prepare("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                values.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(values, ["materialized shared row"]);
    }
}
