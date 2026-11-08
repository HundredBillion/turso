use super::shared_current_base::{CurrentBaseInspection, InspectedCurrentBase};
use super::*;
use crate::alloc::TryClone;
use crate::storage::pager::SharedPagerGeneration;

#[derive(Default)]
pub(super) struct CurrentPrefixState {
    inspection: Option<CurrentBaseInspection>,
    pub(super) base: Option<InspectedCurrentBase>,
    evidence: Option<CurrentBaseEvidence>,
    replay: Option<Arc<SharedReplayContext>>,
    connection: Option<Weak<Connection>>,
    database_id: Option<usize>,
    pub(super) installed: bool,
    replayed: bool,
}

impl SharedOpenState {
    pub(super) fn poll_current_base(&mut self, database: &Arc<crate::Database>) -> IOResultOr<()> {
        return_if_io!(self.poll_manifest());
        if self.current.base.is_some() {
            return Ok(IOResult::Done(()));
        }
        if self.current.inspection.is_none() {
            let record = self.loaded.as_ref().ok_or(LimboError::Busy)?.record();
            let identity = self
                .runtime
                .replay
                .lock()
                .as_ref()
                .map(|r| r.identity())
                .transpose()?;
            if identity
                .is_some_and(|identity| identity.database.generation == record.log_generation())
            {
                return Ok(IOResult::Done(()));
            }
            if !database
                .get_mv_store()
                .as_ref()
                .ok_or(LimboError::Busy)?
                .txs
                .is_empty()
            {
                return Err(LimboError::Busy.into());
            }
            self.current.inspection = Some(CurrentBaseInspection::new(
                self.runtime.clone(),
                database.clone(),
                self.loaded.take().unwrap(),
            )?);
        }
        self.current.base = Some(return_if_io!(self
            .current
            .inspection
            .as_mut()
            .unwrap()
            .poll()));
        self.current.inspection = None;
        Ok(IOResult::Done(()))
    }

    pub(crate) fn poll_current_prefix<C: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        store: &MvStore<C, A>,
        connection: &Arc<Connection>,
        database_id: usize,
        captured: Option<SharedPagerGeneration>,
    ) -> IOResultOr<()> {
        let database = connection.get_source_database(database_id);
        database.validate_shared_runtime(&self.runtime)?;
        let owner = database
            .get_mv_store()
            .as_ref()
            .cloned()
            .ok_or(LimboError::Busy)?;
        if Arc::as_ptr(&owner).cast::<()>() != (store as *const MvStore<C, A>).cast::<()>()
            || !store
                .shared_runtime
                .get()
                .is_some_and(|runtime| Arc::ptr_eq(runtime, &self.runtime))
        {
            return Err(LimboError::InvalidArgument(
                "current-prefix Store belongs to another database".into(),
            )
            .into());
        }
        if let Some(saved) = &self.current.connection {
            if !saved.ptr_eq(&Arc::downgrade(connection))
                || self.current.database_id != Some(database_id)
            {
                return Err(LimboError::InvalidArgument(
                    "current-prefix owner changed across I/O".into(),
                )
                .into());
            }
        } else {
            self.current.connection = Some(Arc::downgrade(connection));
            self.current.database_id = Some(database_id);
        }
        if self.connection_refreshed {
            return Ok(IOResult::Done(()));
        }
        let pager = connection.get_pager_from_database_index(&database_id)?;
        if !self.current.replayed {
            if self.refresh.is_none() {
                return_if_io!(self.poll_current_base(&database));
                if let Some(inspected) = self.current.base.take() {
                    if database_id != MAIN_DB_ID {
                        return Err(LimboError::Busy.into());
                    }
                    inspected.validate()?;
                    if !store.txs.is_empty() || connection.get_mv_tx().is_some() {
                        return Err(LimboError::Busy.into());
                    }
                    database.validate_shared_pager_owner(&pager, &self.runtime)?;
                    let (evidence, manifest, parts) = inspected.into_parts();
                    let mut context = parts.into_context(store);
                    let functions =
                        MvStore::<C, A>::capture_table_valued_functions(&connection.schema.read());
                    let mut schema = context.current_schema.as_ref().try_clone()?;
                    MvStore::<C, A>::rehydrate_table_valued_functions(&mut schema, &functions);
                    let schema = Arc::new(schema);
                    context.current_schema = schema.clone();
                    context.preserved_table_valued_functions = functions;
                    self.runtime.replay.lock().take();
                    let prepared = store.prepare_current_generation_reload(&evidence, &manifest)?;
                    pager.reset_shared_generation()?;
                    prepared.install_current(store, &evidence, &manifest)?;
                    self.current.installed = true;
                    *database.schema.lock() = schema.clone();
                    *connection.schema.write() = schema;
                    connection.bump_prepare_context_generation();
                    let replay = SharedReplayContext::new(
                        context,
                        manifest.record(),
                        self.runtime.storage(),
                    );
                    self.refresh = Some(replay.refresh(manifest, self.runtime.logical.clone())?);
                    self.current.evidence = Some(evidence);
                    self.current.replay = Some(replay);
                } else {
                    let replay = self.runtime.replay.lock().clone().ok_or(LimboError::Busy)?;
                    self.refresh = Some(
                        replay
                            .refresh(self.loaded.take().unwrap(), self.runtime.logical.clone())?,
                    );
                }
            }
            self.loaded = Some(return_if_io!(self
                .refresh
                .as_mut()
                .unwrap()
                .poll(store, connection)));
            self.current.replayed = true;
        }
        let loaded = self.loaded.as_ref().unwrap();
        let guard = loaded.guard()?;
        if !guard.io_retired() {
            return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
        }
        if let Some(evidence) = self.current.evidence.as_ref() {
            evidence.validate_closed_store(store, loaded)?;
            let replay = self.current.replay.as_ref().unwrap();
            let identity = replay.identity()?;
            if identity != evidence.identity()
                || !replay.matches_oracle_identity(identity)
                || guard.published()? != identity.prefix
                || !guard.incarnation_ready()?
                || store.last_committed_tx_ts.load(Ordering::Acquire)
                    != identity.prefix.committed_timestamp
                || store.durable_txid_max.load(Ordering::Acquire)
                    != identity.database.checkpoint_timestamp
                || !Arc::ptr_eq(&database.schema.lock(), &connection.schema.read())
            {
                return Err(LimboError::InvalidArgument(
                    "local replay does not match selected current prefix".into(),
                )
                .into());
            }
            let generation = SharedPagerGeneration {
                database_id: self.runtime.database_id,
                generation: identity.database.generation,
                checkpoint_timestamp: identity.database.checkpoint_timestamp,
            };
            if captured.is_some_and(|captured| captured != generation) {
                return Err(LimboError::Busy.into());
            }
            database.validate_shared_pager_owner(&pager, &self.runtime)?;
            pager.ensure_shared_generation(generation)?;
            *self.runtime.replay.lock() = Some(replay.clone());
        } else {
            self.runtime
                .certify_database_pager(&database, &pager, captured, guard)?;
        }
        self.connection_refreshed = true;
        Ok(IOResult::Done(()))
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_current_prefix_native_finish_failure_withdraws_local_readiness() {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let (io, control) =
            super::super::shared_connection_tests::delaying_native_read(path.clone());
        let database = crate::Database::open(
            io.clone(),
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        let connection = database.connect(crate::ConnectOpts::new()).unwrap();
        connection
            .execute("UPDATE preserved SET value='logical only' WHERE id=1")
            .unwrap();
        let store = database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap().clone();
        runtime.replay.lock().take();
        let mut open = runtime.begin_read_pinned_open().unwrap();
        io.block(|| open.poll_current_prefix(&store, &connection, MAIN_DB_ID, None))
            .unwrap();
        assert!(runtime.replay.lock().is_some());
        control.delay_after_reads(1);
        let pending = runtime
            .main
            .pread(
                0,
                Completion::new_read(Arc::new(crate::Buffer::new_temporary(512)), |_| None),
            )
            .unwrap();
        let held = control.take_pending();
        assert!(matches!(
            open.finish_current_prefix(),
            Err(LimboError::Busy)
        ));
        assert!(
            runtime.replay.lock().is_none(),
            "failed authority release retained local readiness"
        );
        drop(pending);
        drop(open);
        assert!(matches!(
            runtime.files.authority.try_acquire(),
            Err(LimboError::Busy)
        ));
        held.complete();
        assert!(matches!(
            runtime.files.authority.try_acquire(),
            Err(LimboError::Busy)
        ));
        drop(held);
        let fresh = database.connect(crate::ConnectOpts::new()).unwrap();
        fresh.execute("SELECT value FROM preserved").unwrap();
        assert!(runtime.replay.lock().is_some());
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_current_prefix_native_owner_rejection_precedes_stable_replay_and_resume() {
        let directory_a = tempfile::tempdir().unwrap();
        let directory_b = tempfile::tempdir().unwrap();
        let (path_a, limit) = shared_runtime_fixture(directory_a.path());
        let (path_b, _) = shared_runtime_fixture(directory_b.path());
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let open_database = |path: &Path| {
            crate::Database::open(
                io.clone(),
                path.to_str().unwrap(),
                crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                    .with_shared_mvcc_for_test(limit),
            )
            .unwrap()
        };
        let database_a = open_database(&path_a);
        let database_b = open_database(&path_b);
        let connection_a = database_a.connect(crate::ConnectOpts::new()).unwrap();
        let connection_b = database_b.connect(crate::ConnectOpts::new()).unwrap();
        connection_a
            .execute("UPDATE preserved SET value='A logical prefix' WHERE id=1")
            .unwrap();
        let store_a = database_a.get_mv_store().as_ref().unwrap().clone();
        let store_b = database_b.get_mv_store().as_ref().unwrap().clone();
        let runtime_a = store_a.shared_runtime.get().unwrap();
        let runtime_b = store_b.shared_runtime.get().unwrap();
        let roots_b = store_b.table_id_to_rootpage.load_full();
        let frontier_b = store_b.last_committed_tx_ts.load(Ordering::Acquire);
        let schema_b = database_b.schema.lock().clone();
        let replay_b = runtime_b.replay.lock().as_ref().unwrap().clone();
        let mut open = runtime_a.begin_open().unwrap();
        io.block(|| open.poll_manifest()).unwrap();
        for (store, connection) in [(&store_b, &connection_a), (&store_a, &connection_b)] {
            assert!(matches!(
                io.block(|| open.poll_current_prefix(store, connection, MAIN_DB_ID, None)),
                Err(LimboError::InvalidArgument(_))
            ));
            assert!(open.refresh.is_none());
            assert!(open.loaded.is_some());
            assert!(open.current.connection.is_none());
            assert!(Arc::ptr_eq(
                &roots_b,
                &store_b.table_id_to_rootpage.load_full()
            ));
            assert_eq!(
                frontier_b,
                store_b.last_committed_tx_ts.load(Ordering::Acquire)
            );
            assert!(Arc::ptr_eq(&schema_b, &database_b.schema.lock()));
            assert!(Arc::ptr_eq(
                &replay_b,
                runtime_b.replay.lock().as_ref().unwrap()
            ));
        }
        io.block(|| open.poll_current_prefix(&store_a, &connection_a, MAIN_DB_ID, None))
            .unwrap();
        assert!(open.connection_refreshed);
        assert!(matches!(
            io.block(|| open.poll_current_prefix(&store_b, &connection_a, MAIN_DB_ID, None)),
            Err(LimboError::InvalidArgument(_))
        ));
        assert!(Arc::ptr_eq(
            &roots_b,
            &store_b.table_id_to_rootpage.load_full()
        ));
        assert_eq!(
            frontier_b,
            store_b.last_committed_tx_ts.load(Ordering::Acquire)
        );
        open.finish_current_prefix().unwrap();
    }
}
