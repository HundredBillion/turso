use super::shared_current_base::{CurrentBaseInspection, InspectedCurrentBase};
use super::*;
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
        let database = connection.get_source_database(database_id);
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
                    let schema = parts.schema.clone();
                    let context = parts.into_context(store);
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
