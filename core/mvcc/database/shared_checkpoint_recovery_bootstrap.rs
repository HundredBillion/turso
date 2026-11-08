use super::*;

#[allow(dead_code)]
pub(crate) struct RecoveredDatabaseReady {
    database: Arc<crate::Database>,
    connection: Arc<Connection>,
}
#[allow(dead_code)]
pub(crate) struct RecoveryBootstrapEvidence {
    store: Arc<crate::MvStore>,
    bindings: Vec<FileBinding>,
    prepared: Option<
        crate::mvcc::database::shared_generation_reload::PreparedGenerationReload<
            crate::alloc::DynAllocator,
        >,
    >,
    replay: Option<Arc<SharedReplayContext>>,
    parts: Option<PhysicalBaseParts>,
    header: DatabaseHeader,
    persistent_timestamp: u64,
    schema: Arc<Schema>,
    runtime: Arc<SharedMvccRuntime>,
    config: Arc<RecoveryOpenConfig>,
    record: crate::mvcc::commit_manifest::ManifestRecord,
    original_connection: Option<crate::sync::Weak<Connection>>,
    finalized: Option<FinalizedIds>,
    pause: Option<CheckpointPause>,
}

pub(crate) struct RecoveryConstructionPermit {
    runtime: crate::sync::Weak<SharedMvccRuntime>,
    store: crate::sync::Weak<crate::MvStore>,
    config: crate::sync::Weak<RecoveryOpenConfig>,
    lease: crate::sync::Weak<crate::mvcc::shared_authority::PublicationLease>,
    database_incarnation: u64,
    storage: crate::sync::Weak<dyn crate::storage::database::DatabaseStorage>,
}

impl RecoveryConstructionPermit {
    pub(crate) fn validate_source(&self, source: &RecoveryBootstrapEvidence) -> Result<()> {
        source.validate()?;
        let runtime = self.runtime.upgrade().ok_or(LimboError::Busy)?;
        let store = self.store.upgrade().ok_or(LimboError::Busy)?;
        let config = self.config.upgrade().ok_or(LimboError::Busy)?;
        if !Arc::ptr_eq(&runtime, &source.runtime)
            || !Arc::ptr_eq(&store, &source.store)
            || !Arc::ptr_eq(&config, &source.config)
            || !crate::sync::Weak::ptr_eq(&self.lease, &source.guard().io_lease())
        {
            return Err(LimboError::InvalidArgument(
                "recovery constructor resumed with another owner or lease".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn validate_database(&self, database: &crate::Database) -> Result<()> {
        crate::mvcc::shared_authority::PublicationLease::pin(&self.lease)?;
        let runtime = self.runtime.upgrade().ok_or(LimboError::Busy)?;
        let store = self.store.upgrade().ok_or(LimboError::Busy)?;
        let config = self.config.upgrade().ok_or(LimboError::Busy)?;
        let storage = self.storage.upgrade().ok_or(LimboError::Busy)?;
        database.validate_shared_runtime(&runtime)?;
        if database.incarnation != self.database_incarnation
            || !Arc::ptr_eq(&database.db_file, &storage)
            || !database
                .get_mv_store()
                .as_ref()
                .is_some_and(|actual| Arc::ptr_eq(actual, &store))
            || runtime.replay.lock().is_some()
            || config.effective_readonly(runtime.original_readonly) && !database.is_readonly()
        {
            return Err(LimboError::InvalidArgument(
                "recovery construction changed its private Database or Store".into(),
            ));
        }
        let file = database
            .shared_wal
            .read()
            .runtime
            .file
            .clone()
            .ok_or_else(|| {
                LimboError::InvalidArgument(
                    "recovery construction did not attach its retained WAL".into(),
                )
            })?;
        let expected = runtime.physical_wal_file();
        if !Arc::ptr_eq(&file, &expected)
            || file.file_id()? != expected.file_id()?
            || file.size()? != 0
        {
            return Err(LimboError::InvalidArgument(
                "recovery construction attached another WAL".into(),
            ));
        }
        Ok(())
    }
}

impl RecoveredDatabaseReady {
    pub(in crate::mvcc) fn into_parts(self) -> (Arc<crate::Database>, Arc<Connection>) {
        (self.database, self.connection)
    }
}

impl RecoveryBootstrapEvidence {
    pub(crate) fn install(&mut self, connection: &Arc<Connection>) -> Result<()> {
        self.validate()?;
        connection.db.validate_shared_runtime(&self.runtime)?;
        if !connection
            .db
            .get_mv_store()
            .as_ref()
            .is_some_and(|store| Arc::ptr_eq(store, &self.store))
        {
            return Err(LimboError::InvalidArgument(
                "recovery installation changed its Store".into(),
            ));
        }
        let parts = self.parts.take().ok_or_else(|| {
            LimboError::InvalidArgument("recovery base was already installed".into())
        })?;
        let generation = connection.prepare_context_generation();
        generation.checked_add(1).ok_or(LimboError::DatabaseFull)?;
        self.prepared = Some(self.store.prepare_generation_reload(
            parts.header,
            &parts.schema,
            parts.persistent_timestamp,
        )?);
        self.replay = Some(SharedReplayContext::new(
            parts.into_context(&self.store),
            self.record,
            self.runtime.storage(),
        ));
        if !self
            .replay
            .as_ref()
            .unwrap()
            .matches_oracle_identity(self.proof().target_identity())
        {
            return Err(LimboError::Corrupt(
                "recovered replay differs from the completed base".into(),
            ));
        }
        self.original_connection = Some(Arc::downgrade(connection));
        connection.get_pager().reset_shared_generation()?;
        self.prepared
            .take()
            .unwrap()
            .install(&self.store, self.proof(), self.guard())?;
        *connection.db.schema.lock() = self.schema.clone();
        *connection.schema.write() = self.schema.clone();
        connection.bump_prepare_context_generation();
        Ok(())
    }

    pub(crate) fn finish_ready(
        &mut self,
        database: Arc<crate::Database>,
        connection: Arc<Connection>,
    ) -> Result<RecoveredDatabaseReady> {
        self.validate()?;
        let target = self.proof().target_identity();
        if !self.original_connection.as_ref().is_some_and(|original| {
            crate::sync::Weak::ptr_eq(original, &Arc::downgrade(&connection))
        }) || !Arc::ptr_eq(&database, &connection.db)
            || !Arc::ptr_eq(&self.schema, &database.schema.lock())
            || !Arc::ptr_eq(&self.schema, &connection.schema.read())
            || self.store.last_committed_tx_ts.load(Ordering::Acquire)
                != target.prefix.committed_timestamp
            || self.store.durable_txid_max.load(Ordering::Acquire)
                != target.database.checkpoint_timestamp
            || !self
                .replay
                .as_ref()
                .is_some_and(|replay| replay.matches_oracle_identity(target))
        {
            return Err(LimboError::InvalidArgument(
                "recovered Database is not ready for admission".into(),
            ));
        }
        self.runtime
            .certify_completed_pager(&connection, self.proof(), self.guard())?;
        let (ids, _proof) = self.finalized.take().unwrap().into_parts();
        let guard = ids.into_guard();
        *self.runtime.replay.lock() = self.replay.take();
        if let Err(error) = guard.finish() {
            self.runtime.replay.lock().take();
            return Err(error);
        }
        self.bindings.clear();
        self.pause.take();
        Ok(RecoveredDatabaseReady {
            database,
            connection,
        })
    }

    fn guard(&self) -> &AuthorityGuard {
        self.finalized.as_ref().unwrap().guard()
    }
    fn proof(&self) -> &CompletedCheckpointProof {
        self.finalized.as_ref().unwrap().proof()
    }
    pub(crate) fn validate(&self) -> Result<()> {
        self.config
            .validate_history(self.runtime.files.authority.configured_history_limit())?;
        validate_geometry(
            &self.runtime,
            &self.header,
            self.persistent_timestamp,
            self.proof(),
            self.guard(),
        )?;
        if !self
            .store
            .shared_runtime
            .get()
            .is_some_and(|runtime| Arc::ptr_eq(runtime, &self.runtime))
        {
            return Err(LimboError::InvalidArgument(
                "recovery construction changed its actual Store".into(),
            ));
        }
        let target = self.proof().target_identity();
        let logical: Arc<dyn File> = self.runtime.logical.clone();
        let snapshot = self.runtime.storage.shared_commit_snapshot(&logical)?;
        if self.record.requires_recovery()
            || self.record.database_id() != self.runtime.database_id
            || self.record.log_generation() != target.database.generation
            || self.record.prefix() != target.prefix
            || self.record.log_salt() != 0
            || self.runtime.logical.size()? != 0
            || snapshot.offset != 0
            || snapshot.running_crc != 0
            || snapshot.header.is_some()
        {
            return Err(LimboError::InvalidArgument(
                "recovery construction lost its reclaimed target".into(),
            ));
        }
        Ok(())
    }
    fn bind_physical(&mut self) -> Result<()> {
        self.validate()?;
        if self.bindings.is_empty() {
            self.bindings = vec![
                self.runtime.main.bind_checkpoint(self.guard())?,
                self.runtime.wal.bind_checkpoint(self.guard())?,
            ];
        }
        Ok(())
    }
    pub(crate) fn runtime(&self) -> &Arc<SharedMvccRuntime> {
        &self.runtime
    }
    pub(crate) fn config(&self) -> &Arc<RecoveryOpenConfig> {
        &self.config
    }
    pub(crate) fn store(&self) -> &Arc<crate::MvStore> {
        &self.store
    }
    pub(crate) fn original_readonly(&self) -> bool {
        self.runtime.original_readonly
    }
    pub(crate) fn io(&self) -> Arc<dyn crate::IO> {
        self.runtime.io.clone()
    }
    pub(crate) fn path(&self) -> Result<&str> {
        self.runtime
            .path
            .to_str()
            .ok_or_else(|| LimboError::InvalidArgument("shared recovery path must be UTF-8".into()))
    }
    pub(crate) fn make_permit(
        &self,
        database: &crate::Database,
    ) -> Result<Arc<RecoveryConstructionPermit>> {
        self.validate()?;
        if self.bindings.len() != 2 {
            return Err(LimboError::Busy);
        }
        database.validate_shared_runtime(&self.runtime)?;
        if !database
            .get_mv_store()
            .as_ref()
            .is_some_and(|store| Arc::ptr_eq(store, &self.store))
        {
            return Err(LimboError::Busy);
        }
        Ok(Arc::new(RecoveryConstructionPermit {
            runtime: Arc::downgrade(&self.runtime),
            store: Arc::downgrade(&self.store),
            config: Arc::downgrade(&self.config),
            lease: self.guard().io_lease(),
            database_incarnation: database.incarnation,
            storage: Arc::downgrade(&database.db_file),
        }))
    }
    pub(crate) fn physical_retirement(&self) -> Option<Completion> {
        (!self.guard().io_retired()).then(|| self.guard().retirement_completion())
    }
    pub(crate) fn validate_header(&self, header: &DatabaseHeader) -> Result<()> {
        if bytemuck::bytes_of(header) != bytemuck::bytes_of(&self.header) {
            return Err(LimboError::Corrupt(
                "recovery constructor read a changed physical header".into(),
            ));
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryBootstrap {
    factory: Option<crate::database::shared_recovered::RecoveredDatabaseFactory>,
    evidence: Option<RecoveryBootstrapEvidence>,
    reclaimed: Option<ReclaimedRecoveredCheckpoint>,
}
#[allow(dead_code)]
impl ReclaimedRecoveredCheckpoint {
    pub(in crate::mvcc) fn begin_bootstrap(self) -> Result<RecoveryBootstrap> {
        Ok(RecoveryBootstrap {
            factory: None,
            evidence: None,
            reclaimed: Some(self),
        })
    }
}
#[allow(dead_code)]
impl RecoveryBootstrap {
    fn prepare_store(&mut self) -> Result<()> {
        if self.evidence.is_some() {
            return Ok(());
        }
        let reclaimed = self.reclaimed.take().ok_or_else(|| {
            LimboError::InvalidArgument("normal recovered bootstrap has ended".into())
        })?;
        let runtime = &reclaimed.recovered.runtime;
        reclaimed
            .recovered
            .config
            .validate_history(runtime.files.authority.configured_history_limit())?;
        validate_target(
            runtime,
            &reclaimed.recovered.parts,
            reclaimed.proof(),
            reclaimed.guard(),
        )?;
        match &reclaimed.recovered.continuation {
            RecoveryStoreContinuation::Fresh => reclaimed
                .reservation
                .as_ref()
                .ok_or_else(|| {
                    LimboError::InvalidArgument("fresh bootstrap lost its Store reservation".into())
                })?
                .validate(runtime)?,
            RecoveryStoreContinuation::Owned(owner) => {
                if reclaimed.reservation.is_some() {
                    return Err(LimboError::InvalidArgument(
                        "owned bootstrap cannot construct a second Store".into(),
                    ));
                }
                owner.validate_completed(
                    runtime,
                    &reclaimed.recovered.config,
                    reclaimed.proof(),
                    reclaimed.guard(),
                )?;
            }
        }
        let target = reclaimed.proof().target_identity();
        let logical: Arc<dyn File> = runtime.logical.clone();
        let snapshot = runtime.storage.shared_commit_snapshot(&logical)?;
        if !reclaimed.guard().io_retired() {
            return Err(LimboError::Busy);
        }
        if reclaimed.record.requires_recovery()
            || reclaimed.record.database_id() != runtime.database_id
            || reclaimed.record.log_generation() != target.database.generation
            || reclaimed.record.prefix() != target.prefix
            || reclaimed.record.log_salt() != 0
            || runtime.logical.size()? != 0
            || snapshot.offset != 0
            || snapshot.running_crc != 0
            || snapshot.header.is_some()
        {
            return Err(LimboError::InvalidArgument(
                "bootstrap no longer matches reclaimed checkpoint".into(),
            ));
        }
        let FinalizedRecoveredCheckpoint {
            continuation,
            runtime,
            config,
            parts,
            finalized,
            pause,
        } = reclaimed.recovered;
        let store = match continuation {
            RecoveryStoreContinuation::Fresh => reclaimed
                .reservation
                .ok_or_else(|| {
                    LimboError::InvalidArgument("fresh bootstrap lost its Store reservation".into())
                })?
                .construct_store(config.store_allocator())?,
            RecoveryStoreContinuation::Owned(owner) => owner.into_bootstrap_store(
                &runtime,
                &config,
                finalized.proof(),
                finalized.guard(),
            )?,
        };
        self.evidence = Some(RecoveryBootstrapEvidence {
            store,
            bindings: Vec::new(),
            prepared: None,
            replay: None,
            header: parts.header,
            persistent_timestamp: parts.persistent_timestamp,
            schema: parts.schema.clone(),
            parts: Some(parts),
            runtime,
            config,
            record: reclaimed.record,
            original_connection: None,
            finalized: Some(finalized),
            pause: Some(pause),
        });
        self.evidence.as_mut().unwrap().bind_physical()
    }
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredDatabaseReady> {
        let result = (|| {
            self.prepare_store()?;
            if self.factory.is_none() {
                self.factory = Some(
                    crate::database::shared_recovered::RecoveredDatabaseFactory::new(
                        self.evidence.as_ref().unwrap(),
                    )?,
                );
            }
            return_if_io!(self
                .factory
                .as_mut()
                .unwrap()
                .poll(self.evidence.as_mut().unwrap()));
            let ready = self
                .factory
                .as_mut()
                .unwrap()
                .finish(self.evidence.as_mut().unwrap())?;
            self.factory = None;
            self.evidence = None;
            Ok(IOResult::Done(ready))
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn abort(&mut self) {
        self.factory = None;
        if let Some(evidence) = self.evidence.take() {
            evidence.runtime.replay.lock().take();
            drop(evidence);
        }
        self.reclaimed = None;
    }
}
impl Drop for RecoveryBootstrap {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_recovery_bootstrap_tests.rs"]
mod tests;
