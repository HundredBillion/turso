use super::*;
use crate::database::shared_recovered::shared_source::SourceDatabaseFactory;
use crate::io::FileSyncType;
use crate::mvcc::commit_manifest::checkpoint_publication::CheckpointIntentInspection;
use crate::{Buffer, Completion};

pub(in crate::mvcc) struct SourceConstructionState {
    factory: Option<SourceDatabaseFactory>,
    inspection: Option<CheckpointIntentInspection>,
    phase: Arc<SourcePhysicalPhase>,
    source: Option<InspectedIntentSource>,
}

pub(in crate::mvcc) struct SourceStaging {
    pub(super) connection: Arc<Connection>,
    pub(super) pager: Arc<crate::Pager>,
    pub(super) database: Arc<crate::Database>,
    evidence: Arc<SourceConstructionEvidence>,
    aliases: SourceAliases,
    pub(super) source: InspectedIntentSource,
}

pub(in crate::mvcc::database) struct SourcePhysicalPhase {
    logical_binding: Mutex<Option<FileBinding>>,
    physical: Mutex<Option<Arc<SharedWalRecoveryLocks>>>,
    runtime: Arc<SharedMvccRuntime>,
    record: ManifestRecord,
    main_id: FileId,
    wal_id: FileId,
    manifest_id: FileId,
    coordination_id: FileId,
    files_created: AtomicBool,
    lease: Arc<PublicationLease>,
}

struct SourceFile {
    _binding: Option<FileBinding>,
    inner: Arc<GuardedFile>,
    phase: Arc<SourcePhysicalPhase>,
}

pub(crate) struct SourceConstructionEvidence {
    phase: Arc<SourcePhysicalPhase>,
    store: Weak<crate::MvStore>,
    config: Arc<RecoveryOpenConfig>,
    header: crate::storage::sqlite3_ondisk::DatabaseHeader,
    main: Arc<dyn File>,
    wal: Arc<dyn File>,
    logical: Arc<dyn File>,
}

pub(crate) struct SourceConstructionPermit {
    evidence: Arc<SourceConstructionEvidence>,
    database_incarnation: u64,
    storage: Weak<dyn crate::storage::database::DatabaseStorage>,
}

pub(crate) struct SourceAliases {
    database: Weak<crate::Database>,
    connection: Weak<Connection>,
    pager: Weak<crate::Pager>,
    main: Weak<dyn File>,
    wal: Weak<dyn File>,
    logical: Weak<dyn File>,
}

impl InspectedIntentSource {
    pub(in crate::mvcc) fn begin_private_source_construction(
        self,
    ) -> Result<SourceConstructionState> {
        self.owner.require_inspected_source()?;
        self.ids
            .validate_owner(&self.runtime, &self.config, self.owner.record)?;
        if !self.owner.store.txs.is_empty()
            || !self.owner.store.rows.is_empty()
            || self
                .owner
                .store
                .last_committed_tx_ts
                .load(Ordering::Acquire)
                != 0
            || self.owner.store.durable_txid_max.load(Ordering::Acquire) != 0
        {
            return Err(LimboError::InvalidArgument(
                "source Store changed before construction".into(),
            ));
        }
        let phase = SourcePhysicalPhase::new(&self)?;
        let inspection = self
            .ids
            .inspect_source_manifest(&phase.recovery_evidence()?)?;
        Ok(SourceConstructionState {
            factory: None,
            inspection: Some(inspection),
            phase,
            source: Some(self),
        })
    }
}

impl SourceConstructionState {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<SourceStaging> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<SourceStaging> {
        let source = self.source.as_ref().ok_or_else(|| {
            LimboError::InvalidArgument("private source construction has ended".into())
        })?;
        self.phase.validate()?;
        if let Some(inspection) = self.inspection.as_mut() {
            return_if_io!(inspection.poll(source.ids.guard()));
            self.inspection = None;
        }
        if self.factory.is_none() {
            let evidence = SourceConstructionEvidence::new(source, self.phase.clone())?;
            self.factory = Some(SourceDatabaseFactory::new(evidence)?);
        }
        return_if_io!(self.factory.as_mut().unwrap().poll());
        if !source.ids.guard().io_retired() {
            return Ok(IOResult::IO(IOCompletions(
                source.ids.guard().retirement_completion(),
            )));
        }
        let (database, connection, pager, evidence, aliases) =
            self.factory.take().unwrap().finish()?;
        Ok(IOResult::Done(SourceStaging {
            connection,
            pager,
            database,
            evidence,
            aliases,
            source: self.source.take().unwrap(),
        }))
    }

    fn abort(&mut self) {
        self.factory = None;
        self.inspection = None;
        self.source = None;
    }
}

impl Drop for SourceConstructionState {
    fn drop(&mut self) {
        self.abort();
    }
}

impl SourcePhysicalPhase {
    fn new(source: &InspectedIntentSource) -> Result<Arc<Self>> {
        let owner = &source.owner;
        let lifetime = owner
            .store
            .source_recovery_lifetime
            .as_ref()
            .ok_or(LimboError::Busy)?;
        let mut current = lifetime.phase.lock();
        if current.is_some() {
            return Err(LimboError::Busy);
        }
        source
            .ids
            .validate_owner(&source.runtime, &source.config, owner.record)?;
        let physical = SharedWalRecoveryLocks::acquire(source.runtime.physical_coordination())?;
        physical.validate_mapping(&source.runtime.files.physical_coordination)?;
        source.ids.guard().ensure_no_snapshot_locks()?;
        let logical_binding = source
            .runtime
            .logical
            .bind_physical_recovery(source.ids.guard(), physical.clone())?;
        let phase = Arc::new(Self {
            logical_binding: Mutex::new(Some(logical_binding)),
            physical: Mutex::new(Some(physical)),
            runtime: source.runtime.clone(),
            record: owner.record,
            main_id: owner.main_id,
            wal_id: owner.wal_id,
            manifest_id: owner.manifest_id,
            coordination_id: owner.coordination_id,
            files_created: AtomicBool::new(false),
            lease: lifetime.lease.clone(),
        });
        phase.validate()?;
        *current = Some(phase.clone());
        Ok(phase)
    }

    fn validate(&self) -> Result<()> {
        PublicationLease::pin(&Arc::downgrade(&self.lease))?;
        if self.runtime.original_readonly
            || self.runtime.replay.lock().is_some()
            || require_native_id(&self.runtime.files.main)? != self.main_id
            || require_native_id(&self.runtime.files.physical_wal)? != self.wal_id
            || require_native_id(&self.runtime.files.manifest)? != self.manifest_id
            || self.runtime.files.physical_coordination.file_id()? != Some(self.coordination_id)
            || self.record.database_id() != self.main_id
        {
            return Err(LimboError::InvalidArgument(
                "source physical phase changed its retained files".into(),
            ));
        }
        self.physical
            .lock()
            .as_ref()
            .ok_or(LimboError::Busy)?
            .validate_mapping(&self.runtime.files.physical_coordination)
    }

    fn recovery_evidence(&self) -> Result<RecoveryWalEvidence> {
        self.validate()?;
        Ok(RecoveryWalEvidence {
            runtime: self.runtime.clone(),
            physical: self
                .physical
                .lock()
                .as_ref()
                .ok_or(LimboError::Busy)?
                .clone(),
            record: self.record,
            lease: Arc::downgrade(&self.lease),
            main_id: self.main_id,
            wal_id: self.wal_id,
            manifest_id: self.manifest_id,
            coordination_id: self.coordination_id,
        })
    }

    fn file(
        self: &Arc<Self>,
        file: Arc<dyn File>,
        guard: &AuthorityGuard,
    ) -> Result<Arc<dyn File>> {
        self.validate()?;
        if !self.runtime.owns_authority(guard)
            || !Weak::ptr_eq(&Arc::downgrade(&self.lease), &guard.io_lease())
            || !(Arc::ptr_eq(&file, &self.runtime.files.main)
                || Arc::ptr_eq(&file, &self.runtime.files.physical_wal))
        {
            return Err(LimboError::InvalidArgument(
                "source file differs from its actual factory".into(),
            ));
        }
        guard.ensure_no_snapshot_locks()?;
        let inner = GuardedFile::new(file, &self.runtime.files.authority);
        let binding = inner.bind_physical_recovery(
            guard,
            self.physical
                .lock()
                .as_ref()
                .ok_or(LimboError::Busy)?
                .clone(),
        )?;
        Ok(Arc::new(SourceFile {
            _binding: Some(binding),
            inner,
            phase: self.clone(),
        }))
    }
}

impl File for SourceFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        self.inner.file_id()
    }
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.phase.validate()?;
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.phase.validate()?;
        self.inner.unlock_file()
    }
    fn size(&self) -> Result<u64> {
        self.phase.validate()?;
        self.inner.size()
    }
    fn pread(&self, pos: u64, completion: Completion) -> Result<Completion> {
        self.phase.validate()?;
        self.inner.pread(pos, completion)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, completion: Completion) -> Result<Completion> {
        self.phase.validate()?;
        self.inner.pwrite(pos, buffer, completion)
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        self.phase.validate()?;
        self.inner.sync(completion, kind)
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.phase.validate()?;
        self.inner.truncate(size, completion)
    }
}

impl SourceConstructionEvidence {
    fn new(source: &InspectedIntentSource, phase: Arc<SourcePhysicalPhase>) -> Result<Arc<Self>> {
        source
            .ids
            .validate_owner(&source.runtime, &source.config, source.owner.record)?;
        phase.validate()?;
        if phase.files_created.swap(true, Ordering::AcqRel) {
            return Err(LimboError::Busy);
        }
        let main = phase.file(source.runtime.files.main.clone(), source.ids.guard())?;
        let wal = phase.file(
            source.runtime.files.physical_wal.clone(),
            source.ids.guard(),
        )?;
        let logical: Arc<dyn File> = Arc::new(SourceFile {
            _binding: None,
            inner: source.runtime.logical.clone(),
            phase: phase.clone(),
        });
        Ok(Arc::new(Self {
            phase,
            store: Arc::downgrade(&source.owner.store),
            config: source.config.clone(),
            header: source.parts.header,
            main,
            wal,
            logical,
        }))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.phase.validate()?;
        if self
            .config
            .effective_readonly(self.phase.runtime.original_readonly)
        {
            return Err(LimboError::InvalidArgument(
                "shared checkpoint recovery requires a writable participant".into(),
            ));
        }
        let store = self.store.upgrade().ok_or(LimboError::Busy)?;
        if !store.txs.is_empty()
            || !store
                .shared_runtime
                .get()
                .is_some_and(|actual| Arc::ptr_eq(actual, &self.phase.runtime))
            || !store
                .source_recovery_lifetime
                .as_ref()
                .is_some_and(|lifetime| {
                    lifetime
                        .phase
                        .lock()
                        .as_ref()
                        .is_some_and(|phase| Arc::ptr_eq(phase, &self.phase))
                })
        {
            return Err(LimboError::InvalidArgument(
                "source constructor changed its actual Store".into(),
            ));
        }
        self.config.validate_history(
            self.phase
                .runtime
                .files
                .authority
                .configured_history_limit(),
        )
    }

    pub(crate) fn config(&self) -> &Arc<RecoveryOpenConfig> {
        &self.config
    }
    pub(crate) fn io(&self) -> Arc<dyn crate::IO> {
        self.phase.runtime.io.clone()
    }

    pub(crate) fn runtime(&self) -> &Arc<SharedMvccRuntime> {
        &self.phase.runtime
    }
    pub(crate) fn retained_store(&self) -> Result<Arc<crate::MvStore>> {
        self.validate()?;
        self.store.upgrade().ok_or(LimboError::Busy)
    }
    pub(crate) fn main_file(&self) -> Arc<dyn File> {
        self.main.clone()
    }
    pub(crate) fn path(&self) -> Result<&str> {
        self.phase
            .runtime
            .path
            .to_str()
            .ok_or_else(|| LimboError::InvalidArgument("source recovery path must be UTF-8".into()))
    }
    pub(crate) fn validate_header(
        &self,
        header: &crate::storage::sqlite3_ondisk::DatabaseHeader,
    ) -> Result<()> {
        if bytemuck::bytes_of(header) != bytemuck::bytes_of(&self.header) {
            return Err(LimboError::Corrupt(
                "source constructor changed its physical header".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn make_permit(
        self: &Arc<Self>,
        database: &crate::Database,
    ) -> Result<Arc<SourceConstructionPermit>> {
        self.validate()?;
        let permit = Arc::new(SourceConstructionPermit {
            evidence: self.clone(),
            database_incarnation: database.incarnation,
            storage: Arc::downgrade(&database.db_file),
        });
        permit.validate_database(database)?;
        Ok(permit)
    }
    pub(crate) fn capture_aliases(
        &self,
        database: &Arc<crate::Database>,
        connection: &Arc<Connection>,
        pager: &Arc<crate::Pager>,
    ) -> Result<SourceAliases> {
        self.validate()?;
        if !Arc::ptr_eq(database, &connection.db) || !Arc::ptr_eq(pager, &connection.get_pager()) {
            return Err(LimboError::InvalidArgument(
                "source aliases differ from their actual constructor".into(),
            ));
        }
        Ok(SourceAliases {
            database: Arc::downgrade(database),
            connection: Arc::downgrade(connection),
            pager: Arc::downgrade(pager),
            main: Arc::downgrade(&self.main),
            wal: Arc::downgrade(&self.wal),
            logical: Arc::downgrade(&self.logical),
        })
    }
}

impl SourceConstructionPermit {
    pub(crate) fn validate_database(&self, database: &crate::Database) -> Result<()> {
        self.evidence.validate()?;
        database.validate_shared_runtime(self.evidence.runtime())?;
        let storage = self.storage.upgrade().ok_or(LimboError::Busy)?;
        let store = self.evidence.retained_store()?;
        if database.incarnation != self.database_incarnation
            || !Arc::ptr_eq(&database.db_file, &storage)
            || database.is_readonly()
            || !database
                .get_mv_store()
                .as_ref()
                .is_some_and(|actual| Arc::ptr_eq(actual, &store))
        {
            return Err(LimboError::InvalidArgument(
                "source construction changed its private Database".into(),
            ));
        }
        if let Some(file) = database.shared_wal.read().runtime.file.as_ref() {
            if !Arc::ptr_eq(file, &self.evidence.wal) {
                return Err(LimboError::InvalidArgument(
                    "source construction attached another WAL".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn retained_wal(&self, database: &crate::Database) -> Result<Arc<dyn File>> {
        self.validate_database(database)?;
        Ok(self.evidence.wal.clone())
    }
}
