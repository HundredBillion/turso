use super::*;
use crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof;

pub(in crate::mvcc::database) struct SourceStoreLifetime {
    lease: Arc<PublicationLease>,
}

impl std::fmt::Debug for SourceStoreLifetime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceStoreLifetime")
            .finish_non_exhaustive()
    }
}

impl SourceStoreLifetime {
    fn matches_lease(&self, lease: &Weak<PublicationLease>) -> bool {
        Weak::ptr_eq(&Arc::downgrade(&self.lease), lease)
    }
}

pub(in crate::mvcc) struct IntentRecoveryStore {
    store: Arc<crate::MvStore>,
    runtime: Arc<SharedMvccRuntime>,
    config: Arc<RecoveryOpenConfig>,
    lease: Weak<PublicationLease>,
    record: ManifestRecord,
    main_id: FileId,
    wal_id: FileId,
    manifest_id: FileId,
    coordination_id: FileId,
    main_bytes: u64,
    header: crate::storage::sqlite3_ondisk::DatabaseHeader,
    schema: Arc<Schema>,
    source_ids_seeded: bool,
}

pub(in crate::mvcc) struct SourceIntentStore {
    owner: IntentRecoveryStore,
    physical: RecoveredIntentPhysicalBase,
}

pub(in crate::mvcc) struct SourceIdInspection {
    owner: Option<IntentRecoveryStore>,
    parts: Option<PhysicalBaseParts>,
    config: Arc<RecoveryOpenConfig>,
    runtime: Arc<SharedMvccRuntime>,
    load: Option<crate::mvcc::shared_ids::io_protocol::intent_source::IntentSourceIdLoad>,
    pause: Option<crate::mvcc::checkpoint_gate::CheckpointPause>,
}

pub(in crate::mvcc) struct InspectedIntentSource {
    owner: IntentRecoveryStore,
    parts: PhysicalBaseParts,
    config: Arc<RecoveryOpenConfig>,
    runtime: Arc<SharedMvccRuntime>,
    ids: crate::mvcc::shared_ids::io_protocol::intent_source::LoadedSourceIds,
    pause: crate::mvcc::checkpoint_gate::CheckpointPause,
}

pub(in crate::mvcc::database::shared_runtime) struct SourceIdInspectionInput {
    owner: IntentRecoveryStore,
    parts: PhysicalBaseParts,
    config: Arc<RecoveryOpenConfig>,
    evidence: Arc<IntentSourceIdEvidence>,
}

pub(in crate::mvcc) struct IntentSourceIdEvidence {
    runtime: Arc<SharedMvccRuntime>,
    config: Arc<RecoveryOpenConfig>,
    record: ManifestRecord,
    lease: Weak<PublicationLease>,
    main_id: FileId,
    wal_id: FileId,
    manifest_id: FileId,
    coordination_id: FileId,
    main_bytes: u64,
    physical_floor: i64,
}

impl SourceIntentStore {
    pub(in crate::mvcc) fn begin_source_id_inspection(self) -> Result<SourceIdInspection> {
        self.owner.validate_source(&self.physical)?;
        let evidence = Arc::new(IntentSourceIdEvidence::new(&self)?);
        let SourceIntentStore { owner, physical } = self;
        let RecoveredIntentPhysicalBase {
            config,
            parts,
            intent,
        } = physical;
        intent.begin_source_id_inspection(SourceIdInspectionInput {
            owner,
            parts,
            config,
            evidence,
        })
    }
}

impl SourceIdInspectionInput {
    pub(in crate::mvcc::database::shared_runtime) fn begin(
        self,
        runtime: Arc<SharedMvccRuntime>,
        intent: crate::mvcc::commit_manifest::checkpoint_publication::MirroredCheckpointIntent,
        pause: crate::mvcc::checkpoint_gate::CheckpointPause,
    ) -> Result<SourceIdInspection> {
        let load = runtime.ids.inspect_intent_source(intent, self.evidence)?;
        Ok(SourceIdInspection {
            owner: Some(self.owner),
            parts: Some(self.parts),
            config: self.config,
            runtime,
            load: Some(load),
            pause: Some(pause),
        })
    }
}

impl SourceIdInspection {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<InspectedIntentSource> {
        let result = self.poll_inner();
        if result.is_err() {
            self.owner = None;
            self.load = None;
            self.parts = None;
            self.pause = None;
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<InspectedIntentSource> {
        let load = self
            .load
            .as_mut()
            .ok_or_else(|| LimboError::InvalidArgument("source ID inspection has ended".into()))?;
        let ids = return_if_io!(load.poll());
        self.load = None;
        let owner = self.owner.as_mut().unwrap();
        ids.validate_owner(&self.runtime, &self.config, owner.record)?;
        if owner.source_ids_seeded
            || !owner.store.rows.is_empty()
            || !owner.store.txs.is_empty()
            || owner.store.last_committed_tx_ts.load(Ordering::Acquire) != 0
            || owner.store.durable_txid_max.load(Ordering::Acquire) != 0
        {
            return Err(LimboError::InvalidArgument(
                "source Store changed before ID inspection".into(),
            )
            .into());
        }
        let next = ids
            .logical_floor()
            .checked_sub(1)
            .ok_or(LimboError::DatabaseFull)?;
        owner.store.next_table_id.fetch_min(next, Ordering::AcqRel);
        owner.source_ids_seeded = true;
        Ok(IOResult::Done(InspectedIntentSource {
            owner: self.owner.take().unwrap(),
            parts: self.parts.take().unwrap(),
            config: self.config.clone(),
            runtime: self.runtime.clone(),
            ids,
            pause: self.pause.take().unwrap(),
        }))
    }
}

impl IntentSourceIdEvidence {
    fn new(source: &SourceIntentStore) -> Result<Self> {
        source.owner.validate_source(&source.physical)?;
        let owner = &source.owner;
        let physical_floor =
            i64::try_from(owner.main_bytes / u64::from(owner.header.page_size.get()))
                .ok()
                .and_then(i64::checked_neg)
                .filter(|floor| *floor < 0 && *floor != i64::MIN)
                .ok_or(LimboError::DatabaseFull)?;
        Ok(Self {
            runtime: owner.runtime.clone(),
            config: owner.config.clone(),
            record: owner.record,
            lease: owner.lease.clone(),
            main_id: owner.main_id,
            wal_id: owner.wal_id,
            manifest_id: owner.manifest_id,
            coordination_id: owner.coordination_id,
            main_bytes: owner.main_bytes,
            physical_floor,
        })
    }

    pub(in crate::mvcc) fn validate(
        &self,
        ids: &Arc<SharedIds>,
        record: ManifestRecord,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        if !Arc::ptr_eq(ids, &self.runtime.ids)
            || record != self.record
            || !self.runtime.owns_authority(guard)
            || !Weak::ptr_eq(&self.lease, &guard.io_lease())
            || self.runtime.original_readonly
            || self.runtime.replay.lock().is_some()
            || require_native_id(&self.runtime.files.main)? != self.main_id
            || require_native_id(&self.runtime.files.physical_wal)? != self.wal_id
            || require_native_id(&self.runtime.files.manifest)? != self.manifest_id
            || self.runtime.files.physical_coordination.file_id()? != Some(self.coordination_id)
            || self.runtime.files.main.size()? != self.main_bytes
            || self.runtime.files.physical_wal.size()? != 0
        {
            return Err(LimboError::InvalidArgument(
                "source IDs changed their retained owner or files".into(),
            ));
        }
        self.config
            .validate_history(self.runtime.files.authority.configured_history_limit())?;
        guard.ensure_no_snapshot_locks()?;
        if !guard.io_retired() {
            return Err(LimboError::Busy);
        }
        Ok(())
    }

    pub(in crate::mvcc) fn identity(&self) -> RecoveryIdentity {
        RecoveryIdentity {
            database: DatabaseGeneration {
                generation: self.record.log_generation(),
                checkpoint_timestamp: self
                    .record
                    .checkpoint_intent()
                    .unwrap()
                    .source_checkpoint_timestamp,
            },
            prefix: self.record.prefix(),
        }
    }

    pub(in crate::mvcc) fn target(&self) -> DatabaseGeneration {
        DatabaseGeneration {
            generation: self.record.checkpoint_intent().unwrap().target_generation,
            checkpoint_timestamp: self.record.prefix().committed_timestamp,
        }
    }

    pub(in crate::mvcc) fn physical_floor(&self) -> i64 {
        self.physical_floor
    }

    pub(in crate::mvcc) fn validate_owner(
        &self,
        runtime: &Arc<SharedMvccRuntime>,
        config: &Arc<RecoveryOpenConfig>,
        record: ManifestRecord,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        if !Arc::ptr_eq(runtime, &self.runtime) || !Arc::ptr_eq(config, &self.config) {
            return Err(LimboError::InvalidArgument(
                "source IDs changed their retained configuration".into(),
            ));
        }
        self.validate(&runtime.ids, record, guard)
    }
}

impl RecoveredIntentPhysicalBase {
    pub(in crate::mvcc) fn begin_source_store(self) -> Result<SourceIntentStore> {
        validate_source_base(&self)?;
        let runtime = self.intent.retained_runtime().clone();
        let record = self.record();
        let lease = self.guard().io_lease();
        let pin = PublicationLease::pin(&lease)?;
        let authority_owner = lease.upgrade().ok_or(LimboError::Busy)?;
        drop(pin);
        let main_id = require_native_id(&runtime.files.main)?;
        let wal_id = require_native_id(&runtime.files.physical_wal)?;
        let manifest_id = require_native_id(&runtime.files.manifest)?;
        let coordination_id = runtime
            .files
            .physical_coordination
            .file_id()?
            .ok_or_else(|| {
                LimboError::InvalidArgument("source Store requires native coordination".into())
            })?;
        let main_bytes = runtime.files.main.size()?;
        let mut store = runtime
            .reserve_store()?
            .construct_store(self.config.store_allocator())?;
        Arc::get_mut(&mut store)
            .expect("fresh source Store has a single owner")
            .source_recovery_lifetime = Some(SourceStoreLifetime {
            lease: authority_owner,
        });
        let owner = IntentRecoveryStore {
            store,
            runtime,
            config: self.config.clone(),
            lease,
            record,
            main_id,
            wal_id,
            manifest_id,
            coordination_id,
            main_bytes,
            header: self.parts.header,
            schema: self.parts.schema.clone(),
            source_ids_seeded: false,
        };
        owner.validate_source(&self)?;
        Ok(SourceIntentStore {
            owner,
            physical: self,
        })
    }
}

impl IntentRecoveryStore {
    fn validate_source(&self, physical: &RecoveredIntentPhysicalBase) -> Result<()> {
        validate_source_base(physical)?;
        if !Arc::ptr_eq(&self.runtime, physical.intent.retained_runtime())
            || !Arc::ptr_eq(&self.config, &physical.config)
            || !Arc::ptr_eq(&self.schema, &physical.parts.schema)
            || !Weak::ptr_eq(&self.lease, &physical.guard().io_lease())
            || self
                .store
                .source_recovery_lifetime
                .as_ref()
                .is_none_or(|lifetime| !lifetime.matches_lease(&self.lease))
            || self.record != physical.record()
            || bytemuck::bytes_of(&self.header) != bytemuck::bytes_of(&physical.parts.header)
            || require_native_id(&self.runtime.files.main)? != self.main_id
            || require_native_id(&self.runtime.files.physical_wal)? != self.wal_id
            || require_native_id(&self.runtime.files.manifest)? != self.manifest_id
            || self.runtime.files.physical_coordination.file_id()? != Some(self.coordination_id)
            || self.runtime.files.main.size()? != self.main_bytes
            || !self
                .store
                .shared_runtime
                .get()
                .is_some_and(|actual| Arc::ptr_eq(actual, &self.runtime))
            || !self.store.txs.is_empty()
        {
            return Err(LimboError::InvalidArgument(
                "source Store changed its actual owner or base".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::mvcc::database::shared_runtime) fn original_config(
        &self,
    ) -> Arc<RecoveryOpenConfig> {
        self.config.clone()
    }

    pub(in crate::mvcc::database::shared_runtime) fn validate_normal_record(
        &self,
        runtime: &Arc<SharedMvccRuntime>,
        config: &Arc<RecoveryOpenConfig>,
        record: ManifestRecord,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        self.validate_owner(runtime, config, guard)?;
        let target = self.record.checkpoint_intent().unwrap().target_generation;
        if record.requires_recovery()
            || record.database_id() != self.record.database_id()
            || record.log_generation() != target
            || record.log_salt() != 0
            || record.sequence() <= self.record.sequence()
            || self.store.last_committed_tx_ts.load(Ordering::Acquire)
                != self.record.prefix().committed_timestamp
            || self.store.durable_txid_max.load(Ordering::Acquire)
                != self.record.prefix().committed_timestamp
            || record.prefix()
                != (crate::mvcc::shared_authority::PublishedPrefix {
                    committed_timestamp: self.record.prefix().committed_timestamp,
                    end_offset: 0,
                    running_crc: 0,
                })
        {
            return Err(LimboError::InvalidArgument(
                "owned Store differs from completed manifest".into(),
            ));
        }
        self.require_inspected_source()
    }

    pub(in crate::mvcc::database::shared_runtime) fn validate_completed(
        &self,
        runtime: &Arc<SharedMvccRuntime>,
        config: &Arc<RecoveryOpenConfig>,
        proof: &CompletedCheckpointProof,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        self.validate_owner(runtime, config, guard)?;
        let target = proof.target_identity();
        let intent = self.record.checkpoint_intent().unwrap();
        if !proof.matches_lease(guard)
            || proof.source_identity().is_some()
            || proof.source_database().is_some_and(|source| {
                source
                    != DatabaseGeneration {
                        generation: self.record.log_generation(),
                        checkpoint_timestamp: intent.source_checkpoint_timestamp,
                    }
            })
            || target.database.generation != intent.target_generation
            || target.database.checkpoint_timestamp != self.record.prefix().committed_timestamp
            || target.prefix.committed_timestamp != self.record.prefix().committed_timestamp
            || target.prefix.end_offset != 0
            || target.prefix.running_crc != 0
            || self.store.last_committed_tx_ts.load(Ordering::Acquire)
                != target.prefix.committed_timestamp
            || self.store.durable_txid_max.load(Ordering::Acquire)
                != target.database.checkpoint_timestamp
        {
            return Err(LimboError::InvalidArgument(
                "owned Store differs from actual completed checkpoint".into(),
            ));
        }
        self.require_inspected_source()
    }

    pub(in crate::mvcc::database::shared_runtime) fn into_bootstrap_store(
        self,
        runtime: &Arc<SharedMvccRuntime>,
        config: &Arc<RecoveryOpenConfig>,
        proof: &CompletedCheckpointProof,
        guard: &AuthorityGuard,
    ) -> Result<Arc<crate::MvStore>> {
        self.validate_completed(runtime, config, proof, guard)?;
        Ok(self.store)
    }

    fn validate_owner(
        &self,
        runtime: &Arc<SharedMvccRuntime>,
        config: &Arc<RecoveryOpenConfig>,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.runtime, runtime)
            || !Arc::ptr_eq(&self.config, config)
            || !runtime.owns_authority(guard)
            || !Weak::ptr_eq(&self.lease, &guard.io_lease())
            || runtime.replay.lock().is_some()
            || !self
                .store
                .shared_runtime
                .get()
                .is_some_and(|actual| Arc::ptr_eq(actual, runtime))
            || self
                .store
                .source_recovery_lifetime
                .as_ref()
                .is_none_or(|lifetime| !lifetime.matches_lease(&self.lease))
            || !self.store.txs.is_empty()
            || require_native_id(&runtime.files.main)? != self.main_id
            || require_native_id(&runtime.files.physical_wal)? != self.wal_id
            || require_native_id(&runtime.files.manifest)? != self.manifest_id
            || runtime.files.physical_coordination.file_id()? != Some(self.coordination_id)
            || runtime.files.physical_wal.size()? != 0
        {
            return Err(LimboError::InvalidArgument(
                "owned recovery Store changed its runtime, config or lease".into(),
            ));
        }
        config.validate_history(runtime.files.authority.configured_history_limit())?;
        guard.ensure_no_snapshot_locks()?;
        if !guard.io_retired() {
            return Err(LimboError::Busy);
        }
        Ok(())
    }

    fn require_inspected_source(&self) -> Result<()> {
        if !self.source_ids_seeded {
            return Err(LimboError::Busy);
        }
        Ok(())
    }
}

fn validate_source_base(physical: &RecoveredIntentPhysicalBase) -> Result<()> {
    let runtime = physical.intent.retained_runtime();
    let record = physical.record();
    let intent = record.checkpoint_intent().ok_or_else(|| {
        LimboError::InvalidArgument("source Store requires actual checkpoint intent".into())
    })?;
    physical
        .config
        .validate_history(runtime.files.authority.configured_history_limit())?;
    if runtime.original_readonly
        || !runtime.owns_authority(physical.guard())
        || runtime.replay.lock().is_some()
        || record.database_id() != runtime.database_id
        || runtime.files.main.file_id()? != Some(runtime.database_id)
        || physical.parts.persistent_timestamp != intent.source_checkpoint_timestamp
        || runtime.files.physical_wal.size()? != 0
    {
        return Err(LimboError::InvalidArgument(
            "source Store requires the retained unadmitted source base".into(),
        ));
    }
    physical.guard().ensure_no_snapshot_locks()?;
    if !physical.guard().io_retired() {
        return Err(LimboError::Busy);
    }
    validate_image(&physical.parts, record, runtime.files.main.size()?, None)
}

fn require_native_id(file: &Arc<dyn File>) -> Result<FileId> {
    file.file_id()?.ok_or_else(|| {
        LimboError::InvalidArgument("source Store requires retained native files".into())
    })
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;
    use crate::alloc::{AllocError, ApiAllocator, DynAllocator, Global, Layout};
    use crate::io::SharedWalLockKind;
    use crate::mvcc::database::shared_runtime::shared_checkpoint_intent_fixture::{
        CrashPoint, IntentCrashFixture,
    };
    use std::ptr::NonNull;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_factory_native_private_connection_stays_unadmitted_with_private_wal() {
        let (fixture, physical) = physical_case(
            CrashPoint::BeforeWalCommit,
            crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
        );
        let mut inspection = physical
            .begin_source_store()
            .unwrap()
            .begin_source_id_inspection()
            .unwrap();
        let inspected = fixture.io.block(|| inspection.poll()).unwrap();
        let store = Arc::downgrade(&inspected.owner.store);
        let before = durable(&fixture);
        let mut construction = inspected.begin_private_source_construction().unwrap();
        let staging = fixture.io.block(|| construction.poll()).unwrap();
        let actual_store = store.upgrade().unwrap();
        assert!(Arc::ptr_eq(
            staging.database.get_mv_store().as_ref().unwrap(),
            &actual_store
        ));
        assert!(Arc::ptr_eq(&staging.connection.get_pager(), &staging.pager));
        let wal = staging.pager.wal.as_ref().unwrap();
        let wal = wal
            .as_any()
            .downcast_ref::<crate::storage::wal::WalFile>()
            .unwrap();
        assert_eq!(wal.coordination_backend_name(), "in_process");
        assert!(matches!(
            staging.connection.prepare("SELECT 1"),
            Err(LimboError::Busy)
        ));
        assert!(matches!(
            staging.database.connect(crate::ConnectOpts::new()),
            Err(LimboError::Busy)
        ));
        assert!(actual_store.txs.is_empty());
        assert!(actual_store.rows.is_empty());
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(staging.source.ids.guard().requires_recovery());
        assert!(staging.source.ids.guard().published().is_err());
        assert!(staging.source.ids.guard().io_retired());
        assert!(matches!(
            crate::storage::shared_wal_coordination::SharedWalRecoveryLocks::acquire(
                fixture.runtime.physical_coordination()
            ),
            Err(LimboError::Busy)
        ));
        assert_durable(&before);
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_ids_native_genuine_source_seeds_actual_floor_without_publication() {
        let (fixture, physical) = physical_case(
            CrashPoint::BeforeWalCommit,
            crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
        );
        let before = durable(&fixture);
        let record = physical.record();
        let source = physical.begin_source_store().unwrap();
        let store = Arc::downgrade(&source.owner.store);
        let mut inspection = source.begin_source_id_inspection().unwrap();
        let inspected = fixture.io.block(|| inspection.poll()).unwrap();
        assert!(Arc::ptr_eq(
            &store.upgrade().unwrap(),
            &inspected.owner.store
        ));
        inspected.owner.require_inspected_source().unwrap();
        let identity = inspected.ids.identity();
        assert_eq!(identity.database.generation, record.log_generation());
        assert_eq!(
            identity.database.checkpoint_timestamp,
            record
                .checkpoint_intent()
                .unwrap()
                .source_checkpoint_timestamp
        );
        assert_eq!(identity.prefix, record.prefix());
        let floor = inspected.ids.logical_floor();
        assert!(floor < -1);
        assert_eq!(
            inspected.owner.store.next_table_id.load(Ordering::Acquire),
            floor - 1
        );
        assert!(inspected.owner.store.rows.is_empty());
        assert!(inspected.owner.store.txs.is_empty());
        assert_eq!(
            inspected
                .owner
                .store
                .last_committed_tx_ts
                .load(Ordering::Acquire),
            0
        );
        assert_eq!(
            inspected
                .owner
                .store
                .durable_txid_max
                .load(Ordering::Acquire),
            0
        );
        assert!(inspected.ids.guard().requires_recovery());
        assert!(inspected.ids.guard().published().is_err());
        assert!(inspected.ids.guard().io_retired());
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(matches!(
            fixture.runtime.begin_open(),
            Err(LimboError::Busy)
        ));
        assert_durable(&before);
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_store_native_genuine_source_owns_exactly_one_unadmitted_store() {
        let probe = Arc::new(AllocationProbe::default());
        let (fixture, physical) = physical_case(CrashPoint::BeforeWalCommit, options(&probe));
        let record = physical.record();
        let before = durable(&fixture);
        let allocations = probe.allocations.load(Ordering::Acquire);
        let source = physical.begin_source_store().unwrap();
        source.owner.validate_source(&source.physical).unwrap();
        assert!(Arc::ptr_eq(&source.owner.runtime, &fixture.runtime));
        assert!(Arc::ptr_eq(&source.owner.config, &source.physical.config));
        assert!(Arc::ptr_eq(
            source.owner.store.shared_runtime.get().unwrap(),
            &fixture.runtime
        ));
        assert_eq!(source.owner.record, record);
        assert!(Weak::ptr_eq(
            &source.owner.lease,
            &source.physical.guard().io_lease()
        ));
        assert!(probe.allocations.load(Ordering::Acquire) > allocations);
        assert_eq!(source.owner.store.next_table_id.load(Ordering::Acquire), -2);
        assert_eq!(
            source
                .owner
                .store
                .last_committed_tx_ts
                .load(Ordering::Acquire),
            0
        );
        assert_eq!(
            source.owner.store.durable_txid_max.load(Ordering::Acquire),
            0
        );
        assert!(source.owner.store.rows.is_empty());
        assert!(source.owner.store.txs.is_empty());
        assert!(matches!(
            source.owner.require_inspected_source(),
            Err(LimboError::Busy)
        ));
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(fixture.runtime.reserve_store().is_err());
        assert!(source.physical.guard().requires_recovery());
        assert!(source.physical.guard().published().is_err());
        assert!(source.physical.guard().io_retired());
        assert!(matches!(
            fixture.runtime.begin_open(),
            Err(LimboError::Busy)
        ));
        assert_durable(&before);
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_store_native_callee_error_drops_store_before_native_authority() {
        let probe = Arc::new(AllocationProbe::default());
        let (fixture, physical) = physical_case(CrashPoint::BeforeWalCommit, options(&probe));
        let file = fixture
            .io
            .open_file(
                fixture
                    .runtime
                    .files
                    .namespace
                    .join("authority")
                    .to_str()
                    .unwrap(),
                crate::OpenFlags::NoLock,
                false,
            )
            .unwrap();
        *probe.authority.lock() = Some(file.clone());
        let source = physical.begin_source_store().unwrap();
        let SourceIntentStore { owner, physical } = source;
        let store = Arc::downgrade(&owner.store);
        assert!(matches!(
            consume_physical_then_fail(physical),
            Err(LimboError::Interrupt)
        ));
        assert!(
            !native_publication_available(&file),
            "original guard error released native authority before Store destruction"
        );
        assert!(matches!(
            fixture.runtime.files.authority.try_acquire(),
            Err(LimboError::Busy)
        ));
        assert!(store.upgrade().is_some());
        probe.check_native.store(true, Ordering::Release);
        drop(owner);
        assert!(store.upgrade().is_none());
        assert!(probe.checked.load(Ordering::Acquire) > 0);
        assert!(
            !probe.early_release.load(Ordering::Acquire),
            "Store allocator observed released native authority during destruction"
        );
        assert!(native_publication_available(&file));
        let guard = fixture.runtime.files.authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        assert!(guard.published().is_err());
        assert!(
            fixture.runtime.reserve_store().is_err(),
            "source Store drop reset permanent Attached state"
        );
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_store_native_retained_store_clone_keeps_native_authority() {
        let probe = Arc::new(AllocationProbe::default());
        let (fixture, physical) = physical_case(CrashPoint::BeforeWalCommit, options(&probe));
        let file = fixture
            .io
            .open_file(
                fixture
                    .runtime
                    .files
                    .namespace
                    .join("authority")
                    .to_str()
                    .unwrap(),
                crate::OpenFlags::NoLock,
                false,
            )
            .unwrap();
        *probe.authority.lock() = Some(file.clone());
        let SourceIntentStore { owner, physical } = physical.begin_source_store().unwrap();
        let retained_store = owner.store.clone();
        let store = Arc::downgrade(&retained_store);
        assert!(matches!(
            consume_physical_then_fail(physical),
            Err(LimboError::Interrupt)
        ));
        probe.check_native.store(true, Ordering::Release);
        drop(owner);
        assert!(store.upgrade().is_some());
        assert_eq!(probe.checked.load(Ordering::Acquire), 0);
        assert!(
            !native_publication_available(&file),
            "owner drop released native authority while a real Store clone survived"
        );
        assert!(matches!(
            fixture.runtime.files.authority.try_acquire(),
            Err(LimboError::Busy)
        ));
        drop(retained_store);
        assert!(store.upgrade().is_none());
        assert!(probe.checked.load(Ordering::Acquire) > 0);
        assert!(!probe.early_release.load(Ordering::Acquire));
        assert!(native_publication_available(&file));
        let guard = fixture.runtime.files.authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        assert!(guard.published().is_err());
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_store_native_allocation_failure_keeps_unused_claim_free() {
        let probe = Arc::new(AllocationProbe::default());
        let (fixture, physical) = physical_case(CrashPoint::BeforeWalCommit, options(&probe));
        let before = durable(&fixture);
        let attempts = probe.attempts.load(Ordering::Acquire);
        probe.fail.store(true, Ordering::Release);
        assert!(physical.begin_source_store().is_err());
        assert!(probe.attempts.load(Ordering::Acquire) > attempts);
        assert!(fixture.runtime.reserve_store().is_ok());
        assert!(fixture.runtime.replay.lock().is_none());
        assert_durable(&before);
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_store_native_ack_base_cannot_allocate_source_store() {
        let probe = Arc::new(AllocationProbe::default());
        let (fixture, physical) =
            physical_case(CrashPoint::MainSyncedBeforeWalTruncate, options(&probe));
        let before = durable(&fixture);
        let attempts = probe.attempts.load(Ordering::Acquire);
        assert!(matches!(
            physical.begin_source_store(),
            Err(LimboError::InvalidArgument(_))
        ));
        assert_eq!(probe.attempts.load(Ordering::Acquire), attempts);
        assert!(fixture.runtime.reserve_store().is_ok());
        assert!(fixture.runtime.replay.lock().is_none());
        assert_durable(&before);
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_store_native_validates_original_owner_config_and_lease() {
        let (fixture, physical) = physical_case(
            CrashPoint::BeforeWalCommit,
            crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
        );
        let mut source = physical.begin_source_store().unwrap();
        let (other_fixture, other) = physical_case(
            CrashPoint::BeforeWalCommit,
            crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
        );
        assert!(source.owner.validate_source(&other).is_err());
        let original = source.physical.config.clone();
        source.physical.config =
            RecoveryOpenConfig::capture(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
                .unwrap();
        assert!(source.owner.validate_source(&source.physical).is_err());
        source.physical.config = original;
        let lease = source.owner.lease.clone();
        source.owner.lease = Weak::new();
        assert!(source.owner.validate_source(&source.physical).is_err());
        source.owner.lease = lease;
        source.owner.validate_source(&source.physical).unwrap();
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(other_fixture.runtime.replay.lock().is_none());
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_store_native_owned_carrier_cannot_recapture_configuration() {
        let (fixture, physical) = physical_case(
            CrashPoint::BeforeWalCommit,
            crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
        );
        let SourceIntentStore { owner, physical } = physical.begin_source_store().unwrap();
        let source_before = durable(&fixture);
        let (_directory, _runtime, mut selected, before) = completed_selection();
        selected.continuation = RecoveryStoreContinuation::Owned(Box::new(owner));
        let error = match selected
            .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
        {
            Ok(_) => panic!("owned Store recaptured another configuration"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("original captured configuration"));
        assert_durable(&before);
        assert_durable(&source_before);
        assert!(physical.guard().requires_recovery());
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_store_native_owned_carrier_rejects_foreign_completed_selection() {
        let (fixture, physical) = physical_case(
            CrashPoint::BeforeWalCommit,
            crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
        );
        let SourceIntentStore { owner, physical } = physical.begin_source_store().unwrap();
        let source_before = durable(&fixture);
        let (_directory, _runtime, mut selected, before) = completed_selection();
        selected.continuation = RecoveryStoreContinuation::Owned(Box::new(owner));
        let error = match selected.begin_physical_read_owned() {
            Ok(_) => panic!("foreign completed selection reused another source Store"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("runtime, config or lease"));
        assert_durable(&before);
        assert_durable(&source_before);
        assert!(physical.guard().requires_recovery());
    }

    fn completed_selection() -> (
        tempfile::TempDir,
        Arc<SharedMvccRuntime>,
        super::super::super::shared_checkpoint_recovery::CheckpointRecoverySelection,
        Vec<(PathBuf, Vec<u8>)>,
    ) {
        let fixture =
            shared_completed_checkpoint_fixture(|_| Arc::new(crate::io::SyscallIO::new().unwrap()));
        let runtime = fixture
            .database
            .get_mv_store()
            .as_ref()
            .unwrap()
            .shared_runtime
            .get()
            .unwrap()
            .clone();
        drop(fixture.ids);
        drop(fixture.guard);
        drop(fixture.proof);
        drop(fixture.pause);
        drop(fixture.connection);
        drop(fixture.database);
        let mut open = runtime.begin_checkpoint_recovery().unwrap();
        let selected = fixture.io.block(|| open.poll()).unwrap();
        let before = [
            fixture.path.clone(),
            runtime.files.namespace.join("manifest"),
            runtime.files.namespace.join("logical"),
            runtime.files.namespace.join("physical-wal"),
            runtime.files.namespace.join("ids-a"),
            runtime.files.namespace.join("ids-b"),
            runtime.files.namespace.join("ids-selector"),
        ]
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
        (fixture.directory, runtime, selected, before)
    }

    fn consume_physical_then_fail(physical: RecoveredIntentPhysicalBase) -> Result<()> {
        drop(physical);
        Err(LimboError::Interrupt)
    }

    fn physical_case(
        point: CrashPoint,
        options: crate::OpenOptions,
    ) -> (IntentCrashFixture, RecoveredIntentPhysicalBase) {
        let fixture = IntentCrashFixture::new(point, 1);
        let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
        let selected = fixture.io.block(|| opening.poll()).unwrap();
        let mut mirror = selected.begin_intent_mirror().unwrap();
        let intent = fixture.io.block(|| mirror.poll()).unwrap();
        let mut recovery = intent.begin_physical_wal_recovery(options).unwrap();
        let physical = fixture.io.block(|| recovery.poll()).unwrap();
        (fixture, physical)
    }

    fn durable(fixture: &IntentCrashFixture) -> Vec<(PathBuf, Vec<u8>)> {
        fixture
            .durable_before
            .iter()
            .map(|(path, _)| path.clone())
            .chain([
                fixture.runtime.files.namespace.join("manifest"),
                fixture.runtime.files.namespace.join("physical-tshm"),
            ])
            .map(|path| {
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect()
    }
    fn assert_durable(before: &[(PathBuf, Vec<u8>)]) {
        for (path, bytes) in before {
            assert_eq!(&std::fs::read(path).unwrap(), bytes);
        }
    }

    fn options(probe: &Arc<AllocationProbe>) -> crate::OpenOptions {
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).allocators(
            crate::DatabaseAllocators {
                mv_store: DynAllocator::new(ProbeAllocator(probe.clone())),
                ..Default::default()
            },
        )
    }

    fn native_publication_available(file: &Arc<dyn File>) -> bool {
        let available = file
            .shared_wal_try_lock_byte(1, true, SharedWalLockKind::LinuxOfd)
            .unwrap();
        if available {
            file.shared_wal_unlock_byte(1, SharedWalLockKind::LinuxOfd)
                .unwrap();
        }
        available
    }
    #[derive(Default)]
    struct AllocationProbe {
        attempts: AtomicUsize,
        allocations: AtomicUsize,
        checked: AtomicUsize,
        fail: AtomicBool,
        check_native: AtomicBool,
        early_release: AtomicBool,
        authority: Mutex<Option<Arc<dyn File>>>,
    }
    #[derive(Clone)]
    struct ProbeAllocator(Arc<AllocationProbe>);
    unsafe impl ApiAllocator for ProbeAllocator {
        fn allocate(&self, layout: Layout) -> std::result::Result<NonNull<[u8]>, AllocError> {
            self.0.attempts.fetch_add(1, Ordering::AcqRel);
            if self.0.fail.load(Ordering::Acquire) {
                return Err(AllocError);
            }
            let allocated = Global.allocate(layout)?;
            self.0.allocations.fetch_add(1, Ordering::AcqRel);
            Ok(allocated)
        }
        unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
            if self.0.check_native.load(Ordering::Acquire) {
                let file = self.0.authority.lock().clone().unwrap();
                if native_publication_available(&file) {
                    self.0.early_release.store(true, Ordering::Release);
                }
                self.0.checked.fetch_add(1, Ordering::AcqRel);
            }
            unsafe { Global.deallocate(pointer, layout) };
        }
    }
}

#[path = "shared_checkpoint_intent_source.rs"]
mod shared_checkpoint_intent_source;
