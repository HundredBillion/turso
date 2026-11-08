use super::*;

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
    // Callee errors can destroy the guard first. Keep the native publication lock
    // through Store destruction without counting the owner as pending I/O.
    authority_owner: Arc<PublicationLease>,
}

pub(in crate::mvcc) struct SourceIntentStore {
    owner: IntentRecoveryStore,
    physical: RecoveredIntentPhysicalBase,
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
        let store = runtime
            .reserve_store()?
            .construct_store(self.config.store_allocator())?;
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
            authority_owner,
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
            || !Arc::ptr_eq(
                &self.authority_owner,
                &self.lease.upgrade().ok_or(LimboError::Busy)?,
            )
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
