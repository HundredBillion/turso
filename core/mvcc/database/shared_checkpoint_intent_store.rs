use super::*;
use crate::database::shared_recovered::RecoveryOpenConfig;

pub(in crate::mvcc) struct IntentRecoveryStore {
    store: Arc<crate::MvStore>,
    runtime: Arc<SharedMvccRuntime>,
    config: Arc<RecoveryOpenConfig>,
    lease: Weak<PublicationLease>,
    record: ManifestRecord,
}

pub(in crate::mvcc) struct SourceIntentStore {
    owner: IntentRecoveryStore,
    physical: RecoveredIntentPhysicalBase,
}

impl RecoveredIntentPhysicalBase {
    pub(in crate::mvcc) fn begin_source_store(self) -> Result<SourceIntentStore> {
        Err(LimboError::InvalidArgument(
            "interrupted source Store ownership is not implemented".into(),
        ))
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;
    use crate::mvcc::database::shared_runtime::shared_checkpoint_intent_fixture::{
        CrashPoint, IntentCrashFixture,
    };

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn intent_source_store_native_genuine_source_owns_exactly_one_unadmitted_store() {
        let fixture = IntentCrashFixture::new(CrashPoint::BeforeWalCommit, 1);
        let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
        let selected = fixture.io.block(|| opening.poll()).unwrap();
        let record = selected.manifest.record();
        let mut mirror = selected.begin_intent_mirror().unwrap();
        let intent = fixture.io.block(|| mirror.poll()).unwrap();
        let mut recovering = intent
            .begin_physical_wal_recovery(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
            .unwrap();
        let physical = fixture.io.block(|| recovering.poll()).unwrap();
        assert_eq!(
            physical.parts.persistent_timestamp,
            record
                .checkpoint_intent()
                .unwrap()
                .source_checkpoint_timestamp
        );
        let source = physical.begin_source_store().unwrap();
        assert!(Arc::ptr_eq(&source.owner.runtime, &fixture.runtime));
        assert!(Arc::ptr_eq(
            source.owner.store.shared_runtime.get().unwrap(),
            &fixture.runtime
        ));
        assert_eq!(source.owner.record, record);
        assert!(Weak::ptr_eq(
            &source.owner.lease,
            &source.physical.guard().io_lease()
        ));
        assert!(source.owner.store.txs.is_empty());
        assert!(fixture.runtime.replay.lock().is_none());
        assert!(fixture.runtime.reserve_store().is_err());
        assert!(source.physical.guard().requires_recovery());
        assert!(source.physical.guard().published().is_err());
        assert!(matches!(
            fixture.runtime.begin_open(),
            Err(LimboError::Busy)
        ));
    }
}
