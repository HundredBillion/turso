use super::*;
use crate::mvcc::checkpoint_gate::CheckpointPause;
use crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::finalize::FinalizedIds;

pub(in crate::mvcc) struct PublishedSharedCheckpoint {
    pub finalized: FinalizedIds,
    pub pause: CheckpointPause,
}

pub(in crate::mvcc) struct SharedCheckpointPublication<C: LogicalClock, A: ConcurrentAllocator> {
    _store: Arc<MvStore<C, A>>,
    _connection: Arc<Connection>,
}
impl<C: LogicalClock, A: ConcurrentAllocator> SharedCheckpointPublication<C, A> {
    pub(in crate::mvcc) fn new(
        store: Arc<MvStore<C, A>>,
        connection: Arc<Connection>,
    ) -> Result<Self> {
        Ok(Self {
            _store: store,
            _connection: connection,
        })
    }
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<PublishedSharedCheckpoint> {
        Err(
            LimboError::InvalidArgument("shared checkpoint coordinator is not implemented".into())
                .into(),
        )
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
    connection
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
