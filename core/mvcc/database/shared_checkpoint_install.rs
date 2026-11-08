use super::*;
use crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof;
use crate::mvcc::shared_authority::AuthorityGuard;

pub(in crate::mvcc) struct InstalledLocalCheckpoint {
    guard: AuthorityGuard,
    proof: CompletedCheckpointProof,
    pause: CheckpointPause,
    replay: Arc<SharedReplayContext>,
    schema: Arc<Schema>,
}

pub(in crate::mvcc) struct SharedCheckpointLocalInstall<C: LogicalClock, A: ConcurrentAllocator> {
    base: Option<VerifiedPhysicalBase>,
    store: Arc<MvStore<C, A>>,
    connection: Arc<Connection>,
}

impl VerifiedPhysicalBase {
    pub(in crate::mvcc) fn begin_local_install<C: LogicalClock, A: ConcurrentAllocator>(
        self,
        store: Arc<MvStore<C, A>>,
        connection: Arc<Connection>,
    ) -> Result<SharedCheckpointLocalInstall<C, A>> {
        Ok(SharedCheckpointLocalInstall {
            base: Some(self),
            store,
            connection,
        })
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> SharedCheckpointLocalInstall<C, A> {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<InstalledLocalCheckpoint> {
        Err(LimboError::InvalidArgument(
            "local completed checkpoint installation is not implemented".into(),
        )
        .into())
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

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
        assert_eq!(storage.offset, 0);
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
        assert!(installing.poll().is_err());
        drop(installing);
        drop(installed);
        assert!(runtime.replay.lock().is_none());
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
