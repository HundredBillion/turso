use super::*;
use crate::mvcc::database::shared_bootstrap::PhysicalBaseParts;

pub(in crate::mvcc) struct VerifiedPhysicalBase {
    parts: PhysicalBaseParts,
    published: PublishedSharedCheckpoint,
}

impl VerifiedPhysicalBase {
    pub(in crate::mvcc) fn parts(&self) -> &PhysicalBaseParts {
        &self.parts
    }

    pub(in crate::mvcc) fn into_parts(self) -> (PhysicalBaseParts, PublishedSharedCheckpoint) {
        (self.parts, self.published)
    }
}

pub(in crate::mvcc) struct SharedCheckpointPhysicalRead<C: LogicalClock, A: ConcurrentAllocator> {
    store: Arc<MvStore<C, A>>,
    connection: Arc<Connection>,
    published: Option<PublishedSharedCheckpoint>,
}

impl PublishedSharedCheckpoint {
    pub(in crate::mvcc) fn begin_physical_read<C: LogicalClock, A: ConcurrentAllocator>(
        self,
        store: Arc<MvStore<C, A>>,
        connection: Arc<Connection>,
    ) -> Result<SharedCheckpointPhysicalRead<C, A>> {
        Ok(SharedCheckpointPhysicalRead {
            store,
            connection,
            published: Some(self),
        })
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> SharedCheckpointPhysicalRead<C, A> {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<VerifiedPhysicalBase> {
        Err(LimboError::InvalidArgument(
            "completed checkpoint physical staging is not implemented".into(),
        )
        .into())
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_checkpoint_physical_native_stages_target_without_mutating_source() {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let options = crate::OpenOptions::new(Arc::new(crate::dialect::EngineDialect))
            .with_shared_mvcc_for_test(limit);
        let database =
            crate::Database::open(io.clone(), path.to_str().unwrap(), options.clone()).unwrap();
        let connection = database.connect(crate::ConnectOpts::new()).unwrap();
        connection
            .execute("UPDATE preserved SET value='staged completed payload' WHERE id=1")
            .unwrap();
        let store = database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap().clone();
        let mut publication =
            SharedCheckpointPublication::new(store.clone(), connection.clone()).unwrap();
        let published = io.block(|| publication.poll()).unwrap();
        let target = published.finalized.proof().target_identity();
        let schema = database.schema.lock().clone();
        let connection_schema = connection.schema.read().clone();
        let root = store.get_table_id_from_root_page(1);
        store.set_sequence_watermark("checkpoint_physical_sentinel", 9876);
        let main_before = std::fs::read(&path).unwrap();
        let log_before = std::fs::read(runtime.files.namespace.join("logical")).unwrap();
        let manifest_before = std::fs::read(runtime.files.namespace.join("manifest")).unwrap();
        let mut reading = published
            .begin_physical_read(store.clone(), connection.clone())
            .unwrap();
        let verified = io.block(|| reading.poll()).unwrap();
        assert_eq!(
            verified.parts().persistent_timestamp,
            target.database.checkpoint_timestamp
        );
        assert_eq!(verified.parts().header.read_version.0, SHARED_MVCC_VERSION);
        assert_eq!(verified.parts().header.write_version.0, SHARED_MVCC_VERSION);
        assert!(verified
            .parts()
            .schema
            .get_table(NamespaceId::ROOT, "preserved")
            .is_some());
        assert!(Arc::ptr_eq(&schema, &database.schema.lock()));
        assert!(Arc::ptr_eq(&connection_schema, &connection.schema.read()));
        assert_eq!(store.get_table_id_from_root_page(1), root);
        assert_eq!(
            store.sequence_watermark("checkpoint_physical_sentinel"),
            Some(9876)
        );
        assert_eq!(std::fs::read(&path).unwrap(), main_before);
        assert_eq!(
            std::fs::read(runtime.files.namespace.join("logical")).unwrap(),
            log_before
        );
        assert_eq!(
            std::fs::read(runtime.files.namespace.join("manifest")).unwrap(),
            manifest_before
        );
        assert!(matches!(
            runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        let (_parts, published) = verified.into_parts();
        assert!(published.finalized.guard().io_retired());
        assert_eq!(published.finalized.proof().target_identity(), target);
        assert!(reading.poll().is_err());
        drop(reading);
        drop(published);
        drop(publication);
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
        assert_eq!(values, ["staged completed payload"]);
    }
}
