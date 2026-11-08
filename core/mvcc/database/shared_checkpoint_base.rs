use super::*;
use crate::database::shared_physical::SharedPhysicalConnection;
use crate::mvcc::database::shared_bootstrap::{PhysicalBaseParts, PhysicalBaseReadState};
use crate::mvcc::read_pinned_file::ReadPinnedFile;
use crate::storage::database::DatabaseFile;

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
    runtime: Arc<SharedMvccRuntime>,
    opening: Option<SharedPhysicalConnection>,
    reader: Option<Arc<Connection>>,
    state: PhysicalBaseReadState,
    parts: Option<PhysicalBaseParts>,
    published: Option<PublishedSharedCheckpoint>,
    finished: bool,
}

impl PublishedSharedCheckpoint {
    pub(in crate::mvcc) fn begin_physical_read<C: LogicalClock, A: ConcurrentAllocator>(
        self,
        store: Arc<MvStore<C, A>>,
        connection: Arc<Connection>,
    ) -> Result<SharedCheckpointPhysicalRead<C, A>> {
        let runtime = store.shared_runtime.get().cloned().ok_or_else(|| {
            LimboError::InvalidArgument("physical staging requires shared MVCC".into())
        })?;
        validate_owner(&store, &connection, &runtime, &self)?;
        let pager = connection.get_pager();
        if pager.is_encryption_ctx_set() || pager.has_external_page_codec() {
            return Err(LimboError::InvalidArgument(
                "checkpoint physical staging requires unencoded pages".into(),
            ));
        }
        let file = ReadPinnedFile::new(runtime.files.main.clone(), self.finalized.guard())?;
        let options = crate::OpenOptions::new(connection.db.dialect().clone())
            .db_opts(connection.db.opts)
            .allocators(connection.db.allocators.clone())
            .storage(Arc::new(DatabaseFile::new(file)));
        let opening = SharedPhysicalConnection::new(
            connection.db.io.clone(),
            connection.db.path.clone(),
            options,
        )?;
        Ok(SharedCheckpointPhysicalRead {
            store,
            connection,
            runtime,
            opening: Some(opening),
            reader: None,
            state: Default::default(),
            parts: None,
            published: Some(self),
            finished: false,
        })
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> SharedCheckpointPhysicalRead<C, A> {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<VerifiedPhysicalBase> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<VerifiedPhysicalBase> {
        if self.finished {
            return Err(
                LimboError::InvalidArgument("checkpoint physical staging ended".into()).into(),
            );
        }
        let published = self.published.as_ref().unwrap();
        validate_owner(&self.store, &self.connection, &self.runtime, published)?;
        if self.reader.is_none() && self.parts.is_none() {
            self.reader = Some(return_if_io!(self.opening.as_mut().unwrap().poll()));
            self.opening = None;
        }
        if self.parts.is_none() {
            self.parts = Some(return_if_io!(self
                .state
                .poll(self.reader.as_ref().unwrap())));
            self.reader = None;
        }
        validate_parts(self.parts.as_ref().unwrap(), &self.runtime, published)?;
        let guard = published.finalized.guard();
        if !guard.io_retired() {
            return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
        }
        self.finished = true;
        Ok(IOResult::Done(VerifiedPhysicalBase {
            parts: self.parts.take().unwrap(),
            published: self.published.take().unwrap(),
        }))
    }

    fn abort(&mut self) {
        self.state = Default::default();
        self.reader = None;
        self.opening = None;
        self.parts = None;
        self.published = None;
        self.finished = true;
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> Drop for SharedCheckpointPhysicalRead<C, A> {
    fn drop(&mut self) {
        self.abort();
    }
}

fn validate_owner<C: LogicalClock, A: ConcurrentAllocator>(
    store: &Arc<MvStore<C, A>>,
    connection: &Connection,
    runtime: &SharedMvccRuntime,
    published: &PublishedSharedCheckpoint,
) -> Result<()> {
    let guard = published.finalized.guard();
    let proof = published.finalized.proof();
    let target = proof.target_identity();
    if connection
        .db
        .get_mv_store()
        .as_ref()
        .is_none_or(|actual| Arc::as_ptr(actual).cast::<()>() != Arc::as_ptr(store).cast::<()>())
        || !runtime.owns_authority(guard)
        || !proof.matches_lease(guard)
        || guard.published()? != target.prefix
        || target.prefix.end_offset != 0
        || target.prefix.running_crc != 0
        || target.prefix.committed_timestamp != target.database.checkpoint_timestamp
        || runtime.files.main.file_id()? != Some(runtime.database_id)
    {
        return Err(LimboError::InvalidArgument(
            "physical staging requires the exact completed checkpoint owner".into(),
        ));
    }
    if !store.txs.is_empty() || !guard.live_registrations()?.is_empty() {
        return Err(LimboError::Busy);
    }
    if runtime.files.physical_wal.size()? != 0 {
        return Err(LimboError::Corrupt(
            "checkpoint physical WAL is not empty".into(),
        ));
    }
    Ok(())
}

fn validate_parts(
    parts: &PhysicalBaseParts,
    runtime: &SharedMvccRuntime,
    published: &PublishedSharedCheckpoint,
) -> Result<()> {
    let header = &parts.header;
    let bytes = runtime.files.main.size()?;
    let page_size = u64::from(header.page_size.get());
    let pages = i64::from(header.database_size.get());
    if header.magic != *b"SQLite format 3\0"
        || header.read_version.0 != SHARED_MVCC_VERSION
        || header.write_version.0 != SHARED_MVCC_VERSION
        || crate::PageSize::new(header.page_size.get()).is_none()
        || !header
            .page_size
            .has_valid_reserved_space(header.reserved_space)
        || pages <= 0
        || bytes % page_size != 0
        || bytes / page_size < pages as u64
        || parts.persistent_timestamp
            != published
                .finalized
                .proof()
                .target_identity()
                .database
                .checkpoint_timestamp
    {
        return Err(LimboError::Corrupt(
            "invalid completed physical checkpoint facts".into(),
        ));
    }
    let physical_floor = i64::try_from(bytes / page_size)
        .ok()
        .and_then(i64::checked_neg)
        .filter(|floor| *floor != i64::MIN)
        .ok_or(LimboError::DatabaseFull)?;
    if published.finalized.proof().materialized_floor() > physical_floor {
        return Err(LimboError::Corrupt(
            "completed ID floor misses physical pages".into(),
        ));
    }
    let mut roots = HashSet::default();
    for root in parts
        .schema
        .all_tables()
        .filter_map(|table| match table.as_ref() {
            crate::schema::Table::BTree(table) => Some(table.root_page),
            _ => None,
        })
        .chain(
            parts
                .schema
                .all_indexes()
                .filter(|index| index.is_btree_backed())
                .map(|index| index.root_page),
        )
    {
        roots.try_reserve(1)?;
        if root <= 0 || root > pages || !roots.insert(root) {
            return Err(LimboError::Corrupt(
                "invalid or duplicate physical checkpoint root".into(),
            ));
        }
    }
    if !roots.contains(&1) {
        return Err(LimboError::Corrupt(
            "physical checkpoint lacks schema root".into(),
        ));
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_base_retirement_tests.rs"]
mod retirement_tests;

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
