use super::*;

/// Opens only retained physical main storage. The caller supplies a read-only facade
/// with read pins and verifies that physical WAL recovery is complete and WAL is empty.
/// The returned bootstrap connection is private staging state, never recovery proof.
#[allow(dead_code)]
pub(crate) struct SharedPhysicalConnection {
    database: Arc<Database>,
    init: InitState,
    pager: Option<Arc<Pager>>,
    connection: Option<Arc<Connection>>,
    reparse: crate::connection::ReparseSchemaState,
    terminal: bool,
}

#[allow(dead_code)]
impl SharedPhysicalConnection {
    pub(crate) fn new(io: Arc<dyn IO>, path: String, options: OpenOptions) -> Result<Self> {
        if path.is_empty() || is_memory_like(&path) {
            return Err(LimboError::InvalidArgument(
                "physical reader requires a persistent diagnostic path".into(),
            ));
        }
        if options.encryption.is_some()
            || options.page_codec.is_some()
            || options.db_opts.enable_encryption
            || options.durable_storage.is_some()
            || options.readonly_storage.is_some()
        {
            return Err(LimboError::InvalidArgument(
                "physical reader requires unencrypted retained read-only storage".into(),
            ));
        }
        let storage = options.storage.ok_or_else(|| {
            LimboError::InvalidArgument("physical reader requires retained storage".into())
        })?;
        // Reject an empty image before Database::new can synthesize page one.
        if storage.size()? < 512 {
            return Err(LimboError::InvalidArgument(
                "physical reader requires an existing database header".into(),
            ));
        }
        let wal_path = options.wal_path.unwrap_or_else(|| format!("{path}-wal"));
        let database = Arc::new(Database::new(
            options.db_opts,
            OpenFlags::ReadOnly | OpenFlags::NoLock,
            path,
            wal_path,
            &io,
            storage,
            None,
            options.allocators,
            None,
            options.dialect,
            &options.native_extensions,
        )?);
        Ok(Self {
            database,
            init: InitState::default(),
            pager: None,
            connection: None,
            reparse: Default::default(),
            terminal: false,
        })
    }

    pub(crate) fn poll(&mut self) -> IOResultOr<Arc<Connection>> {
        if self.terminal {
            return Err(LimboError::InvalidArgument(
                "physical reader construction has ended".into(),
            )
            .into());
        }
        let result = self.poll_inner();
        if result.is_err() {
            self.terminal = true;
            self.reparse = Default::default();
            self.connection = None;
            self.pager = None;
            self.init = InitState::default();
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<Arc<Connection>> {
        if self.pager.is_none() {
            self.pager = Some(Arc::new(return_if_io!(self.database._init_nonblock(
                &mut self.init,
                None,
                None
            ))));
        }
        if self.connection.is_none() {
            let pager = self.pager.as_ref().unwrap();
            let cache_size =
                return_if_io!(pager.with_header(|header| header.default_page_cache_size)).get();
            self.connection = Some(self.database.new_connection(
                true,
                pager.clone(),
                None,
                cache_size,
                ConnectionContext::default(),
                true,
            )?);
        }
        let connection = self.connection.as_ref().unwrap();
        return_if_io!(connection.reparse_schema_nonblock(&mut self.reparse));
        // Equal and zero cookies are valid: both private caches must own the parsed schema.
        *self.database.schema.lock() = connection.schema.read().clone();
        self.terminal = true;
        self.pager = None;
        Ok(IOResult::Done(self.connection.take().unwrap()))
    }
}

#[cfg(all(test, feature = "fs", not(shuttle)))]
mod tests {
    use super::*;
    use crate::io::FileSyncType;
    use crate::storage::database::DatabaseStorage;
    use crate::sync::atomic::AtomicUsize;
    use crate::IOContext;

    struct ReadonlyStorage {
        inner: Arc<dyn DatabaseStorage>,
        writes: AtomicUsize,
    }
    impl DatabaseStorage for ReadonlyStorage {
        fn read_header(&self, completion: Completion) -> Result<Completion> {
            self.inner.read_header(completion)
        }
        fn read_page(
            &self,
            page: usize,
            context: &IOContext,
            completion: Completion,
        ) -> Result<Completion> {
            self.inner.read_page(page, context, completion)
        }
        fn write_page(
            &self,
            _: usize,
            _: Arc<Buffer>,
            _: &IOContext,
            _: Completion,
        ) -> Result<Completion> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Err(LimboError::ReadOnly)
        }
        fn write_pages(
            &self,
            _: usize,
            _: usize,
            _: Vec<Arc<Buffer>>,
            _: &IOContext,
            _: Completion,
        ) -> Result<Completion> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Err(LimboError::ReadOnly)
        }
        fn sync(&self, _: Completion, _: FileSyncType) -> Result<Completion> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Err(LimboError::ReadOnly)
        }
        fn size(&self) -> Result<u64> {
            self.inner.size()
        }
        fn truncate(&self, _: usize, _: Completion) -> Result<Completion> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Err(LimboError::ReadOnly)
        }
    }

    #[test]
    fn shared_physical_connection_reads_retained_main_without_registry_or_writes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("physical.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let dialect: Arc<dyn Dialect> = Arc::new(crate::EngineDialect);
        let source =
            Database::open_file(io.clone(), path.to_str().unwrap(), dialect.clone()).unwrap();
        let writer = source.connect(ConnectOpts::new()).unwrap();
        writer.execute("CREATE TABLE physical(id INTEGER PRIMARY KEY, value); INSERT INTO physical VALUES(1,'from main'); PRAGMA schema_version=0").unwrap();
        writer
            .checkpoint(crate::CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();
        let source_schema = writer.schema.read().clone();
        let bytes = std::fs::read(&path).unwrap();
        let storage = Arc::new(ReadonlyStorage {
            inner: source.db_file.clone(),
            writes: AtomicUsize::new(0),
        });
        let diagnostic_path = directory.path().join("must-not-be-opened.db");
        let options = OpenOptions::new(dialect.clone()).storage(storage.clone());
        let mut open = SharedPhysicalConnection::new(
            io.clone(),
            diagnostic_path.to_str().unwrap().into(),
            options,
        )
        .unwrap();
        let reader = io.block(|| open.poll()).unwrap();
        assert!(!Arc::ptr_eq(&reader.db, &source));
        assert!(!Arc::ptr_eq(&reader.get_pager(), &writer.get_pager()));
        assert!(!reader.get_pager().uses_shared_state(&source.pager_state));
        assert!(reader.db.registry_key.get().is_none());
        assert!(reader.db.get_mv_store().is_none());
        assert!(!reader.get_pager().has_wal());
        assert!(reader.is_mvcc_bootstrap_connection());
        assert!(reader.is_readonly(crate::MAIN_DB_ID));
        assert!(Arc::ptr_eq(&reader.db.dialect, &dialect));
        assert!(Arc::ptr_eq(&reader.schema.read(), &reader.db.schema.lock()));
        assert!(Arc::ptr_eq(&writer.schema.read(), &source_schema));
        let mut values = Vec::new();
        reader
            .prepare("SELECT value FROM physical WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                values.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(values, ["from main"]);
        assert!(matches!(
            reader.execute("INSERT INTO physical VALUES(2,'forbidden')"),
            Err(LimboError::ReadOnly)
        ));
        drop(reader);
        drop(open);
        assert_eq!(storage.writes.load(Ordering::SeqCst), 0);
        assert!(!diagnostic_path.exists());
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
    #[test]
    fn shared_physical_connection_keeps_source_mvcc_state_and_reads_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("physical-mvcc.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let source = Database::open_file(
            io.clone(),
            path.to_str().unwrap(),
            Arc::new(crate::EngineDialect),
        )
        .unwrap();
        let writer = source.connect(ConnectOpts::new()).unwrap();
        writer.execute("PRAGMA journal_mode=mvcc; CREATE TABLE physical(id INTEGER PRIMARY KEY, value); INSERT INTO physical VALUES(1,'checkpointed'); CREATE SEQUENCE physical_sequence START WITH 4").unwrap();
        writer
            .checkpoint(crate::CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();
        let store = source.get_mv_store().as_ref().unwrap().clone();
        store.set_sequence_watermark("physical_sequence", 9876);
        writer.execute("CREATE TABLE log_only(value)").unwrap();
        let source_schema = source.schema.lock().clone();
        let writer_schema = writer.schema.read().clone();
        let source_root = store.get_table_id_from_root_page(1);
        let storage = Arc::new(ReadonlyStorage {
            inner: source.db_file.clone(),
            writes: AtomicUsize::new(0),
        });
        let bytes = std::fs::read(&path).unwrap();
        let options = OpenOptions::new(source.dialect.clone()).storage(storage.clone());
        let mut open =
            SharedPhysicalConnection::new(io.clone(), path.to_str().unwrap().into(), options)
                .unwrap();
        let reader = io.block(|| open.poll()).unwrap();
        assert!(reader.prepare("SELECT * FROM log_only").is_err());
        let mut timestamps = Vec::new();
        reader
            .prepare("SELECT v FROM __turso_internal_mvcc_meta WHERE k='persistent_tx_ts_max'")
            .unwrap()
            .run_with_row_callback(|row| {
                timestamps.push(row.get::<i64>(0)?);
                Ok(())
            })
            .unwrap();
        assert_eq!(timestamps.len(), 1);
        assert!(timestamps[0] > 0);
        reader.execute("SELECT * FROM physical").unwrap();
        assert!(reader
            .schema
            .read()
            .get_sequence(crate::namespace::NamespaceId::ROOT, "physical_sequence")
            .is_some());
        assert_eq!(store.sequence_watermark("physical_sequence"), Some(9876));
        assert_eq!(store.get_table_id_from_root_page(1), source_root);
        assert!(Arc::ptr_eq(&source.schema.lock(), &source_schema));
        assert!(Arc::ptr_eq(&writer.schema.read(), &writer_schema));
        assert!(reader.db.get_mv_store().is_none());
        assert!(Arc::ptr_eq(&reader.schema.read(), &reader.db.schema.lock()));
        assert_eq!(storage.writes.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn shared_physical_connection_rejects_empty_or_unretained_storage() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("empty.db");
        std::fs::write(&path, []).unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let dialect: Arc<dyn Dialect> = Arc::new(crate::EngineDialect);
        assert!(SharedPhysicalConnection::new(
            io.clone(),
            path.to_str().unwrap().into(),
            OpenOptions::new(dialect.clone())
        )
        .is_err());
        let file = io
            .open_file(
                path.to_str().unwrap(),
                OpenFlags::ReadOnly | OpenFlags::NoLock,
                false,
            )
            .unwrap();
        let storage = Arc::new(ReadonlyStorage {
            inner: Arc::new(crate::storage::database::DatabaseFile::new(file)),
            writes: AtomicUsize::new(0),
        });
        for diagnostic in [":memory:", path.to_str().unwrap()] {
            assert!(SharedPhysicalConnection::new(
                io.clone(),
                diagnostic.into(),
                OpenOptions::new(dialect.clone()).storage(storage.clone())
            )
            .is_err());
        }
        assert_eq!(storage.writes.load(Ordering::SeqCst), 0);
        assert!(std::fs::read(path).unwrap().is_empty());
    }
}
