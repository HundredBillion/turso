use super::*;

#[allow(dead_code)]
pub(crate) struct RecoveryOpenConfig {
    options: OpenOptions,
}

#[allow(dead_code)]
impl RecoveryOpenConfig {
    pub(crate) fn capture(options: OpenOptions) -> Result<Arc<Self>> {
        if options.storage.is_some()
            || options.durable_storage.is_some()
            || options.wal_path.is_some()
            || options.encryption.is_some()
            || options.page_codec.is_some()
            || options.db_opts.enable_encryption
            || options.db_opts.enable_multiprocess_wal
            || options.db_opts.enable_experimental_mvcc_passive_checkpoint
            || options.shared_mvcc_history_limit == Some(0)
        {
            return Err(LimboError::InvalidArgument(
                "recovery bootstrap requires unencoded retained shared storage and compatible open options".into(),
            ));
        }
        if options.dialect.supports_roles() {
            return Err(LimboError::InvalidArgument(
                "recovery bootstrap does not support role-aware dialects".into(),
            ));
        }
        Ok(Arc::new(Self { options }))
    }

    pub(crate) fn open_physical(
        &self,
        io: Arc<dyn IO>,
        path: String,
        file: Arc<dyn crate::File>,
    ) -> Result<super::shared_physical::SharedPhysicalConnection> {
        super::shared_physical::SharedPhysicalConnection::new_with_retained_file(
            io,
            path,
            self.options.clone(),
            file,
        )
    }

    pub(crate) fn validate_history(&self, actual_limit: u64) -> Result<()> {
        if actual_limit == 0
            || self
                .options
                .shared_mvcc_history_limit
                .is_some_and(|requested| requested != actual_limit)
        {
            return Err(LimboError::InvalidArgument(
                "recovery open options differ from the validated shared history budget".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn store_allocator(&self) -> crate::alloc::DynAllocator {
        self.options.allocators.mv_store.clone()
    }

    pub(crate) fn effective_readonly(&self, original_readonly: bool) -> bool {
        original_readonly || self.options.flags.contains(OpenFlags::ReadOnly)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::{AllocError, ApiAllocator, DynAllocator, Global, Layout};
    use crate::function::ExternalFunc;
    use crate::native_ext::{FunctionArity, ScalarCall, ScalarFunction};
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use std::ptr::NonNull;

    #[derive(Clone)]
    struct CountingAllocator(Arc<AtomicUsize>);
    unsafe impl ApiAllocator for CountingAllocator {
        fn allocate(&self, layout: Layout) -> std::result::Result<NonNull<[u8]>, AllocError> {
            self.0.fetch_add(1, Ordering::AcqRel);
            Global.allocate(layout)
        }
        unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
            unsafe { Global.deallocate(pointer, layout) }
        }
    }
    #[derive(Debug)]
    struct Answer;
    impl ScalarFunction for Answer {
        type Call = Self;
        fn create_call(&self) -> Result<Self> {
            Ok(Self)
        }
    }
    impl ScalarCall for Answer {
        type Output = i64;
        fn step(&mut self, _: &Arc<Connection>, _: &[crate::Register]) -> IOResultOr<i64> {
            Ok(IOResult::Done(9))
        }
    }

    #[test]
    fn recovery_open_config_preserves_actual_options_and_allocators() {
        let dialect: Arc<dyn Dialect> = Arc::new(crate::EngineDialect);
        let mv_calls = Arc::new(AtomicUsize::new(0));
        let fts_calls = Arc::new(AtomicUsize::new(0));
        let opts = DatabaseOpts::new().with_views(true).with_custom_types(true);
        let original = OpenOptions::new(dialect.clone())
            .db_opts(opts)
            .allocators(DatabaseAllocators {
                mv_store: DynAllocator::new(CountingAllocator(mv_calls.clone())),
                fts: DynAllocator::new(CountingAllocator(fts_calls.clone())),
            })
            .extension_function(
                ExternalFunc::new_native_scalar(
                    "recovered_answer".into(),
                    FunctionArity::Exact(0),
                    true,
                    Answer,
                )
                .unwrap(),
            );
        let function = original.native_extensions.functions[0].clone();
        let config = RecoveryOpenConfig::capture(original).unwrap();
        assert!(Arc::ptr_eq(&config.options.dialect, &dialect));
        assert_eq!(config.options.db_opts, opts);
        assert!(Arc::ptr_eq(
            &config.options.native_extensions.functions[0],
            &function
        ));
        let layout = Layout::from_size_align(64, 16).unwrap();
        let mv_allocator = config.store_allocator();
        let block = mv_allocator.allocate(layout).unwrap();
        unsafe { mv_allocator.deallocate(block.cast(), layout) };
        assert_eq!(mv_calls.load(Ordering::Acquire), 1);
        assert_eq!(fts_calls.load(Ordering::Acquire), 0);
        let block = config.options.allocators.fts.allocate(layout).unwrap();
        unsafe {
            config
                .options
                .allocators
                .fts
                .deallocate(block.cast(), layout)
        };
        assert_eq!(fts_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn recovery_open_config_retains_and_validates_original_optin() {
        let config = RecoveryOpenConfig::capture(
            OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(64 * 1024 * 1024),
        )
        .unwrap();
        assert_eq!(
            config.options.shared_mvcc_history_limit,
            Some(64 * 1024 * 1024)
        );
        config.validate_history(64 * 1024 * 1024).unwrap();
        assert!(config.validate_history(1).is_err());
        assert!(config.validate_history(0).is_err());
    }

    #[test]
    fn recovery_open_config_preserves_readonly_restrictions() {
        let writable =
            RecoveryOpenConfig::capture(OpenOptions::new(Arc::new(crate::EngineDialect))).unwrap();
        assert!(!writable.effective_readonly(false));
        assert!(writable.effective_readonly(true));
        let readonly = RecoveryOpenConfig::capture(
            OpenOptions::new(Arc::new(crate::EngineDialect)).flags(OpenFlags::ReadOnly),
        )
        .unwrap();
        assert!(readonly.effective_readonly(false));
        assert!(readonly.effective_readonly(true));
    }

    struct RoleDialect;
    impl Dialect for RoleDialect {
        fn name(&self) -> &'static str {
            "roles-test"
        }
        fn parse(&self, sql: &str) -> Result<(Option<turso_parser::ast::Cmd>, usize)> {
            crate::EngineDialect.parse(sql)
        }
        fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<crate::schema::BTreeTable> {
            crate::EngineDialect.parse_table_sql(sql, root_page)
        }
        fn parse_table_sql_ast(&self, sql: &str) -> Result<turso_parser::ast::Stmt> {
            crate::EngineDialect.parse_table_sql_ast(sql)
        }
        fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
            crate::EngineDialect.table_sql_for_replay(sql)
        }
        fn format_table_sql(
            &self,
            input: &str,
            name: &turso_parser::ast::QualifiedName,
            body: &turso_parser::ast::CreateTableBody,
        ) -> Result<String> {
            crate::EngineDialect.format_table_sql(input, name, body)
        }
        fn register_catalog(
            &self,
            schema: &mut crate::schema::Schema,
            enabled: bool,
        ) -> Result<()> {
            crate::EngineDialect.register_catalog(schema, enabled)
        }
        fn resolve_function(&self, name: &str, args: usize) -> Result<Option<crate::Func>> {
            crate::EngineDialect.resolve_function(name, args)
        }
        fn supports_roles(&self) -> bool {
            true
        }
    }

    #[test]
    fn recovery_open_config_rejects_role_catalog_before_io() {
        let error = RecoveryOpenConfig::capture(OpenOptions::new(Arc::new(RoleDialect)))
            .err()
            .unwrap();
        assert!(error
            .to_string()
            .contains("does not support role-aware dialects"));
    }

    #[test]
    fn recovery_open_config_rejects_storage_and_mode_overrides() {
        for opts in [
            DatabaseOpts::new().with_encryption(true),
            DatabaseOpts::new().with_multiprocess_wal(true),
            DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
        ] {
            assert!(RecoveryOpenConfig::capture(
                OpenOptions::new(Arc::new(crate::EngineDialect)).db_opts(opts)
            )
            .is_err());
        }
        let options = OpenOptions::new(Arc::new(crate::EngineDialect)).wal_path("other-wal");
        assert!(RecoveryOpenConfig::capture(options).is_err());
        let io = crate::MemoryIO::new();
        let file = io
            .open_file("recovery-config", OpenFlags::Create, false)
            .unwrap();
        let options = OpenOptions::new(Arc::new(crate::EngineDialect))
            .storage(Arc::new(DatabaseFile::new(file)));
        assert!(RecoveryOpenConfig::capture(options).is_err());
    }
}

pub(crate) struct RecoveredDatabaseFactory {
    connection: Option<Arc<Connection>>,
    pager: Option<Arc<Pager>>,
    database: Option<Arc<Database>>,
    building: Option<Database>,
    validation: HeaderValidationState,
    permit: Arc<crate::mvcc::database::RecoveryConstructionPermit>,
    config: Arc<RecoveryOpenConfig>,
}
impl RecoveredDatabaseFactory {
    pub(crate) fn new(source: &crate::mvcc::database::RecoveryBootstrapEvidence) -> Result<Self> {
        source.validate()?;
        let config = source.config().clone();
        let io = source.io();
        let runtime = source.runtime();
        let flags = if config.effective_readonly(source.original_readonly()) {
            OpenFlags::NoLock | OpenFlags::ReadOnly
        } else {
            OpenFlags::NoLock
        };
        let mut database = Database::new(
            config.options.db_opts,
            flags,
            source.path()?,
            runtime.wal_path().to_str().ok_or_else(|| {
                LimboError::InvalidArgument("shared WAL path must be UTF-8".into())
            })?,
            &io,
            Arc::new(DatabaseFile::new(runtime.main_file())),
            None,
            config.options.allocators.clone(),
            None,
            config.options.dialect.clone(),
            &config.options.native_extensions,
        )?;
        {
            let shared =
                Arc::get_mut(&mut database.shared).expect("new recovery Database state is unique");
            shared.shared_mvcc_runtime = Some(runtime.clone());
            shared.durable_storage = Some(runtime.storage());
            shared.mv_store.store(Some(source.store().clone()));
        }
        let permit = source.make_permit(&database)?;
        Arc::get_mut(&mut database.shared)
            .expect("new recovery Database state is unique")
            .recovery_construction = Some(permit.clone());
        Ok(Self {
            connection: None,
            pager: None,
            database: None,
            building: Some(database),
            validation: Default::default(),
            permit,
            config,
        })
    }
    pub(crate) fn poll(
        &mut self,
        source: &mut crate::mvcc::database::RecoveryBootstrapEvidence,
    ) -> IOResultOr<()> {
        self.permit.validate_source(source)?;
        if !Arc::ptr_eq(source.config(), &self.config) {
            return Err(LimboError::InvalidArgument(
                "recovery constructor resumed with another configuration".into(),
            )
            .into());
        }
        if self.database.is_none() {
            let pager = return_if_io!(self.building.as_mut().unwrap().header_validation(
                &mut self.validation,
                None,
                None
            ));
            self.pager = Some(pager);
            self.database = Some(Arc::new(self.building.take().unwrap()));
        }
        let database = self.database.as_ref().unwrap();
        self.permit.validate_database(database)?;
        let pager = self.pager.as_ref().unwrap();
        let header = return_if_io!(pager.with_header(|header| *header));
        source.validate_header(&header)?;
        if let Some(waiter) = source.physical_retirement() {
            return Ok(IOResult::IO(crate::types::IOCompletions(waiter)));
        }
        if self.connection.is_none() {
            self.connection = Some(database.new_connection(
                false,
                pager.clone(),
                None,
                header.default_page_cache_size.get(),
                ConnectionContext::default(),
                database.is_readonly(),
            )?);
        }
        Ok(IOResult::Done(()))
    }
    fn abort(&mut self) {
        if let HeaderValidationState::Start { init } = &mut self.validation {
            init.cancel();
        }
        self.connection = None;
        self.validation = Default::default();
        self.pager = None;
        self.database = None;
        self.building = None;
    }
}
impl Drop for RecoveredDatabaseFactory {
    fn drop(&mut self) {
        self.abort();
    }
}
