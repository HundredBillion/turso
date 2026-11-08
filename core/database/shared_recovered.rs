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
