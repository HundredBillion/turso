use super::*;

impl<A: alloc::ConcurrentAllocator, F: alloc::ConcurrentAllocator> Database<A, F> {
    /// The runtime supplies read-pinned files bound to its continuously held authority.
    /// This pager owns separate cache/read state and never changes the requesting pager.
    #[allow(dead_code)]
    pub(crate) fn shared_oracle_pager(
        &self,
        source: &Pager,
        main: Arc<dyn io::File>,
        physical_wal: Arc<dyn io::File>,
    ) -> Result<Arc<Pager>> {
        if !source.uses_shared_state(&self.pager_state) {
            return Err(LimboError::InvalidArgument(
                "oracle source pager belongs to another database".into(),
            ));
        }
        let runtime = self.shared_mvcc_runtime.as_ref().ok_or_else(|| {
            LimboError::InvalidArgument("oracle pager requires shared MVCC runtime".into())
        })?;
        let main_id = runtime.main_file().file_id()?.ok_or_else(|| {
            LimboError::InvalidArgument("oracle main file has no retained identity".into())
        })?;
        let wal_id = runtime.physical_wal_file().file_id()?.ok_or_else(|| {
            LimboError::InvalidArgument("oracle WAL file has no retained identity".into())
        })?;
        if main.file_id()? != Some(main_id) || physical_wal.file_id()? != Some(wal_id) {
            return Err(LimboError::InvalidArgument(
                "oracle file identity mismatch".into(),
            ));
        }
        if source.is_encryption_ctx_set() || source.has_external_page_codec() {
            return Err(LimboError::InvalidArgument(
                "shared oracle pager requires unencoded pages".into(),
            ));
        }
        let page_size = source.get_page_size().ok_or_else(|| {
            LimboError::InvalidArgument("oracle source pager is not initialized".into())
        })?;
        let reserved = source.get_reserved_space().ok_or_else(|| {
            LimboError::InvalidArgument("oracle source pager layout is not initialized".into())
        })?;
        let wal: Option<Arc<dyn Wal>> = if source.has_wal() {
            let snapshot = self.shared_wal.read().last_checksum_and_max_frame();
            Some(Arc::new(
                WalFile::new_with_shared_coordination(
                    self.io.clone(),
                    self.shared_wal.clone(),
                    runtime.physical_coordination(),
                    snapshot,
                    self.buffer_pool.clone(),
                )
                .with_read_file(physical_wal)?,
            ))
        } else {
            None
        };
        let pager = Pager::new(
            Arc::new(DatabaseFile::new(main)),
            wal,
            self.io.clone(),
            PageCache::default(),
            self.buffer_pool.clone(),
            self.pager_state.clone(),
        )?;
        pager.set_page_size(page_size);
        pager.set_reserved_space_bytes(reserved);
        if reserved != CHECKSUM_REQUIRED_RESERVED_BYTES {
            pager.reset_checksum_context();
        }
        Ok(Arc::new(pager))
    }
}

#[cfg(all(test, target_os = "linux", feature = "fs", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_oracle_pager_uses_retained_identity_and_independent_read_state() {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = Database::open(
            io.clone(),
            path.to_str().unwrap(),
            OpenOptions::new(Arc::new(crate::dialect::engine::EngineDialect))
                .flags(OpenFlags::None)
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        let connection = db.connect(crate::ConnectOpts::new()).unwrap();
        let source = connection.get_pager();
        let runtime = db.shared_mvcc_runtime.as_ref().unwrap();
        let foreign = Pager::new(
            db.db_file.clone(),
            None,
            io.clone(),
            PageCache::default(),
            db.buffer_pool.clone(),
            Arc::new(SharedPagerState::new(None)),
        )
        .unwrap();
        foreign.set_page_size(source.get_page_size().unwrap());
        foreign.set_reserved_space_bytes(source.get_reserved_space().unwrap());
        let error =
            db.shared_oracle_pager(&foreign, runtime.main_file(), runtime.physical_wal_file());
        assert!(
            matches!(error, Err(LimboError::InvalidArgument(message)) if message.contains("source pager"))
        );
        let before = source.holds_read_lock();
        assert!(db
            .shared_oracle_pager(
                &source,
                runtime.physical_wal_file(),
                runtime.physical_wal_file()
            )
            .is_err());
        assert!(db
            .shared_oracle_pager(&source, runtime.main_file(), runtime.main_file())
            .is_err());
        let oracle = db
            .shared_oracle_pager(&source, runtime.main_file(), runtime.physical_wal_file())
            .unwrap();
        assert!(!Arc::ptr_eq(&source, &oracle));
        assert_eq!(oracle.get_page_size(), source.get_page_size());
        assert_eq!(oracle.get_reserved_space(), source.get_reserved_space());
        oracle.begin_read_tx().unwrap();
        let (page, completion) = loop {
            match oracle.read_page(1).unwrap() {
                IOResult::Done(result) => break result,
                IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
            }
        };
        if let Some(completion) = completion {
            io.wait_for_completion(completion).unwrap();
        }
        assert!(page.is_loaded());
        assert!(!page.is_locked());
        oracle.end_read_tx();
        assert_eq!(source.holds_read_lock(), before);
    }
}
