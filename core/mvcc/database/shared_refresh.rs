use super::*;
use crate::mvcc::commit_manifest::{LoadedManifest, ManifestRecord};
use crate::mvcc::persistent_storage::Storage;

/// Replay state survives a cancelled admission; the next admission restarts its bounded reader.
pub(super) struct SharedReplayContext {
    storage: Arc<Storage>,
    state: Mutex<Option<ReplayState>>,
}

struct ReplayState {
    context: Box<RecoverCtx>,
    observed: ManifestRecord,
}

pub(super) struct SharedRefresh {
    owner: Arc<SharedReplayContext>,
    state: Option<ReplayState>,
    loaded: Option<LoadedManifest>,
    file: Arc<dyn File>,
    header_read: bool,
    last_frame_timestamp: u64,
    pending: Option<PreparedRecoveryFrame>,
    finished: bool,
    locally_committed: Option<u64>,
}

impl SharedReplayContext {
    pub(super) fn new(
        context: Box<RecoverCtx>,
        observed: ManifestRecord,
        storage: Arc<Storage>,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage,
            state: Mutex::new(Some(ReplayState { context, observed })),
        })
    }

    pub(super) fn identity(
        &self,
    ) -> Result<crate::mvcc::shared_ids::io_protocol::RecoveryIdentity> {
        let state = self.state.lock();
        let state = state.as_ref().ok_or(LimboError::Busy)?;
        Ok(crate::mvcc::shared_ids::io_protocol::RecoveryIdentity {
            database: crate::mvcc::shared_ids::DatabaseGeneration {
                generation: state.observed.log_generation(),
                checkpoint_timestamp: state.context.persistent_tx_ts_max,
            },
            prefix: state.observed.prefix(),
        })
    }

    pub(super) fn matches_oracle_identity(
        &self,
        identity: crate::mvcc::shared_ids::io_protocol::RecoveryIdentity,
    ) -> bool {
        let state = self.state.lock();
        let Some(state) = state.as_ref() else {
            return false;
        };
        !state.observed.requires_recovery()
            && state.observed.prefix() == identity.prefix
            && state.observed.log_generation() == identity.database.generation
            && state.context.persistent_tx_ts_max == identity.database.checkpoint_timestamp
            && state.context.max_commit_ts_seen == identity.prefix.committed_timestamp
    }

    pub(super) fn refresh(
        self: &Arc<Self>,
        loaded: LoadedManifest,
        file: Arc<dyn File>,
    ) -> Result<SharedRefresh> {
        if loaded.record().requires_recovery() {
            return Err(LimboError::InvalidArgument(
                "shared manifest recovery is required".into(),
            ));
        }
        let state = self.state.lock().take().ok_or(LimboError::Busy)?;
        let mut refresh = SharedRefresh {
            owner: self.clone(),
            last_frame_timestamp: 0,
            state: Some(state),
            loaded: Some(loaded),
            file,
            header_read: false,
            pending: None,
            finished: false,
            locally_committed: None,
        };
        refresh.reset_reader()?;
        Ok(refresh)
    }
}

pub(super) struct SharedBeginOptions {
    pub pager: Arc<Pager>,
    pub expected_schema_generation: Option<u64>,
    pub portable_logical_changes: bool,
    pub database_id: usize,
    pub exclusive: bool,
}

impl SharedRefresh {
    pub(super) fn poll_begin<C: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        store: &MvStore<C, A>,
        connection: &Arc<Connection>,
        options: SharedBeginOptions,
    ) -> IOResultOr<TxID> {
        let loaded = return_if_io!(self.poll(store, connection));
        Self::begin_loaded(loaded, store, connection, options)
    }

    pub(super) fn begin_loaded<C: LogicalClock, A: ConcurrentAllocator>(
        mut loaded: LoadedManifest,
        store: &MvStore<C, A>,
        connection: &Arc<Connection>,
        options: SharedBeginOptions,
    ) -> IOResultOr<TxID> {
        #[cfg(all(host_shared_wal, unix))]
        if let Some(runtime) = store.shared_runtime.get() {
            runtime.certify_database_pager(
                &connection.get_source_database(options.database_id),
                &options.pager,
                None,
                loaded.guard()?,
            )?;
        }
        let snapshot = loaded.guard_mut()?.register_snapshot()?;
        let checkpoint_read_guard =
            store.try_acquire_checkpoint_read_guard_for_fresh_mvcc_begin()?;
        let opened_read = !options.pager.holds_read_lock();
        if opened_read {
            if let Err(error) = options.pager.begin_read_tx() {
                store.release_unadopted_checkpoint_read_guard(checkpoint_read_guard);
                return Err(error.into());
            }
        }
        let tx_id = match store.begin_tx_at_snapshot(
            options.pager.clone(),
            options.expected_schema_generation,
            checkpoint_read_guard,
            options.portable_logical_changes,
            BeginTimestamp::Shared(snapshot),
        ) {
            Ok(tx_id) => tx_id,
            Err(error) => {
                if opened_read {
                    options.pager.end_read_tx();
                }
                return Err(error.into());
            }
        };
        if options.exclusive {
            if let Err(error) = store.begin_exclusive_tx_with_checkpoint_read_guard(
                options.pager.clone(),
                Some(tx_id),
                connection,
                options.expected_schema_generation,
                CheckpointReadLockState::NotHeld,
            ) {
                store.rollback_tx(
                    tx_id,
                    options.pager.clone(),
                    connection,
                    options.database_id,
                );
                if opened_read {
                    options.pager.end_read_tx();
                }
                return Err(error.into());
            }
        }
        if let Err(error) = loaded.into_guard()?.finish() {
            store.rollback_tx(
                tx_id,
                options.pager.clone(),
                connection,
                options.database_id,
            );
            if opened_read {
                options.pager.end_read_tx();
            }
            return Err(error.into());
        }
        Ok(IOResult::Done(tx_id))
    }

    fn reset_reader(&mut self) -> Result<()> {
        let record = self.loaded.as_ref().unwrap().record();
        let state = self.state.as_mut().unwrap();
        if record.database_id() != state.observed.database_id()
            || record.log_generation() != state.observed.log_generation()
            || record.sequence() < state.observed.sequence()
            || record.prefix().committed_timestamp >= crate::mvcc::shared_authority::MAX_TIMESTAMP
            || record.prefix().committed_timestamp < state.context.max_commit_ts_seen
        {
            return Err(LimboError::Corrupt(
                "shared replay boundary changed incompatibly".into(),
            ));
        }
        crate::mvcc::commit_manifest::select_latest(
            [Some(state.observed), Some(record)],
            record.database_id(),
        )?;
        if record.prefix().end_offset != 0 {
            let file = crate::mvcc::read_pinned_file::ReadPinnedFile::for_manifest_replay(
                self.file.clone(),
                self.loaded.as_ref().unwrap(),
            )?;
            state.context.reader = Box::new(
                StreamingLogicalLogReader::new_deferred(file, None).with_committed_prefix(
                    record.prefix().end_offset,
                    record.prefix().running_crc,
                )?,
            );
        }
        self.header_read = false;
        self.last_frame_timestamp = 0;
        Ok(())
    }

    pub(super) fn observe_local_commit(mut self, timestamp: u64) -> Result<Self> {
        if self
            .loaded
            .as_ref()
            .unwrap()
            .record()
            .prefix()
            .committed_timestamp
            != timestamp
        {
            return Err(LimboError::Corrupt(
                "local commit differs from selected manifest".into(),
            ));
        }
        self.locally_committed = Some(timestamp);
        Ok(self)
    }

    pub(super) fn poll<C: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        store: &MvStore<C, A>,
        connection: &Arc<Connection>,
    ) -> IOResultOr<LoadedManifest> {
        if self.finished {
            return Err(
                LimboError::InvalidArgument("shared refresh already completed".into()).into(),
            );
        }
        let record = self.loaded.as_ref().unwrap().record();
        let prefix = record.prefix();
        let guard = self.loaded.as_ref().unwrap().guard()?;
        if !guard.requires_recovery() && guard.published()? != prefix {
            return Err(LimboError::Corrupt(
                "mapped MVCC prefix differs from durable manifest".into(),
            )
            .into());
        }
        if prefix.end_offset == 0 {
            let context = &self.state.as_ref().unwrap().context;
            if record.log_salt() != 0
                || context.persistent_tx_ts_max != prefix.committed_timestamp
                || context.max_commit_ts_seen != prefix.committed_timestamp
            {
                return Err(LimboError::Corrupt(
                    "empty MVCC prefix differs from materialized boundary".into(),
                )
                .into());
            }
        } else {
            if !self.header_read {
                let context = &mut self.state.as_mut().unwrap().context;
                match return_if_io!(context.reader.try_read_header_nonblock()) {
                    HeaderReadResult::Valid(header) if header.salt == record.log_salt() => {}
                    _ => {
                        return Err(LimboError::Corrupt(
                            "acknowledged MVCC header is missing or changed".into(),
                        )
                        .into())
                    }
                }
                self.header_read = true;
            }
            loop {
                let context = &mut self.state.as_mut().unwrap().context;
                if self.pending.is_some() {
                    if let Some(timestamp) = self.locally_committed {
                        store.observe_local_replay_frame(
                            connection,
                            context,
                            &mut self.pending,
                            timestamp,
                        )?;
                    } else {
                        store.install_peer_frame(connection, context, &mut self.pending)?;
                    }
                }
                let Some(frame) = return_if_io!(context.reader.next_transaction_frame()) else {
                    break;
                };
                if frame.commit_ts <= self.last_frame_timestamp
                    || frame.commit_ts > prefix.committed_timestamp
                {
                    return Err(LimboError::Corrupt(
                        "acknowledged MVCC frame timestamps are inconsistent".into(),
                    )
                    .into());
                }
                self.last_frame_timestamp = frame.commit_ts;
                if frame.commit_ts <= context.max_commit_ts_seen {
                    continue;
                }
                let prepared = if frame.ops.is_empty() {
                    Ok(PreparedRecoveryFrame {
                        commit_timestamp: frame.commit_ts,
                        decoded_frame: crate::alloc::Vec::try_with_capacity_ext(0)?,
                        schema_rows_after: None,
                        type_sqls_after: None,
                        schema_after: None,
                        index_infos: context.index_infos.clone(),
                    })
                } else {
                    store.prepare_recovery_frame(connection, context, frame.ops)
                };
                match prepared {
                    Ok(prepared) => self.pending = Some(prepared),
                    Err(error) => {
                        // Preparation consumed the parsed frame. Restart at the fixed boundary
                        // on retry, skipping only frames already installed in this context.
                        self.reset_reader()?;
                        return Err(error.into());
                    }
                }
            }
            let context = &self.state.as_ref().unwrap().context;
            if self.last_frame_timestamp != prefix.committed_timestamp
                || context.max_commit_ts_seen != prefix.committed_timestamp
            {
                return Err(LimboError::Corrupt(
                    "acknowledged MVCC timestamp does not end its prefix".into(),
                )
                .into());
            }
        }
        // Only a fully checked prefix can clear recovery or seed a subsequent append.
        let loaded = self.loaded.as_mut().unwrap();
        if loaded.guard()?.requires_recovery() {
            loaded.guard_mut()?.recover(prefix)?;
        }
        if !loaded.guard()?.io_retired() {
            return Ok(IOResult::IO(IOCompletions(
                loaded.guard()?.retirement_completion(),
            )));
        }
        let header = if prefix.end_offset == 0 {
            None
        } else {
            self.state
                .as_ref()
                .unwrap()
                .context
                .reader
                .header()
                .cloned()
        };
        self.owner.storage.adopt_shared_prefix(
            header,
            prefix.end_offset,
            prefix.running_crc,
            prefix.committed_timestamp,
        )?;
        self.state.as_mut().unwrap().observed = record;
        self.finished = true;
        self.return_context();
        Ok(IOResult::Done(self.loaded.take().unwrap()))
    }
}

impl SharedRefresh {
    fn return_context(&mut self) {
        if let Some(state) = self.state.take() {
            let previous = self.owner.state.lock().replace(state);
            assert!(previous.is_none(), "shared replay context has two owners");
        }
    }
}

impl<C: LogicalClock, A: ConcurrentAllocator> MvStore<C, A> {
    fn observe_local_replay_frame(
        &self,
        connection: &Arc<Connection>,
        context: &mut RecoverCtx,
        pending: &mut Option<PreparedRecoveryFrame>,
        expected_timestamp: u64,
    ) -> Result<()> {
        let frame = pending.as_ref().unwrap();
        if frame.commit_timestamp != expected_timestamp
            || self.last_committed_tx_ts.load(Ordering::Acquire) != expected_timestamp
        {
            return Err(LimboError::Corrupt(
                "local replay context is not a finalized commit".into(),
            ));
        }
        let mut dropped = context.dropped_root_pages.clone();
        let mut touched = HashMap::default();
        touched.try_reserve(frame.decoded_frame.len())?;
        for op in &frame.decoded_frame {
            match op {
                StreamingResult::UpsertTableRow { row, .. }
                    if row.id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID =>
                {
                    let root = self.validate_recovery_schema_root(row.payload())?;
                    if root.1 && root.0 > 0 {
                        dropped.remove(&root.0);
                    }
                    touched.insert(row.id.row_id.to_int_or_panic(), Some(root));
                }
                StreamingResult::DeleteTableRow { rowid, .. }
                    if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID =>
                {
                    let id = rowid.row_id.to_int_or_panic();
                    let root = match touched.get(&id) {
                        Some(root) => *root,
                        None => context
                            .schema_rows
                            .get(&id)
                            .map(|record| self.validate_recovery_schema_root(record.as_blob()))
                            .transpose()?,
                    };
                    if let Some((root, true)) = root {
                        if root > 0 {
                            dropped.insert(root);
                        }
                    }
                    touched.insert(id, None);
                }
                _ => {}
            }
        }
        let frame = pending.take().unwrap();
        let schema_changed = frame.schema_after.is_some();
        if let Some(rows) = frame.schema_rows_after {
            context.schema_rows = rows;
        }
        if let Some(types) = frame.type_sqls_after {
            context.type_sqls = types;
        }
        if schema_changed {
            context.current_schema = connection.schema.read().clone();
        }
        context.index_infos = if schema_changed {
            HashMap::default()
        } else {
            frame.index_infos
        };
        context.dropped_root_pages = dropped;
        context.max_commit_ts_seen = expected_timestamp;
        Ok(())
    }
}

impl Drop for SharedRefresh {
    fn drop(&mut self) {
        self.return_context();
    }
}

#[cfg(all(test, not(shuttle)))]
mod tests {
    use super::*;
    use crate::io::{Buffer, FileId, OpenFlags, SharedWalLockKind};
    use crate::mvcc::commit_manifest::{ManifestLoad, SLOT_SIZE};
    use crate::mvcc::shared_authority::{authority_for_test, PublishedPrefix};

    struct AuthorityFile;
    impl File for AuthorityFile {
        fn lock_file(&self, _: bool) -> Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }
        fn size(&self) -> Result<u64> {
            Ok(4096)
        }
        fn pread(&self, _: u64, _: Completion) -> Result<Completion> {
            unreachable!()
        }
        fn pwrite(&self, _: u64, _: Arc<Buffer>, _: Completion) -> Result<Completion> {
            unreachable!()
        }
        fn sync(&self, _: Completion, _: FileSyncType) -> Result<Completion> {
            unreachable!()
        }
        fn truncate(&self, _: u64, _: Completion) -> Result<Completion> {
            unreachable!()
        }
        fn shared_wal_try_lock_byte(&self, _: u64, _: bool, _: SharedWalLockKind) -> Result<bool> {
            Ok(true)
        }
        fn shared_wal_unlock_byte(&self, _: u64, _: SharedWalLockKind) -> Result<()> {
            Ok(())
        }
    }

    fn context(db: &super::super::tests::MvccTestDb) -> Box<RecoverCtx> {
        Box::new(RecoverCtx {
            reader: Box::new(StreamingLogicalLogReader::new(
                db.mvcc_store.get_logical_log_file(),
                None,
            )),
            preserved_table_valued_functions: Vec::new(),
            cookie: 0,
            persistent_tx_ts_max: 0,
            replay_cutoff_ts: 0,
            max_commit_ts_seen: 0,
            schema_rows: HashMap::default(),
            type_sqls: BTreeMap::new(),
            dropped_root_pages: HashSet::default(),
            current_schema: db.conn.schema.read().clone(),
            index_infos: HashMap::default(),
        })
    }

    #[test]
    fn shared_refresh_empty_acknowledged_prefix_ignores_physical_tail() {
        use crate::IO;
        let db = super::super::tests::MvccTestDb::new();
        let io: Arc<dyn IO> = Arc::new(crate::MemoryIO::new());
        let file = io
            .open_file("unacknowledged-log", OpenFlags::Create, false)
            .unwrap();
        io.wait_for_completion(
            file.pwrite(
                0,
                Arc::new(Buffer::new(vec![255; 4096])),
                Completion::new_write(|_| {}),
            )
            .unwrap(),
        )
        .unwrap();
        let record = ManifestRecord::new(
            1,
            FileId { dev: 1, ino: 2 },
            1,
            0,
            PublishedPrefix::default(),
        )
        .unwrap();
        let manifest = io.open_file("manifest", OpenFlags::Create, false).unwrap();
        let mut bytes = vec![0; SLOT_SIZE * 2];
        bytes[..128].copy_from_slice(&record.encode());
        io.wait_for_completion(
            manifest
                .pwrite(
                    0,
                    Arc::new(Buffer::new(bytes)),
                    Completion::new_write(|_| {}),
                )
                .unwrap(),
        )
        .unwrap();
        let authority = authority_for_test(Arc::new(AuthorityFile));
        let guard = authority.try_acquire().unwrap();
        let mut load = ManifestLoad::new(manifest, guard, record.database_id()).unwrap();
        let loaded = io.block(|| Ok(load.poll()?)).unwrap();
        let replay = SharedReplayContext::new(
            context(&db),
            record,
            Arc::new(Storage::new(file.clone(), io.clone(), None)),
        );
        let mut refresh = replay.refresh(loaded, file).unwrap();
        let loaded = io
            .block(|| refresh.poll(db.mvcc_store.as_ref(), &db.conn))
            .unwrap();
        assert_eq!(
            loaded.guard().unwrap().published().unwrap(),
            record.prefix()
        );
        loaded.into_guard().unwrap().finish().unwrap();
        drop(refresh);
        assert_eq!(
            replay
                .state
                .lock()
                .as_ref()
                .unwrap()
                .context
                .max_commit_ts_seen,
            0
        );
    }
    fn loaded(
        io: &Arc<dyn crate::IO>,
        record: ManifestRecord,
        authority: &Arc<crate::mvcc::shared_authority::SharedMvccAuthority>,
    ) -> LoadedManifest {
        let manifest = io
            .open_file("selected-manifest", OpenFlags::Create, false)
            .unwrap();
        let mut bytes = vec![0; SLOT_SIZE * 2];
        bytes[..128].copy_from_slice(&record.encode());
        io.wait_for_completion(
            manifest
                .pwrite(
                    0,
                    Arc::new(Buffer::new(bytes)),
                    Completion::new_write(|_| {}),
                )
                .unwrap(),
        )
        .unwrap();
        let mut load = ManifestLoad::new(
            manifest,
            authority.try_acquire().unwrap(),
            record.database_id(),
        )
        .unwrap();
        io.block(|| Ok(load.poll()?)).unwrap()
    }

    #[test]
    fn shared_refresh_checks_whole_prefix_and_retries_after_partial_import() {
        use super::super::tests::{generate_simple_string_row, MvccTestDb};
        use crate::mvcc::persistent_storage::logical_log::LogicalLog;
        for corruption in 0..3 {
            let db = MvccTestDb::new();
            let store = &db.mvcc_store;
            let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
            let file = io.open_file("peer-log", OpenFlags::Create, false).unwrap();
            let mut log = LogicalLog::new(file.clone(), io.clone(), None);
            let row = generate_simple_string_row((-22).into(), 1, "peer");
            store.insert_table_id_to_rootpage(row.id.table_id, None);
            let old_reader = store.begin_tx(db.conn.pager.load().clone()).unwrap();
            let version = RowVersion::new(
                1,
                Some(TxTimestampOrID::Timestamp(10)),
                None,
                row.data.clone(),
                false,
            );
            io.wait_for_completion(
                log.log_tx(LogRecord::for_test(10, &[(row.id.clone(), version)], None))
                    .unwrap(),
            )
            .unwrap();
            io.wait_for_completion(log.log_tx(LogRecord::for_test(20, &[], None)).unwrap())
                .unwrap();
            let prefix = PublishedPrefix {
                committed_timestamp: 20,
                end_offset: log.offset,
                running_crc: log.running_crc,
            };
            let initial = ManifestRecord::new(
                1,
                FileId { dev: 1, ino: 2 },
                1,
                0,
                PublishedPrefix::default(),
            )
            .unwrap();
            let record = ManifestRecord::new(
                2,
                initial.database_id(),
                1,
                log.header().unwrap().salt,
                prefix,
            )
            .unwrap();
            // A complete unacknowledged frame must not advance the imported frontier.
            io.wait_for_completion(log.log_tx(LogRecord::for_test(30, &[], None)).unwrap())
                .unwrap();
            let mut bad_prefix = prefix;
            let salt = match corruption {
                0 => {
                    bad_prefix.running_crc ^= 1;
                    record.log_salt()
                }
                1 => {
                    bad_prefix.committed_timestamp = 19;
                    record.log_salt()
                }
                _ => record.log_salt() ^ 1,
            };
            let bad = ManifestRecord::new(2, initial.database_id(), 1, salt, bad_prefix).unwrap();
            let authority = authority_for_test(Arc::new(AuthorityFile));
            let replay = SharedReplayContext::new(
                context(&db),
                initial,
                Arc::new(Storage::new(file.clone(), io.clone(), None)),
            );
            let mut refresh = replay
                .refresh(loaded(&io, bad, &authority), file.clone())
                .unwrap();
            assert!(io.block(|| refresh.poll(store.as_ref(), &db.conn)).is_err());
            assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
            drop(refresh);
            assert!(authority.try_acquire().unwrap().requires_recovery());
            let mut refresh = replay
                .refresh(loaded(&io, record, &authority), file)
                .unwrap();
            let loaded = io.block(|| refresh.poll(store.as_ref(), &db.conn)).unwrap();
            assert_eq!(loaded.guard().unwrap().published().unwrap(), prefix);
            let chain = store.rows.get(&row.id).unwrap();
            assert_eq!(
                chain.value().read().len(),
                1,
                "retry installed duplicate row version"
            );
            assert!(store.read(old_reader, &row.id).unwrap().is_none());
            loaded.into_guard().unwrap().finish().unwrap();
            drop(refresh);
            assert_eq!(
                replay
                    .state
                    .lock()
                    .as_ref()
                    .unwrap()
                    .context
                    .max_commit_ts_seen,
                20
            );
            store.rollback_tx(
                old_reader,
                db.conn.pager.load().clone(),
                &db.conn,
                crate::MAIN_DB_ID,
            );
        }
    }
    #[test]
    fn shared_refresh_local_commit_updates_context_without_rewriting_live_chains() {
        use super::super::tests::{generate_simple_string_row, MvccTestDb};
        use crate::mvcc::persistent_storage::logical_log::LogicalLog;
        let db = MvccTestDb::new();
        let store = &db.mvcc_store;
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let file = io.open_file("local-log", OpenFlags::Create, false).unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);
        let row = generate_simple_string_row((-22).into(), 1, "local");
        store.insert_table_id_to_rootpage(row.id.table_id, None);
        let tx = store.begin_tx(db.conn.pager.load().clone()).unwrap();
        store.insert(tx, row.clone()).unwrap();
        let version = RowVersion::new(
            123,
            Some(TxTimestampOrID::Timestamp(10)),
            None,
            row.data.clone(),
            false,
        );
        io.wait_for_completion(
            log.log_tx(LogRecord::for_test(10, &[(row.id.clone(), version)], None))
                .unwrap(),
        )
        .unwrap();
        let initial = ManifestRecord::new(
            1,
            FileId { dev: 1, ino: 2 },
            1,
            0,
            PublishedPrefix::default(),
        )
        .unwrap();
        let selected = ManifestRecord::new(
            2,
            initial.database_id(),
            1,
            log.header().unwrap().salt,
            PublishedPrefix {
                committed_timestamp: 10,
                end_offset: log.offset,
                running_crc: log.running_crc,
            },
        )
        .unwrap();
        let authority = authority_for_test(Arc::new(AuthorityFile));
        let replay = SharedReplayContext::new(
            context(&db),
            initial,
            Arc::new(Storage::new(file.clone(), io.clone(), None)),
        );
        let chain = store.rows.get(&row.id).unwrap();
        let original_id = chain.value().read()[0].id;
        store.last_committed_tx_ts.store(10, Ordering::Release);
        let mut refresh = replay
            .refresh(loaded(&io, selected, &authority), file)
            .unwrap()
            .observe_local_commit(10)
            .unwrap();
        let selected = io.block(|| refresh.poll(store.as_ref(), &db.conn)).unwrap();
        assert!(store
            .txs
            .get(&tx)
            .unwrap()
            .value()
            .ensure_peer_unconflicted()
            .is_ok());
        assert_eq!(chain.value().read().len(), 1);
        assert_eq!(chain.value().read()[0].id, original_id);
        assert_eq!(
            replay
                .state
                .lock()
                .as_ref()
                .unwrap()
                .context
                .max_commit_ts_seen,
            10
        );
        selected.into_guard().unwrap().finish().unwrap();
        store.rollback_tx(
            tx,
            db.conn.pager.load().clone(),
            &db.conn,
            crate::MAIN_DB_ID,
        );
    }

    #[test]
    fn shared_refresh_validates_retained_prefix_after_base_materialization() {
        use super::super::tests::MvccTestDb;
        use crate::mvcc::persistent_storage::logical_log::LogicalLog;
        let db = MvccTestDb::new();
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let file = io
            .open_file("checkpoint-log", OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);
        for timestamp in [10, 20] {
            io.wait_for_completion(
                log.log_tx(LogRecord::for_test(timestamp, &[], None))
                    .unwrap(),
            )
            .unwrap();
        }
        let record = ManifestRecord::new(
            3,
            FileId { dev: 1, ino: 2 },
            1,
            log.header().unwrap().salt,
            PublishedPrefix {
                committed_timestamp: 20,
                end_offset: log.offset,
                running_crc: log.running_crc,
            },
        )
        .unwrap();
        let mut base = context(&db);
        base.persistent_tx_ts_max = 20;
        base.max_commit_ts_seen = 20;
        base.replay_cutoff_ts = 20;
        let authority = authority_for_test(Arc::new(AuthorityFile));
        let replay = SharedReplayContext::new(
            base,
            record,
            Arc::new(Storage::new(file.clone(), io.clone(), None)),
        );
        let mut refresh = replay
            .refresh(loaded(&io, record, &authority), file)
            .unwrap();
        let loaded = io
            .block(|| refresh.poll(db.mvcc_store.as_ref(), &db.conn))
            .unwrap();
        loaded.into_guard().unwrap().finish().unwrap();
        assert_eq!(
            replay
                .state
                .lock()
                .as_ref()
                .unwrap()
                .context
                .max_commit_ts_seen,
            20
        );
    }

    struct DelayedLogFile {
        inner: Arc<dyn File>,
        pending: Mutex<Option<(u64, Completion)>>,
    }
    impl DelayedLogFile {
        fn retire(&self) {
            drop(self.complete_retained());
        }
        fn complete_retained(&self) -> Completion {
            let (offset, completion) = self.pending.lock().take().unwrap();
            let retained = completion.clone();
            let buffer = Arc::new(Buffer::new_temporary(
                completion.as_read().buf().len().min(64),
            ));
            drop(
                self.inner
                    .pread(
                        offset,
                        Completion::new_read(buffer, move |result| {
                            match result {
                                Ok((buffer, count)) => {
                                    completion.as_read().buf().as_mut_slice()[..count as usize]
                                        .copy_from_slice(&buffer.as_slice()[..count as usize]);
                                    completion.complete(count);
                                }
                                Err(error) => completion.error(error),
                            }
                            None
                        }),
                    )
                    .unwrap(),
            );
            retained
        }
    }
    impl File for DelayedLogFile {
        fn lock_file(&self, _: bool) -> Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }
        fn size(&self) -> Result<u64> {
            self.inner.size()
        }
        fn pread(&self, pos: u64, completion: Completion) -> Result<Completion> {
            assert!(self
                .pending
                .lock()
                .replace((pos, completion.clone()))
                .is_none());
            Ok(completion)
        }
        fn pwrite(&self, _: u64, _: Arc<Buffer>, _: Completion) -> Result<Completion> {
            unreachable!()
        }
        fn sync(&self, _: Completion, _: FileSyncType) -> Result<Completion> {
            unreachable!()
        }
        fn truncate(&self, _: u64, _: Completion) -> Result<Completion> {
            unreachable!()
        }
    }

    #[test]
    fn shared_refresh_cancellation_keeps_imported_context_and_restarts_reader() {
        use super::super::tests::{generate_simple_string_row, MvccTestDb};
        use crate::mvcc::persistent_storage::logical_log::LogicalLog;
        let db = MvccTestDb::new();
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let file = io
            .open_file("cancelled-peer-log", OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);
        let row = generate_simple_string_row((-22).into(), 1, "peer");
        db.mvcc_store
            .insert_table_id_to_rootpage(row.id.table_id, None);
        let version = RowVersion::new(
            1,
            Some(TxTimestampOrID::Timestamp(10)),
            None,
            row.data.clone(),
            false,
        );
        io.wait_for_completion(
            log.log_tx(LogRecord::for_test(10, &[(row.id.clone(), version)], None))
                .unwrap(),
        )
        .unwrap();
        for timestamp in [20, 30, 40, 50] {
            io.wait_for_completion(
                log.log_tx(LogRecord::for_test(timestamp, &[], None))
                    .unwrap(),
            )
            .unwrap();
        }
        let prefix = PublishedPrefix {
            committed_timestamp: 50,
            end_offset: log.offset,
            running_crc: log.running_crc,
        };
        let initial = ManifestRecord::new(
            1,
            FileId { dev: 1, ino: 2 },
            1,
            0,
            PublishedPrefix::default(),
        )
        .unwrap();
        let record = ManifestRecord::new(
            2,
            initial.database_id(),
            1,
            log.header().unwrap().salt,
            prefix,
        )
        .unwrap();
        let authority = authority_for_test(Arc::new(AuthorityFile));
        let replay = SharedReplayContext::new(
            context(&db),
            initial,
            Arc::new(Storage::new(file.clone(), io.clone(), None)),
        );
        let delayed = Arc::new(DelayedLogFile {
            inner: file.clone(),
            pending: Mutex::new(None),
        });
        let mut refresh = replay
            .refresh(loaded(&io, record, &authority), delayed.clone())
            .unwrap();
        let mut cancelled_after_import = false;
        for _ in 0..32 {
            assert!(matches!(
                refresh.poll(db.mvcc_store.as_ref(), &db.conn).unwrap(),
                IOResult::IO(_)
            ));
            if refresh.state.as_ref().unwrap().context.max_commit_ts_seen >= 10 {
                cancelled_after_import = true;
                break;
            }
            delayed.retire();
        }
        assert!(cancelled_after_import);
        drop(refresh);
        assert!(
            matches!(authority.try_acquire(), Err(LimboError::Busy)),
            "cancelled replay released authority before physical callback"
        );
        let physical = delayed.complete_retained();
        assert!(
            matches!(authority.try_acquire(), Err(LimboError::Busy)),
            "cancelled replay callback released backend authority"
        );
        drop(physical);
        assert!(authority.try_acquire().unwrap().requires_recovery());
        let mut refresh = replay
            .refresh(loaded(&io, record, &authority), file)
            .unwrap();
        let loaded = io
            .block(|| refresh.poll(db.mvcc_store.as_ref(), &db.conn))
            .unwrap();
        assert_eq!(loaded.guard().unwrap().published().unwrap(), prefix);
        assert_eq!(
            db.mvcc_store
                .rows
                .get(&row.id)
                .unwrap()
                .value()
                .read()
                .len(),
            1
        );
        loaded.into_guard().unwrap().finish().unwrap();
    }
    #[test]
    fn shared_refresh_begin_uses_global_snapshot_and_retains_registration() {
        use super::super::tests::MvccTestDb;
        for exhausted in [false, true] {
            let db = MvccTestDb::new();
            db.mvcc_store.clock.reset(10_000);
            let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
            let file = io
                .open_file("begin-peer-log", OpenFlags::Create, false)
                .unwrap();
            let committed_timestamp = if exhausted { (1_u64 << 62) - 3 } else { 0 };
            let prefix = PublishedPrefix {
                committed_timestamp,
                ..PublishedPrefix::default()
            };
            let record = ManifestRecord::new(1, FileId { dev: 1, ino: 2 }, 1, 0, prefix).unwrap();
            let authority = authority_for_test(Arc::new(AuthorityFile));
            let mut context = context(&db);
            context.persistent_tx_ts_max = committed_timestamp;
            context.max_commit_ts_seen = committed_timestamp;
            let replay = SharedReplayContext::new(
                context,
                record,
                Arc::new(Storage::new(file.clone(), io.clone(), None)),
            );
            let options = || SharedBeginOptions {
                pager: db.conn.pager.load().clone(),
                expected_schema_generation: None,
                portable_logical_changes: false,
                database_id: crate::MAIN_DB_ID,
                exclusive: false,
            };
            let mut refresh = replay
                .refresh(loaded(&io, record, &authority), file.clone())
                .unwrap();
            let tx_id = io
                .block(|| refresh.poll_begin(db.mvcc_store.as_ref(), &db.conn, options()))
                .unwrap();
            {
                let tx = db.mvcc_store.txs.get(&tx_id).unwrap();
                assert_eq!(tx.value().begin_ts, committed_timestamp + 1);
                assert!(tx
                    .value()
                    .shared_snapshot
                    .as_ref()
                    .unwrap()
                    .lock()
                    .is_some());
            }
            let guard = authority.try_acquire().unwrap();
            assert_eq!(
                guard.oldest_snapshot().unwrap(),
                Some(committed_timestamp + 1)
            );
            let live = guard.live_registrations().unwrap();
            assert_eq!(live.len(), 1);
            assert_eq!(live[0].1, committed_timestamp + 1);
            guard.finish().unwrap();
            db.mvcc_store.rollback_tx(
                tx_id,
                db.conn.pager.load().clone(),
                &db.conn,
                crate::MAIN_DB_ID,
            );
            let guard = authority.try_acquire().unwrap();
            assert_eq!(guard.oldest_snapshot().unwrap(), None);
            assert!(guard.live_registrations().unwrap().is_empty());
            guard.finish().unwrap();
            if exhausted {
                let mut refresh = replay
                    .refresh(loaded(&io, record, &authority), file)
                    .unwrap();
                assert!(io
                    .block(|| refresh.poll_begin(db.mvcc_store.as_ref(), &db.conn, options()))
                    .is_err());
                assert!(db.mvcc_store.txs.is_empty());
            }
        }
    }
}
