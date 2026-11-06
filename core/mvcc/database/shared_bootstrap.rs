use super::*;
use crate::mvcc::shared_authority::MAX_TIMESTAMP;
use crate::sync::Weak;

#[derive(Default)]
pub(super) struct SharedBaseContextState {
    stage: Stage,
    connection: Option<Weak<Connection>>,
    timestamp_state: ReadPersistentTxTsMaxState,
    persistent_timestamp: u64,
    cookie: u32,
    statement: Option<Box<crate::Statement>>,
    schema_rows: HashMap<i64, ImmutableRecord>,
    type_sqls: BTreeMap<i64, String>,
    schema: Option<Arc<Schema>>,
    table_valued_functions: Vec<Arc<crate::vtab::VirtualTable>>,
}

#[derive(Default)]
enum Stage {
    #[default]
    Start,
    Timestamp,
    Cookie,
    Schema,
    Types,
    Done,
}

impl<C: LogicalClock, A: ConcurrentAllocator> MvStore<C, A> {
    /// The caller protects the checkpoint generation and installs one base schema Arc in both caches.
    /// Otherwise normal statement preparation can replace the base schema with a newer logical schema.
    /// Reading this context must not publish replay state or change the transaction clock.
    pub(super) fn prepare_shared_base_context_nonblock(
        &self,
        connection: &Arc<Connection>,
        state: &mut SharedBaseContextState,
    ) -> IOResultOr<Box<RecoverCtx>> {
        let result = state.poll(self, connection);
        if result.is_err() {
            *state = SharedBaseContextState {
                stage: Stage::Done,
                ..Default::default()
            };
        }
        result
    }
}

impl SharedBaseContextState {
    fn poll<C: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        store: &MvStore<C, A>,
        connection: &Arc<Connection>,
    ) -> IOResultOr<Box<RecoverCtx>> {
        if !connection.is_mvcc_bootstrap_connection() || connection.db.is_in_memory_db() {
            return Err(LimboError::InvalidArgument(
                "shared base context requires a persistent bootstrap connection".into(),
            )
            .into());
        }
        let schema = connection.schema.read().clone();
        if !Arc::ptr_eq(&schema, &connection.db.schema.lock())
            || self
                .schema
                .as_ref()
                .is_some_and(|base| !Arc::ptr_eq(base, &schema))
        {
            return Err(LimboError::InvalidArgument(
                "shared base schema differs from the quiescent database schema".into(),
            )
            .into());
        }
        if self
            .connection
            .as_ref()
            .is_some_and(|original| !Weak::ptr_eq(original, &Arc::downgrade(connection)))
        {
            return Err(LimboError::InvalidArgument(
                "shared base context resumed with another connection".into(),
            )
            .into());
        }
        loop {
            match self.stage {
                Stage::Start => {
                    self.connection = Some(Arc::downgrade(connection));
                    let schema = connection.schema.read().clone();
                    self.table_valued_functions =
                        MvStore::<C, A>::capture_table_valued_functions(&schema);
                    self.schema = Some(schema);
                    self.stage = Stage::Timestamp;
                }
                Stage::Timestamp => {
                    self.persistent_timestamp = return_if_io!(store
                        .try_read_persistent_tx_ts_max_nonblock(
                            connection,
                            &mut self.timestamp_state
                        ))
                    .ok_or_else(|| {
                        LimboError::Corrupt("shared database has no MVCC metadata table".into())
                    })?;
                    if self.persistent_timestamp >= MAX_TIMESTAMP {
                        return Err(
                            LimboError::Corrupt("invalid shared base timestamp".into()).into()
                        );
                    }
                    self.stage = Stage::Cookie;
                }
                Stage::Cookie => {
                    self.cookie = return_if_io!(connection
                        .pager
                        .load()
                        .with_header(|header| header.schema_cookie))
                    .get();
                    self.statement = connection
                        .query(
                            "SELECT rowid, type, name, tbl_name, rootpage, sql FROM sqlite_schema",
                        )?
                        .map(Box::new);
                    self.stage = Stage::Schema;
                }
                Stage::Schema => {
                    if let Some(statement) = self.statement.as_mut() {
                        return_if_io!(statement.run_with_row_callback_nonblock(|row| {
                            let rowid = row.get::<i64>(0)?;
                            let values = (1..=5)
                                .map(|index| row.get_value(index).clone())
                                .collect::<Vec<_>>();
                            self.schema_rows.try_reserve(1)?;
                            self.schema_rows.insert(
                                rowid,
                                ImmutableRecord::from_values(&values, values.len())?,
                            );
                            Ok(())
                        }));
                    }
                    self.statement.take();
                    let types_in_base = connection.experimental_custom_types_enabled()
                        && self
                            .schema
                            .as_ref()
                            .expect("base schema captured")
                            .get_btree_table(
                                NamespaceId::ROOT,
                                crate::schema::TURSO_TYPES_TABLE_NAME,
                            )
                            .is_some_and(|table| table.root_page > 0);
                    self.statement = if types_in_base {
                        connection
                            .query(format!(
                                "SELECT rowid, sql FROM {}",
                                crate::schema::TURSO_TYPES_TABLE_NAME
                            ))?
                            .map(Box::new)
                    } else {
                        None
                    };
                    self.stage = Stage::Types;
                }
                Stage::Types => {
                    if let Some(statement) = self.statement.as_mut() {
                        return_if_io!(statement.run_with_row_callback_nonblock(|row| {
                            self.type_sqls
                                .insert(row.get::<i64>(0)?, row.get::<&str>(1)?.to_owned());
                            Ok(())
                        }));
                    }
                    let completed = std::mem::replace(
                        self,
                        Self {
                            stage: Stage::Done,
                            ..Default::default()
                        },
                    );
                    return Ok(IOResult::Done(Box::new(RecoverCtx {
                        reader: Box::new(StreamingLogicalLogReader::new_deferred(
                            store.get_logical_log_file(),
                            store.storage.encryption_ctx(),
                        )),
                        preserved_table_valued_functions: completed.table_valued_functions,
                        cookie: completed.cookie,
                        persistent_tx_ts_max: completed.persistent_timestamp,
                        replay_cutoff_ts: completed.persistent_timestamp,
                        max_commit_ts_seen: completed.persistent_timestamp,
                        schema_rows: completed.schema_rows,
                        type_sqls: completed.type_sqls,
                        dropped_root_pages: HashSet::default(),
                        current_schema: completed.schema.expect("base schema captured"),
                        index_infos: HashMap::default(),
                    })));
                }
                Stage::Done => {
                    return Err(LimboError::InvalidArgument(
                        "shared base context already completed or failed".into(),
                    )
                    .into())
                }
            }
        }
    }
}

#[cfg(all(test, not(shuttle)))]
mod tests {
    use super::super::tests::MvccTestDbNoConn;
    use super::*;
    use crate::alloc::TryClone;
    use crate::DatabaseOpts;

    fn materialized() -> (MvccTestDbNoConn, Arc<Connection>) {
        let fixture = MvccTestDbNoConn::new_with_random_db_with_opts(
            DatabaseOpts::new().with_custom_types(true),
        );
        let connection = fixture.connect();
        connection.execute("CREATE TABLE base_table(id INTEGER PRIMARY KEY, value TEXT); CREATE INDEX base_value ON base_table(value); INSERT INTO base_table VALUES(1, 'kept')").unwrap();
        connection.execute("CREATE TYPE my_uint(value any) BASE text ENCODE my_uint_enc(value) DECODE CAST(my_uint_dec(value) AS text)").unwrap();
        connection
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        (fixture, connection)
    }

    #[test]
    fn shared_base_context_reads_checkpoint_without_replay_or_publication() {
        let (fixture, connection) = materialized();
        let store = fixture.get_mvcc_store();
        let persistent = store.durable_txid_max.load(Ordering::SeqCst);
        assert!(persistent > 0);
        connection.execute("CREATE TABLE log_only(value)").unwrap();
        connection.demote_to_mvcc_connection();
        connection.reparse_schema().unwrap();
        let base_schema = connection.schema.read().clone();
        assert!(base_schema
            .get_table(NamespaceId::ROOT, "base_table")
            .is_some());
        assert!(base_schema
            .get_table(NamespaceId::ROOT, "log_only")
            .is_none());
        let global_schema = connection.db.schema.lock().clone();
        let mismatch = store.prepare_shared_base_context_nonblock(
            &connection,
            &mut SharedBaseContextState::default(),
        );
        assert!(mismatch.is_err());
        assert!(Arc::ptr_eq(&connection.schema.read(), &base_schema));
        assert!(Arc::ptr_eq(&connection.db.schema.lock(), &global_schema));
        *connection.db.schema.lock() = base_schema.clone();
        let global_schema = base_schema.clone();
        let tvfs = crate::MvStore::capture_table_valued_functions(&base_schema);
        let database_path = &connection.db.path;
        let log_path = format!("{database_path}-log");
        std::fs::write(&log_path, b"invalid unacknowledged log header").unwrap();
        let database_before = std::fs::read(database_path).unwrap();
        let log_before = std::fs::read(&log_path).unwrap();
        let durable_before = store.durable_txid_max.load(Ordering::SeqCst);
        let committed_before = store.last_committed_tx_ts.load(Ordering::SeqCst);
        let clock_before = store.clock.get_timestamp(crate::mvcc::clock::no_op);
        connection.pager.load().clear_page_cache(false);

        let mut state = SharedBaseContextState::default();
        let context = connection
            .db
            .io
            .block(|| store.prepare_shared_base_context_nonblock(&connection, &mut state))
            .unwrap();
        assert_eq!(context.persistent_tx_ts_max, persistent);
        assert_eq!(context.replay_cutoff_ts, persistent);
        assert_eq!(context.max_commit_ts_seen, persistent);
        assert!(Arc::ptr_eq(&context.current_schema, &base_schema));
        assert!(context.schema_rows.len() >= 3);
        assert_eq!(context.type_sqls.len(), 1);
        assert!(context
            .type_sqls
            .values()
            .next()
            .unwrap()
            .contains("my_uint"));
        assert_eq!(context.preserved_table_valued_functions.len(), tvfs.len());
        assert!(context
            .preserved_table_valued_functions
            .iter()
            .zip(&tvfs)
            .all(|(a, b)| Arc::ptr_eq(a, b)));
        assert!(context.index_infos.is_empty());
        assert!(context.dropped_root_pages.is_empty());
        assert!(context.reader.header().is_none());
        assert!(Arc::ptr_eq(&connection.schema.read(), &base_schema));
        assert!(Arc::ptr_eq(&connection.db.schema.lock(), &global_schema));
        assert_eq!(
            store.clock.get_timestamp(crate::mvcc::clock::no_op),
            clock_before + 1
        );
        assert_eq!(
            store.durable_txid_max.load(Ordering::SeqCst),
            durable_before
        );
        assert_eq!(
            store.last_committed_tx_ts.load(Ordering::SeqCst),
            committed_before
        );
        assert_eq!(std::fs::read(database_path).unwrap(), database_before);
        assert_eq!(std::fs::read(&log_path).unwrap(), log_before);
        assert!(store
            .prepare_shared_base_context_nonblock(&connection, &mut state)
            .is_err());
    }

    #[test]
    fn shared_base_context_rejects_regular_connection_and_missing_metadata() {
        let (fixture, connection) = materialized();
        let store = fixture.get_mvcc_store();
        assert!(store
            .prepare_shared_base_context_nonblock(
                &connection,
                &mut SharedBaseContextState::default()
            )
            .is_err());
        connection.demote_to_mvcc_connection();
        connection.reparse_schema().unwrap();
        connection
            .with_schema_mut(|schema| schema.remove_table(NamespaceId::ROOT, MVCC_META_TABLE_NAME))
            .unwrap();
        let before = connection.schema.read().clone();
        *connection.db.schema.lock() = before.clone();
        let mut state = SharedBaseContextState::default();
        let result = connection
            .db
            .io
            .block(|| store.prepare_shared_base_context_nonblock(&connection, &mut state));
        assert!(matches!(result, Err(LimboError::Corrupt(_))));
        assert!(Arc::ptr_eq(&before, &connection.schema.read()));
        assert!(connection
            .schema
            .read()
            .get_table(NamespaceId::ROOT, MVCC_META_TABLE_NAME)
            .is_none());
        assert!(matches!(state.stage, Stage::Done));
    }

    #[derive(Debug)]
    struct YieldOnce(AtomicBool);
    impl crate::mvcc::yield_points::YieldInjector for YieldOnce {
        fn should_yield(
            &self,
            _: u64,
            _: u64,
            point: crate::mvcc::yield_points::YieldPoint,
        ) -> bool {
            use crate::mvcc::yield_hooks::YieldPointMarker;
            point == crate::vdbe::execute::TransactionYieldPoint::BeforeStart.point()
                && self.0.swap(false, Ordering::SeqCst)
        }
    }

    #[test]
    fn shared_base_context_resumes_io_and_rejects_connection_switch() {
        let (fixture, connection) = materialized();
        let store = fixture.get_mvcc_store();
        connection.demote_to_mvcc_connection();
        connection.reparse_schema().unwrap();
        connection.pager.load().clear_page_cache(false);
        let other = fixture.connect();
        other.demote_to_mvcc_connection();
        other.reparse_schema().unwrap();
        *connection.db.schema.lock() = connection.schema.read().clone();
        *other.schema.write() = connection.schema.read().clone();
        connection.pager.load().clear_page_cache(false);
        connection.set_yield_injector(Some(Arc::new(YieldOnce(AtomicBool::new(true)))));
        let mut state = SharedBaseContextState::default();
        let first = store
            .prepare_shared_base_context_nonblock(&connection, &mut state)
            .unwrap();
        let IOResult::IO(io) = first else {
            panic!("base query must expose the injected yield")
        };
        io.wait(connection.db.io.as_ref()).unwrap();
        assert!(store
            .prepare_shared_base_context_nonblock(&other, &mut state)
            .is_err());
        assert!(matches!(state.stage, Stage::Done));
        connection.pager.load().clear_page_cache(false);
        let mut retry = SharedBaseContextState::default();
        let context = connection
            .db
            .io
            .block(|| store.prepare_shared_base_context_nonblock(&connection, &mut retry))
            .unwrap();
        assert!(!context.schema_rows.is_empty());
        assert_eq!(context.type_sqls.len(), 1);
        connection.set_yield_injector(None);
        connection.set_yield_injector(Some(Arc::new(YieldOnce(AtomicBool::new(true)))));
        let mut changed = SharedBaseContextState::default();
        assert!(store
            .prepare_shared_base_context_nonblock(&connection, &mut changed)
            .unwrap()
            .is_io());
        let replacement = Arc::new(connection.schema.read().as_ref().try_clone().unwrap());
        *connection.schema.write() = replacement.clone();
        *connection.db.schema.lock() = replacement;
        let result = store.prepare_shared_base_context_nonblock(&connection, &mut changed);
        assert!(matches!(result, Err(error) if matches!(*error, LimboError::InvalidArgument(_))));
        assert!(matches!(changed.stage, Stage::Done));
    }
}
