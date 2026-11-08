use super::{MvStore, SharedOracleTransaction, SQLITE_SCHEMA_MVCC_TABLE_ID};
use crate::alloc::ConcurrentAllocator;
use crate::mvcc::clock::LogicalClock;
use crate::mvcc::cursor::{MvccCursorType, MvccLazyCursor};
use crate::mvcc::shared_authority::AuthorityGuard;
use crate::mvcc::shared_ids::io_protocol::RecoveryIdentity;
use crate::mvcc::shared_ids::policy::{CommittedIdOracle, IdFact, IdQuery};
use crate::numeric::Numeric;
use crate::schema::Sequence;
use crate::storage::btree::{BTreeCursor, CursorTrait};
use crate::storage::checksum::CHECKSUM_REQUIRED_RESERVED_BYTES;
use crate::sync::Arc;
use crate::types::{SeekKey, SeekOp, SeekResult, ValueRef};
use crate::{return_if_io, Connection, IOResult, LimboError, Result};

#[derive(Clone, Copy)]
enum Phase {
    Start,
    Last,
    Rowid,
    SequenceEdge(bool),
    SequenceRecord,
    Seek(i64),
    Rewind,
    SchemaRecord,
    Next,
}

pub(super) struct SharedIdOracle<C: LogicalClock + 'static, A: ConcurrentAllocator> {
    cursor: Option<MvccLazyCursor<C, A>>,
    connection: Arc<Connection>,
    request: Option<IdQuery>,
    phase: Phase,
    floor: i64,
    failed: bool,
    context: Option<SharedOracleTransaction<C, A>>,
    store: Arc<MvStore<C, A>>,
    identity: RecoveryIdentity,
    expected_sequence: Option<(String, Arc<Sequence>)>,
}

impl<C: LogicalClock + 'static, A: ConcurrentAllocator> SharedIdOracle<C, A> {
    pub(super) fn new(
        store: Arc<MvStore<C, A>>,
        connection: Arc<Connection>,
        identity: RecoveryIdentity,
    ) -> Self {
        Self {
            cursor: None,
            connection,
            request: None,
            phase: Phase::Start,
            floor: -1,
            failed: false,
            context: None,
            store,
            identity,
            expected_sequence: None,
        }
    }

    pub(super) fn expect_sequence(mut self, backing_name: String, sequence: Arc<Sequence>) -> Self {
        self.expected_sequence = Some((backing_name, sequence));
        self
    }

    fn context(&self) -> &SharedOracleTransaction<C, A> {
        self.context
            .as_ref()
            .expect("active oracle query owns context")
    }

    fn cursor(&self, table: i64) -> Result<MvccLazyCursor<C, A>> {
        let store = self.context().store();
        MvccLazyCursor::new(
            store.clone(),
            &self.connection,
            self.context().tx_id(),
            table,
            MvccCursorType::Table,
            Box::new(BTreeCursor::new(self.context().pager().clone(), table, 5)),
        )
    }

    fn done(&mut self, fact: IdFact) -> IOResult<IdFact> {
        self.cursor.take();
        self.context.take();
        self.request = None;
        self.phase = Phase::Start;
        IOResult::Done(fact)
    }

    fn poll_inner(
        &mut self,
        guard: &AuthorityGuard,
        request: &IdQuery,
    ) -> Result<IOResult<IdFact>> {
        if self.context.is_none() {
            self.context = Some(self.store.begin_shared_oracle(
                &self.connection,
                guard,
                self.identity,
            )?);
        }
        if !self.context().matches_guard(guard)
            || guard.published()? != self.context().identity().prefix
        {
            return Err(LimboError::InvalidArgument(
                "committed ID oracle authority or prefix changed".into(),
            ));
        }
        if self.request.is_some_and(|current| current != *request) {
            return Err(LimboError::InvalidArgument(
                "committed ID oracle request changed during I/O".into(),
            ));
        }
        if self.request.is_none() {
            match *request {
                IdQuery::RowFacts {
                    domain,
                    autoincrement,
                } => {
                    if autoincrement {
                        return Err(LimboError::InvalidArgument(
                            "shared MVCC AUTOINCREMENT must reserve its implicit sequence value"
                                .into(),
                        ));
                    }
                    self.validate_domain(domain)?;
                }
                IdQuery::RowExists { domain, .. } => self.validate_domain(domain)?,
                IdQuery::SequenceState { domain, .. } => {
                    if self.expected_sequence.is_none() {
                        return Err(LimboError::InvalidArgument(
                            "sequence oracle requires backing identity".into(),
                        ));
                    }
                    self.validate_domain(crate::mvcc::shared_ids::journal::Domain {
                        sequence: false,
                        ..domain
                    })?;
                    if !domain.sequence {
                        return Err(LimboError::InvalidArgument(
                            "sequence oracle requires a sequence domain".into(),
                        ));
                    }
                }
                IdQuery::LogicalFloor => {}
            }
            let header = self
                .context()
                .store()
                .get_transaction_database_header(&self.context().tx_id());
            self.floor = -i64::from(header.database_size.get()).max(1);
            let schema_table = i64::from(SQLITE_SCHEMA_MVCC_TABLE_ID);
            let intrinsic_schema = match *request {
                IdQuery::RowFacts { domain, .. }
                | IdQuery::RowExists { domain, .. }
                | IdQuery::SequenceState { domain, .. } => domain.table == schema_table,
                IdQuery::LogicalFloor => false,
            };
            self.phase = if intrinsic_schema {
                Self::row_phase(request)
            } else {
                Phase::Rewind
            };
            self.request = Some(*request);
        }
        let pager = self.context().pager();
        if pager.get_reserved_space().is_none() {
            let (page_size, reserved) = return_if_io!(
                pager.with_header(|header| (header.page_size, header.reserved_space))
            );
            if pager.get_page_size() != Some(page_size)
                || !page_size.has_valid_reserved_space(reserved)
                || (reserved == CHECKSUM_REQUIRED_RESERVED_BYTES)
                    != (pager.io_ctx.read().get_reserved_space_bytes()
                        == CHECKSUM_REQUIRED_RESERVED_BYTES)
            {
                return Err(LimboError::InvalidArgument(
                    "oracle header layout differs from the initialized database".into(),
                ));
            }
            pager.set_reserved_space_bytes(reserved);
        }
        if self.cursor.is_none() {
            self.cursor = Some(self.cursor(i64::from(SQLITE_SCHEMA_MVCC_TABLE_ID))?);
        }
        loop {
            let cursor = self.cursor.as_mut().expect("request owns cursor");
            match self.phase {
                Phase::Start => unreachable!(),
                Phase::Last => {
                    return_if_io!(cursor.last());
                    self.phase = Phase::Rowid;
                }
                Phase::SequenceEdge(descending) => {
                    if descending {
                        return_if_io!(cursor.rewind());
                    } else {
                        return_if_io!(cursor.last());
                    }
                    self.phase = Phase::SequenceRecord;
                }
                Phase::SequenceRecord => {
                    let value = return_if_io!(cursor.rowid());
                    let Some(value) = value else {
                        return Ok(self.done(IdFact::SequenceState {
                            committed: true,
                            row: None,
                        }));
                    };
                    let Some(record) = return_if_io!(cursor.record()) else {
                        return Err(LimboError::Corrupt(
                            "sequence backing row disappeared".into(),
                        ));
                    };
                    let expected = &self.expected_sequence.as_ref().unwrap().1;
                    for (index, expected_value) in [
                        (2, expected.start_value),
                        (3, expected.increment_by),
                        (4, expected.min_value),
                        (5, expected.max_value),
                        (6, i64::from(expected.cycle)),
                    ] {
                        let ValueRef::Numeric(Numeric::Integer(actual)) =
                            record.get_value(index)?
                        else {
                            return Err(LimboError::Corrupt(
                                "sequence descriptor contains a noninteger field".into(),
                            ));
                        };
                        if actual != expected_value {
                            return Err(LimboError::SchemaUpdated);
                        }
                    }
                    let called = match record.get_value(1)? {
                        ValueRef::Numeric(Numeric::Integer(0)) => false,
                        ValueRef::Numeric(Numeric::Integer(1)) => true,
                        _ => {
                            return Err(LimboError::Corrupt(
                                "sequence is_called is not boolean".into(),
                            ))
                        }
                    };
                    return Ok(self.done(IdFact::SequenceState {
                        committed: true,
                        row: Some((value, called)),
                    }));
                }
                Phase::Rowid => {
                    let maximum = return_if_io!(cursor.rowid());
                    return Ok(self.done(IdFact::RowFacts {
                        maximum,
                        autoincrement_floor: 0,
                    }));
                }
                Phase::Seek(value) => {
                    let result = return_if_io!(
                        cursor.seek(SeekKey::TableRowId(value), SeekOp::GE { eq_only: true })
                    );
                    let exists = match result {
                        SeekResult::Found => true,
                        SeekResult::NotFound => false,
                        SeekResult::TryAdvance => {
                            return Err(LimboError::InternalError(
                                "exact committed row lookup returned a range continuation".into(),
                            ))
                        }
                    };
                    return Ok(self.done(IdFact::RowExists(exists)));
                }
                Phase::Rewind => {
                    return_if_io!(cursor.rewind());
                    self.phase = Phase::SchemaRecord;
                }
                Phase::SchemaRecord => {
                    let Some(record) = return_if_io!(cursor.record()) else {
                        let fact = match *request {
                            IdQuery::LogicalFloor => IdFact::LogicalFloor(self.floor),
                            IdQuery::RowFacts { .. } => IdFact::RowFacts {
                                maximum: None,
                                autoincrement_floor: 0,
                            },
                            IdQuery::RowExists { .. } => IdFact::RowExists(false),
                            IdQuery::SequenceState { .. } => IdFact::SequenceState {
                                committed: false,
                                row: None,
                            },
                        };
                        return Ok(self.done(fact));
                    };
                    let ValueRef::Numeric(Numeric::Integer(root)) = record.get_value(3)? else {
                        return Err(LimboError::Corrupt(
                            "committed schema root is not an integer".into(),
                        ));
                    };
                    if root == i64::MIN {
                        return Err(LimboError::Corrupt(
                            "committed schema root is outside the logical ID domain".into(),
                        ));
                    }
                    let is_table = matches!(record.get_value(0)?, ValueRef::Text(kind) if kind.as_str() == "table");
                    let sequence_backing_matches =
                        if matches!(request, IdQuery::SequenceState { .. }) {
                            let expected = &self.expected_sequence.as_ref().unwrap().0;
                            let ValueRef::Text(name) = record.get_value(1)? else {
                                return Err(LimboError::Corrupt(
                                    "sequence schema name is not text".into(),
                                ));
                            };
                            crate::util::normalize_ident(name.as_str())
                                == crate::util::normalize_ident(expected)
                        } else {
                            true
                        };
                    self.floor = self.floor.min(-root.abs());
                    let requested = match *request {
                        IdQuery::RowFacts { domain, .. }
                        | IdQuery::RowExists { domain, .. }
                        | IdQuery::SequenceState { domain, .. } => Some(domain.table),
                        IdQuery::LogicalFloor => None,
                    };
                    if root != 0 && requested.is_some() {
                        let store = self.context().store();
                        let table = store
                            .try_get_table_id_from_root_page_at(
                                root,
                                store.read_snapshot_ts(self.context().tx_id()),
                            )
                            .ok_or_else(|| {
                                LimboError::Corrupt(
                                    "committed schema root has no logical domain".into(),
                                )
                            })?;
                        if requested == Some(i64::from(table)) {
                            if !is_table {
                                return Err(LimboError::InvalidArgument(
                                    "row allocation domain is not a committed table".into(),
                                ));
                            }
                            if !sequence_backing_matches {
                                return Err(LimboError::SchemaUpdated);
                            }
                            self.cursor = Some(self.cursor(root)?);
                            self.phase = Self::row_phase(request);
                            continue;
                        }
                    }
                    self.phase = Phase::Next;
                }
                Phase::Next => {
                    return_if_io!(cursor.next());
                    self.phase = Phase::SchemaRecord;
                }
            }
        }
    }

    fn row_phase(request: &IdQuery) -> Phase {
        match *request {
            IdQuery::RowFacts { .. } => Phase::Last,
            IdQuery::RowExists { value, .. } => Phase::Seek(value),
            IdQuery::LogicalFloor => unreachable!(),
            IdQuery::SequenceState { descending, .. } => Phase::SequenceEdge(descending),
        }
    }

    fn validate_domain(&self, domain: crate::mvcc::shared_ids::journal::Domain) -> Result<()> {
        if domain.generation != self.context().identity().database.generation
            || domain.table >= 0
            || domain.table == i64::MIN
            || domain.sequence
        {
            return Err(LimboError::InvalidArgument(
                "committed ID oracle domain differs from the refreshed generation".into(),
            ));
        }
        Ok(())
    }
}

impl<C: LogicalClock + 'static, A: ConcurrentAllocator> CommittedIdOracle for SharedIdOracle<C, A> {
    fn identity(&self) -> RecoveryIdentity {
        self.identity
    }
    fn poll(&mut self, guard: &AuthorityGuard, request: &IdQuery) -> Result<IOResult<IdFact>> {
        if self.failed {
            return Err(LimboError::InvalidArgument(
                "committed ID oracle already failed".into(),
            ));
        }
        let result = self.poll_inner(guard, request);
        if result.is_err() {
            self.failed = true;
            self.cursor.take();
            self.context.take();
        }
        result
    }
}

impl<C: LogicalClock + 'static, A: ConcurrentAllocator> Drop for SharedIdOracle<C, A> {
    fn drop(&mut self) {
        // The temporary transaction is removed only after no cursor can inspect its versions.
        self.cursor.take();
    }
}

#[cfg(all(test, not(shuttle)))]
mod fixture_tests {
    use super::*;

    #[test]
    fn stale_concurrent_snapshot_can_write_rows_but_cannot_upgrade_for_ddl() {
        let io = Arc::new(crate::io::MemoryIO::new());
        let db =
            crate::Database::open_file(io, ":memory:", Arc::new(crate::EngineDialect)).unwrap();
        let writer = db.connect(crate::ConnectOpts::new()).unwrap();
        writer.execute("PRAGMA journal_mode='mvcc'").unwrap();
        writer.execute("CREATE TABLE preserved(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO preserved VALUES(1,'base')").unwrap();
        let outer = db.connect(crate::ConnectOpts::new()).unwrap();
        outer.execute("BEGIN CONCURRENT").unwrap();
        outer.execute("SELECT * FROM preserved").unwrap();
        writer
            .execute("INSERT INTO preserved VALUES(99,'peer')")
            .unwrap();
        outer
            .execute("INSERT INTO preserved VALUES(500,'outer')")
            .expect("stale snapshot disjoint insert");
        outer
            .execute("DELETE FROM preserved WHERE id=1")
            .expect("stale snapshot delete unchanged row");
        assert!(matches!(
            outer.execute("CREATE TABLE pending(id INTEGER PRIMARY KEY)"),
            Err(LimboError::Busy)
        ));
        outer.execute("ROLLBACK").unwrap();
    }
}
