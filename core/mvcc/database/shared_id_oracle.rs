use super::{MvStore, SharedOracleTransaction, SQLITE_SCHEMA_MVCC_TABLE_ID};
use crate::alloc::ConcurrentAllocator;
use crate::mvcc::clock::LogicalClock;
use crate::mvcc::cursor::{MvccCursorType, MvccLazyCursor};
use crate::mvcc::shared_authority::AuthorityGuard;
use crate::mvcc::shared_ids::io_protocol::RecoveryIdentity;
use crate::mvcc::shared_ids::policy::{CommittedIdOracle, IdFact, IdQuery};
use crate::numeric::Numeric;
use crate::storage::btree::{BTreeCursor, CursorTrait};
use crate::sync::Arc;
use crate::types::{SeekKey, SeekOp, SeekResult, ValueRef};
use crate::{return_if_io, Connection, IOResult, LimboError, Result};

#[derive(Clone, Copy)]
enum Phase {
    Start,
    Last,
    Rowid,
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
        }
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
                IdQuery::LogicalFloor => {}
            }
            let header = self
                .context()
                .store()
                .get_transaction_database_header(&self.context().tx_id());
            self.floor = -i64::from(header.database_size.get()).max(1);
            let schema_table = i64::from(SQLITE_SCHEMA_MVCC_TABLE_ID);
            let intrinsic_schema = match *request {
                IdQuery::RowFacts { domain, .. } | IdQuery::RowExists { domain, .. } => {
                    domain.table == schema_table
                }
                IdQuery::LogicalFloor => false,
            };
            self.phase = if intrinsic_schema {
                Self::row_phase(request)
            } else {
                Phase::Rewind
            };
            self.cursor = Some(self.cursor(schema_table)?);
            self.request = Some(*request);
        }
        loop {
            let cursor = self.cursor.as_mut().expect("request owns cursor");
            match self.phase {
                Phase::Start => unreachable!(),
                Phase::Last => {
                    return_if_io!(cursor.last());
                    self.phase = Phase::Rowid;
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
                    self.floor = self.floor.min(-root.abs());
                    let requested = match *request {
                        IdQuery::RowFacts { domain, .. } | IdQuery::RowExists { domain, .. } => {
                            Some(domain.table)
                        }
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
