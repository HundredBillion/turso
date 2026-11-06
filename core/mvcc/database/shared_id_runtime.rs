use super::*;
use crate::mvcc::database::shared_id_oracle::SharedIdOracle;
use crate::mvcc::shared_ids::journal::{Domain, Owner};
use crate::mvcc::shared_ids::policy::{IdAction, IdOperation, IdResult, SequenceDurability};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SharedIdRequest {
    ReserveRow {
        table_id: i64,
    },
    ClaimExplicitRow {
        table_id: i64,
        value: i64,
    },
    ReserveLogicalTable,
    ClaimSequence {
        table_id: i64,
        value: i64,
        autonomous: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SharedIdResult {
    Value(i64),
    Claimed,
}

#[derive(Default)]
pub(crate) struct SharedIdState {
    pending: Option<PendingId>,
}

struct PendingId {
    connection: Weak<Connection>,
    pager: Arc<Pager>,
    runtime: Arc<SharedMvccRuntime>,
    tx_id: TxID,
    database_id: usize,
    request: SharedIdRequest,
    owner: Owner,
    refresh: Option<SharedTransactionCommit>,
    operation: Option<IdOperation>,
}
impl Debug for SharedIdState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedIdState")
            .field("pending", &self.pending.is_some())
            .finish()
    }
}
impl SharedIdState {
    pub(crate) fn reset(&mut self) {
        self.pending = None;
    }
}

impl<C: LogicalClock + 'static, A: ConcurrentAllocator + 'static> MvStore<C, A> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn shared_id_nonblock(
        self: &Arc<Self>,
        connection: &Arc<Connection>,
        pager: Arc<Pager>,
        tx_id: TxID,
        database_id: usize,
        request: SharedIdRequest,
        state: &mut SharedIdState,
    ) -> IOResultOr<SharedIdResult> {
        let result = (|| {
            let runtime = self.shared_runtime.get().ok_or_else(|| {
                LimboError::InvalidArgument("ID reservation requires shared MVCC".into())
            })?;
            if database_id != 0 || !Arc::ptr_eq(&pager, &connection.get_pager()) {
                return Err(LimboError::InvalidArgument(
                    "shared ID oracle requires the main database pager".into(),
                )
                .into());
            }
            if !connection.db.get_mv_store().as_ref().is_some_and(|store| {
                Arc::as_ptr(store).cast::<()>() == Arc::as_ptr(self).cast::<()>()
            }) {
                return Err(LimboError::InvalidArgument(
                    "shared ID store differs from connection".into(),
                )
                .into());
            }
            if connection.is_readonly(database_id) {
                return Err(LimboError::ReadOnly.into());
            }
            let owner = {
                let tx = self.txs.get(&tx_id).ok_or_else(|| {
                    LimboError::InvalidArgument("shared ID requester is no longer active".into())
                })?;
                tx.value().ensure_peer_unconflicted()?;
                if tx.value().state.load() != TransactionState::Active {
                    return Err(LimboError::InvalidArgument(
                        "shared ID requester is no longer active".into(),
                    )
                    .into());
                }
                let registration = tx
                    .value()
                    .shared_snapshot
                    .as_ref()
                    .ok_or_else(|| {
                        LimboError::InvalidArgument("ID requester has no shared snapshot".into())
                    })?
                    .lock();
                let registration = registration.as_ref().ok_or_else(|| {
                    LimboError::InvalidArgument("shared ID requester is no longer active".into())
                })?;
                Owner {
                    epoch: registration.epoch,
                    begin: registration.begin_timestamp,
                }
            };
            if let Some(pending) = &state.pending {
                if !pending
                    .connection
                    .upgrade()
                    .is_some_and(|saved| Arc::ptr_eq(&saved, connection))
                    || !Arc::ptr_eq(&pending.pager, &pager)
                    || !Arc::ptr_eq(&pending.runtime, runtime)
                    || pending.tx_id != tx_id
                    || pending.database_id != database_id
                    || pending.request != request
                    || pending.owner != owner
                {
                    return Err(LimboError::InvalidArgument(
                        "shared ID request changed across I/O".into(),
                    )
                    .into());
                }
            } else {
                state.pending = Some(PendingId {
                    connection: Arc::downgrade(connection),
                    pager,
                    runtime: runtime.clone(),
                    tx_id,
                    database_id,
                    request,
                    owner,
                    refresh: Some(SharedTransactionCommit::new(runtime.clone())?),
                    operation: None,
                });
            }
            let pending = state.pending.as_mut().unwrap();
            if pending.operation.is_none() {
                let refresh = pending.refresh.as_mut().unwrap();
                return_if_io!(refresh.poll_refresh(self, connection, false));
                let tx = self.txs.get(&tx_id).ok_or_else(|| {
                    LimboError::InvalidArgument("shared ID requester is no longer active".into())
                })?;
                tx.value().ensure_peer_unconflicted()?;
                if tx.value().state.load() != TransactionState::Active {
                    return Err(LimboError::InvalidArgument(
                        "shared ID requester is no longer active".into(),
                    )
                    .into());
                }
                drop(tx);
                let replay = runtime.replay.lock().clone().ok_or(LimboError::Busy)?;
                let identity = replay.identity()?;
                let loaded = refresh.loaded.take().unwrap();
                if loaded.record().prefix() != identity.prefix
                    || loaded.record().log_generation() != identity.database.generation
                {
                    return Err(
                        LimboError::InvalidArgument("ID refresh frontier differs".into()).into(),
                    );
                }
                let domain = |table, sequence| Domain {
                    generation: identity.database.generation,
                    table,
                    sequence,
                };
                let action = match request {
                    SharedIdRequest::ReserveRow { table_id } => IdAction::ReserveRow {
                        owner,
                        domain: domain(table_id, false),
                        autoincrement: false,
                    },
                    SharedIdRequest::ClaimExplicitRow { table_id, value } => {
                        IdAction::ClaimExplicitRow {
                            owner,
                            domain: domain(table_id, false),
                            value,
                        }
                    }
                    SharedIdRequest::ReserveLogicalTable => IdAction::ReserveLogicalTable { owner },
                    SharedIdRequest::ClaimSequence {
                        table_id,
                        value,
                        autonomous,
                    } => IdAction::ClaimSequence {
                        owner,
                        domain: domain(table_id, true),
                        value,
                        durability: if autonomous {
                            SequenceDurability::AutonomousInner
                        } else {
                            SequenceDurability::OuterTransaction
                        },
                    },
                };
                pending.operation = Some(runtime.ids.start(
                    loaded.into_guard()?,
                    identity,
                    Box::new(SharedIdOracle::new(
                        self.clone(),
                        connection.clone(),
                        identity,
                    )),
                    connection.db.io.clone(),
                    action,
                )?);
                pending.refresh = None;
            }
            let outcome = return_if_io!(pending.operation.as_mut().unwrap().poll());
            let value = match (request, outcome.result) {
                (
                    SharedIdRequest::ReserveRow { .. } | SharedIdRequest::ReserveLogicalTable,
                    IdResult::Reserved { value, .. },
                ) => SharedIdResult::Value(value),
                (
                    SharedIdRequest::ClaimExplicitRow { .. }
                    | SharedIdRequest::ClaimSequence { .. },
                    IdResult::Reserved { .. },
                ) => SharedIdResult::Claimed,
                _ => {
                    return Err(
                        LimboError::InvalidArgument("unexpected shared ID outcome".into()).into(),
                    )
                }
            };
            outcome.guard.finish()?;
            Ok(IOResult::Done(value))
        })();
        if !matches!(&result, Ok(IOResult::IO(_))) {
            state.reset();
        }
        result
    }
}
