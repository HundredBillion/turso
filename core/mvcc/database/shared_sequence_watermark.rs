use super::*;
use crate::mvcc::database::shared_id_oracle::SharedIdOracle;
use crate::mvcc::shared_ids::journal::{Domain, Owner};
use crate::mvcc::shared_ids::policy::{
    CommittedIdOracle, IdAction, IdFact, IdOperation, IdQuery, IdResult,
};

#[derive(Default)]
pub(crate) struct SharedSequenceWatermarkState {
    pending: Option<PendingWatermark>,
}
struct PendingWatermark {
    connection: Weak<Connection>,
    runtime: Arc<SharedMvccRuntime>,
    root: i64,
    sequence: Arc<Sequence>,
    refresh: SharedTransactionCommit,
    oracle: Option<Box<dyn CommittedIdOracle>>,
    domain: Option<Domain>,
    boundary: Option<i64>,
    operation: Option<IdOperation>,
}
impl Debug for SharedSequenceWatermarkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSequenceWatermarkState")
            .field("pending", &self.pending.is_some())
            .finish()
    }
}
impl SharedSequenceWatermarkState {
    pub(crate) fn reset(&mut self) {
        self.pending = None;
    }
}
impl<C: LogicalClock + 'static, A: ConcurrentAllocator + 'static> MvStore<C, A> {
    pub(crate) fn shared_sequence_watermark_nonblock(
        self: &Arc<Self>,
        connection: &Arc<Connection>,
        database_id: usize,
        root: i64,
        sequence: Arc<Sequence>,
        state: &mut SharedSequenceWatermarkState,
    ) -> IOResultOr<Option<i64>> {
        let result = (|| {
            let runtime = self.shared_runtime.get().ok_or_else(|| {
                LimboError::InvalidArgument("watermark requires shared MVCC".into())
            })?;
            if database_id != 0
                || !connection.db.get_mv_store().as_ref().is_some_and(|store| {
                    Arc::as_ptr(store).cast::<()>() == Arc::as_ptr(self).cast::<()>()
                })
            {
                return Err(LimboError::InvalidArgument(
                    "shared watermark requires the main database store".into(),
                )
                .into());
            }
            if let Some(pending) = &state.pending {
                if !pending
                    .connection
                    .upgrade()
                    .is_some_and(|saved| Arc::ptr_eq(&saved, connection))
                    || !Arc::ptr_eq(&pending.runtime, runtime)
                    || pending.root != root
                    || pending.sequence.name != sequence.name
                    || pending.sequence.start_value != sequence.start_value
                    || pending.sequence.increment_by != sequence.increment_by
                    || pending.sequence.min_value != sequence.min_value
                    || pending.sequence.max_value != sequence.max_value
                    || pending.sequence.cycle != sequence.cycle
                {
                    return Err(LimboError::InvalidArgument(
                        "watermark request changed across I/O".into(),
                    )
                    .into());
                }
            } else {
                state.pending = Some(PendingWatermark {
                    connection: Arc::downgrade(connection),
                    runtime: runtime.clone(),
                    root,
                    sequence,
                    refresh: SharedTransactionCommit::new(runtime.clone())?,
                    oracle: None,
                    domain: None,
                    boundary: None,
                    operation: None,
                });
            }
            let pending = state.pending.as_mut().unwrap();
            if pending.operation.is_none() {
                return_if_io!(pending.refresh.poll_refresh(self, connection, false));
                let identity = runtime
                    .replay
                    .lock()
                    .as_ref()
                    .ok_or(LimboError::Busy)?
                    .identity()?;
                let loaded = pending.refresh.loaded.as_ref().unwrap();
                if loaded.record().prefix() != identity.prefix
                    || loaded.record().log_generation() != identity.database.generation
                {
                    return Err(LimboError::InvalidArgument(
                        "watermark refresh frontier differs".into(),
                    )
                    .into());
                }
                if pending.oracle.is_none() {
                    let table = self
                        .try_get_table_id_from_root_page_at(
                            root,
                            identity.prefix.committed_timestamp.saturating_add(1),
                        )
                        .ok_or(LimboError::SchemaUpdated)?;
                    pending.domain = Some(Domain {
                        generation: identity.database.generation,
                        table: table.into(),
                        sequence: true,
                    });
                    pending.oracle = Some(Box::new(SharedIdOracle::new(
                        self.clone(),
                        connection.clone(),
                        identity,
                    )));
                }
                let domain = pending.domain.unwrap();
                let fact = return_if_io!(pending.oracle.as_mut().unwrap().poll(
                    loaded.guard()?,
                    &IdQuery::SequenceState {
                        domain,
                        descending: pending.sequence.increment_by < 0
                    }
                ));
                let IdFact::SequenceState { committed, row } = fact else {
                    return Err(LimboError::InternalError(
                        "watermark oracle returned another fact".into(),
                    )
                    .into());
                };
                let provisional_owner = if committed {
                    None
                } else {
                    let tx_id = connection
                        .get_mv_tx_id_for_db(0)
                        .ok_or(LimboError::SchemaUpdated)?;
                    let tx = self.txs.get(&tx_id).ok_or(LimboError::SchemaUpdated)?;
                    tx.value().ensure_peer_unconflicted()?;
                    if tx.value().state.load() != TransactionState::Active
                        || !tx
                            .value()
                            .created_table_ids
                            .lock()
                            .iter()
                            .any(|table| i64::from(*table) == domain.table)
                    {
                        return Err(LimboError::SchemaUpdated.into());
                    }
                    let registration = tx
                        .value()
                        .shared_snapshot
                        .as_ref()
                        .ok_or(LimboError::SchemaUpdated)?
                        .lock();
                    let registration = registration.as_ref().ok_or(LimboError::SchemaUpdated)?;
                    Some(Owner {
                        epoch: registration.epoch,
                        begin: registration.begin_timestamp,
                    })
                };
                pending.boundary = row.map(|(value, called)| {
                    first_unsafe_sequence_watermark(&pending.sequence, value, called)
                });
                let loaded = pending.refresh.loaded.take().unwrap();
                pending.operation = Some(runtime.ids.start(
                    loaded.into_guard()?,
                    identity,
                    pending.oracle.take().unwrap(),
                    connection.db.io.clone(),
                    IdAction::LowestLiveSequenceClaim {
                        domain,
                        provisional_owner,
                    },
                )?);
            }
            let outcome = return_if_io!(pending.operation.as_mut().unwrap().poll());
            let IdResult::LowestClaim(claim) = outcome.result else {
                return Err(LimboError::InternalError(
                    "watermark journal returned another result".into(),
                )
                .into());
            };
            let watermark = match (pending.boundary, claim) {
                (Some(boundary), Some(claim)) => Some(boundary.min(claim)),
                (boundary, None) => boundary,
                (None, claim) => claim,
            };
            outcome.guard.finish()?;
            Ok(IOResult::Done(watermark))
        })();
        if !matches!(result, Ok(IOResult::IO(_))) {
            state.reset();
        }
        result
    }
}
