//! Reservation decisions under one recovered publication authority.
use super::io_protocol::{IdAppend, IdLoad, LoadedIds, RecoveryIdentity, SharedIds};
use super::journal::{Change, Claim, ClaimKey, Domain, Owner};
use super::*;
use crate::mvcc::shared_authority::AuthorityGuard;
use crate::sync::Arc;
use crate::types::IOCompletions;
use crate::{IOResult, IO};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mvcc) struct ReservationId {
    pub owner: Owner,
    pub ordinal: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mvcc) struct SavepointMark {
    pub owner: Owner,
    pub next_ordinal: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mvcc) enum SequenceDurability {
    OuterTransaction,
    AutonomousInner,
}
#[derive(Clone, Copy, Debug)]
pub(in crate::mvcc) enum IdAction {
    ReserveRow {
        owner: Owner,
        domain: Domain,
        autoincrement: bool,
    },
    ClaimExplicitRow {
        owner: Owner,
        domain: Domain,
        value: i64,
    },
    ClaimSequence {
        owner: Owner,
        domain: Domain,
        value: i64,
        durability: SequenceDurability,
    },
    ReserveLogicalTable {
        owner: Owner,
    },
    Savepoint {
        owner: Owner,
    },
    ReleaseAfter {
        mark: SavepointMark,
    },
    ReleaseOwner {
        owner: Owner,
    },
    /// Numeric minimum of live claims, not a complete or chronological scan boundary.
    LowestLiveSequenceClaim {
        domain: Domain,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mvcc) enum IdQuery {
    RowFacts { domain: Domain, autoincrement: bool },
    RowExists { domain: Domain, value: i64 },
    LogicalFloor,
    SequenceState { domain: Domain, descending: bool },
}
#[derive(Clone, Copy, Debug)]
pub(in crate::mvcc) enum IdFact {
    RowFacts {
        maximum: Option<i64>,
        autoincrement_floor: i64,
    },
    RowExists(bool),
    LogicalFloor(i64),
    SequenceState(Option<(i64, bool)>),
}
pub(in crate::mvcc) trait CommittedIdOracle: Send {
    fn identity(&self) -> RecoveryIdentity;
    /// Reads must retain the supplied authority lease through backend completion retirement.
    fn poll(&mut self, guard: &AuthorityGuard, query: &IdQuery) -> Result<IOResult<IdFact>>;
}
#[derive(Debug, PartialEq, Eq)]
pub(in crate::mvcc) enum IdResult {
    Reserved { id: ReservationId, value: i64 },
    Marked(SavepointMark),
    Released,
    LowestClaim(Option<i64>),
}
pub(in crate::mvcc) struct IdOutcome {
    pub guard: AuthorityGuard,
    pub result: IdResult,
    /// Physical ID metadata bytes sampled while this returned guard remains held.
    pub retained_bytes: u64,
    /// Physical metadata plus reserved replacement-bank capacity, excluding the logical log.
    pub required_budget_bytes: u64,
}
impl SharedIds {
    pub(in crate::mvcc) fn start(
        self: &Arc<Self>,
        guard: AuthorityGuard,
        identity: RecoveryIdentity,
        oracle: Box<dyn CommittedIdOracle>,
        io: Arc<dyn IO>,
        action: IdAction,
    ) -> Result<IdOperation> {
        validate_action(action, identity.database.generation)?;
        if oracle.identity() != identity {
            return Err(invalid(
                "ID oracle does not match recovered generation and prefix",
            ));
        }
        Ok(IdOperation {
            stage: Stage::Loading(self.load(guard, identity)?),
            oracle,
            io,
            action,
            identity,
        })
    }
}

pub(in crate::mvcc) struct IdOperation {
    stage: Stage,
    oracle: Box<dyn CommittedIdOracle>,
    io: Arc<dyn IO>,
    action: IdAction,
    identity: RecoveryIdentity,
}
enum Stage {
    Loading(IdLoad),
    Evaluating(LoadedIds),
    Querying(LoadedIds, QueryState),
    Appending(IdAppend, Option<IdResult>),
    Retiring(LoadedIds, IdResult),
    Finished,
}
#[derive(Clone, Copy)]
enum QueryState {
    Rows {
        owner: Owner,
        domain: Domain,
        autoincrement: bool,
    },
    Exists {
        owner: Owner,
        domain: Domain,
        candidate: i64,
        attempts: u8,
    },
    Logical {
        owner: Owner,
    },
}
enum Decision {
    Return(IdResult),
    Query(QueryState),
    Append(Change, IdResult),
}
impl IdOperation {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<IdOutcome>> {
        loop {
            match std::mem::replace(&mut self.stage, Stage::Finished) {
                Stage::Loading(mut load) => match load.poll()? {
                    IOResult::IO(completions) => {
                        self.stage = Stage::Loading(load);
                        return Ok(IOResult::IO(completions));
                    }
                    IOResult::Done(loaded) => self.stage = Stage::Evaluating(loaded),
                },
                Stage::Evaluating(loaded) => {
                    self.check_identity(&loaded)?;
                    let live = loaded.guard.live_registrations()?;
                    if let Some(owner) = action_owner(self.action) {
                        if !matches!(self.action, IdAction::ReleaseOwner { .. })
                            && !live.contains(&(owner.epoch, owner.begin))
                        {
                            return Err(invalid("ID owner has no live snapshot registration"));
                        }
                    }
                    if let IdAction::ClaimExplicitRow {
                        owner,
                        domain,
                        value,
                    }
                    | IdAction::ClaimSequence {
                        owner,
                        domain,
                        value,
                        ..
                    } = self.action
                    {
                        if let Some(result) = existing_claim(
                            &loaded,
                            owner,
                            Claim {
                                key: ClaimKey::Domain(domain),
                                value,
                            },
                        )? {
                            self.stage = Stage::Retiring(loaded, result);
                            continue;
                        }
                    }
                    if matches!(
                        self.action,
                        IdAction::ReserveRow { .. }
                            | IdAction::ClaimExplicitRow { .. }
                            | IdAction::ClaimSequence { .. }
                            | IdAction::ReserveLogicalTable { .. }
                    ) {
                        let dead = loaded
                            .state
                            .active_owners()
                            .filter(|owner| !live.contains(&(owner.epoch, owner.begin)))
                            .min_by_key(|owner| (owner.epoch, owner.begin));
                        if let Some(owner) = dead {
                            self.stage = Stage::Appending(
                                loaded.append(Change::ReleaseOwner { owner })?,
                                None,
                            );
                            continue;
                        }
                    }
                    let decision = evaluate(&loaded, self.action, &live)?;
                    self.stage = decide(loaded, decision)?;
                }
                Stage::Querying(loaded, query) => {
                    self.check_identity(&loaded)?;
                    let request = match query {
                        QueryState::Rows {
                            domain,
                            autoincrement,
                            ..
                        } => IdQuery::RowFacts {
                            domain,
                            autoincrement,
                        },
                        QueryState::Exists {
                            domain, candidate, ..
                        } => IdQuery::RowExists {
                            domain,
                            value: candidate,
                        },
                        QueryState::Logical { .. } => IdQuery::LogicalFloor,
                    };
                    match self.oracle.poll(&loaded.guard, &request)? {
                        IOResult::IO(completions) => {
                            self.stage = Stage::Querying(loaded, query);
                            return Ok(IOResult::IO(completions));
                        }
                        IOResult::Done(fact) => {
                            self.check_identity(&loaded)?;
                            let decision = self.fact(&loaded, query, fact)?;
                            self.stage = decide(loaded, decision)?;
                        }
                    }
                }
                Stage::Appending(mut append, result) => match append.poll()? {
                    IOResult::IO(completions) => {
                        self.stage = Stage::Appending(append, result);
                        return Ok(IOResult::IO(completions));
                    }
                    IOResult::Done(loaded) => {
                        self.stage = match result {
                            Some(result) => Stage::Retiring(loaded, result),
                            None => Stage::Evaluating(loaded),
                        };
                    }
                },
                Stage::Retiring(loaded, result) => {
                    if !loaded.guard.io_retired() {
                        let completion = loaded.guard.retirement_completion();
                        self.stage = Stage::Retiring(loaded, result);
                        return Ok(IOResult::IO(IOCompletions(completion)));
                    }
                    let retained_bytes = loaded.retained_bytes()?;
                    let required_budget_bytes = loaded.required_budget_bytes()?;
                    return Ok(IOResult::Done(IdOutcome {
                        guard: loaded.guard,
                        result,
                        retained_bytes,
                        required_budget_bytes,
                    }));
                }
                Stage::Finished => return Err(invalid("ID operation already terminated")),
            }
        }
    }

    fn check_identity(&self, loaded: &LoadedIds) -> Result<()> {
        if self.oracle.identity() != self.identity
            || loaded.identity != self.identity
            || loaded.guard.published()? != self.identity.prefix
        {
            return Err(invalid(
                "ID oracle changed its recovered generation or prefix",
            ));
        }
        Ok(())
    }

    fn fact(&self, loaded: &LoadedIds, query: QueryState, fact: IdFact) -> Result<Decision> {
        match (query, fact) {
            (
                QueryState::Rows {
                    owner,
                    domain,
                    autoincrement,
                },
                IdFact::RowFacts {
                    maximum,
                    autoincrement_floor,
                },
            ) => {
                let maximum = maximum
                    .into_iter()
                    .chain(loaded.state.maximum_claimed_row(domain))
                    .max();
                let base = if autoincrement {
                    if autoincrement_floor < 0 {
                        return Err(invalid("negative AUTOINCREMENT floor"));
                    }
                    maximum.unwrap_or(0).max(autoincrement_floor)
                } else {
                    maximum.unwrap_or(0)
                };
                if let Some(value) = base.checked_add(1) {
                    claim(
                        loaded,
                        owner,
                        Claim {
                            key: ClaimKey::Domain(domain),
                            value,
                        },
                    )
                } else if autoincrement {
                    Err(LimboError::DatabaseFull)
                } else {
                    self.random_candidate(owner, domain, 0)
                }
            }
            (
                QueryState::Exists {
                    owner,
                    domain,
                    candidate,
                    attempts,
                },
                IdFact::RowExists(exists),
            ) => {
                if exists || loaded.state.row_owner(domain, candidate).is_some() {
                    self.random_candidate(owner, domain, attempts + 1)
                } else {
                    claim(
                        loaded,
                        owner,
                        Claim {
                            key: ClaimKey::Domain(domain),
                            value: candidate,
                        },
                    )
                }
            }
            (QueryState::Logical { owner }, IdFact::LogicalFloor(floor)) => {
                if floor >= 0 || floor == i64::MIN {
                    return Err(invalid("invalid committed logical ID floor"));
                }
                let value = loaded
                    .state
                    .logical_floor
                    .min(floor)
                    .checked_sub(1)
                    .filter(|value| *value != i64::MIN)
                    .ok_or(LimboError::DatabaseFull)?;
                claim(
                    loaded,
                    owner,
                    Claim {
                        key: ClaimKey::LogicalTable,
                        value,
                    },
                )
            }
            _ => Err(invalid("ID oracle returned a fact for another query")),
        }
    }

    fn random_candidate(&self, owner: Owner, domain: Domain, attempts: u8) -> Result<Decision> {
        if attempts >= 100 {
            return Err(LimboError::DatabaseFull);
        }
        let candidate = (self.io.generate_random_number() & (i64::MAX >> 1)) + 1;
        Ok(Decision::Query(QueryState::Exists {
            owner,
            domain,
            candidate,
            attempts,
        }))
    }
}

fn decide(loaded: LoadedIds, decision: Decision) -> Result<Stage> {
    Ok(match decision {
        Decision::Return(result) => Stage::Retiring(loaded, result),
        Decision::Query(query) => Stage::Querying(loaded, query),
        Decision::Append(change, result) => Stage::Appending(loaded.append(change)?, Some(result)),
    })
}

fn evaluate(loaded: &LoadedIds, action: IdAction, live: &[(u64, u64)]) -> Result<Decision> {
    Ok(match action {
        IdAction::ReserveRow {
            owner,
            domain,
            autoincrement,
        } => {
            loaded.state.next_ordinal(owner)?;
            Decision::Query(QueryState::Rows {
                owner,
                domain,
                autoincrement,
            })
        }
        IdAction::ReserveLogicalTable { owner } => {
            loaded.state.next_ordinal(owner)?;
            Decision::Query(QueryState::Logical { owner })
        }
        IdAction::ClaimExplicitRow {
            owner,
            domain,
            value,
        } => {
            if loaded
                .state
                .row_owner(domain, value)
                .is_some_and(|other| other != owner)
            {
                return Err(LimboError::Busy);
            }
            claim(
                loaded,
                owner,
                Claim {
                    key: ClaimKey::Domain(domain),
                    value,
                },
            )?
        }
        IdAction::ClaimSequence {
            owner,
            domain,
            value,
            ..
        } => claim(
            loaded,
            owner,
            Claim {
                key: ClaimKey::Domain(domain),
                value,
            },
        )?,
        IdAction::Savepoint { owner } => Decision::Return(IdResult::Marked(SavepointMark {
            owner,
            next_ordinal: loaded.state.next_ordinal(owner)?,
        })),
        IdAction::ReleaseAfter { mark } => {
            if mark.next_ordinal > loaded.state.next_ordinal(mark.owner)? {
                return Err(invalid("ID savepoint mark is newer than its owner"));
            }
            if loaded
                .state
                .claims(mark.owner)
                .any(|(ordinal, _)| ordinal >= mark.next_ordinal)
            {
                Decision::Append(
                    Change::ReleaseAfter {
                        owner: mark.owner,
                        next_ordinal: mark.next_ordinal,
                    },
                    IdResult::Released,
                )
            } else {
                Decision::Return(IdResult::Released)
            }
        }
        IdAction::ReleaseOwner { owner } => {
            if loaded.state.active_owners().any(|active| active == owner) {
                Decision::Append(Change::ReleaseOwner { owner }, IdResult::Released)
            } else {
                Decision::Return(IdResult::Released)
            }
        }
        IdAction::LowestLiveSequenceClaim { domain } => {
            let watermark = loaded
                .state
                .active_owners()
                .filter(|owner| live.contains(&(owner.epoch, owner.begin)))
                .flat_map(|owner| loaded.state.claims(owner))
                .filter_map(|(_, claim)| {
                    (claim.key == ClaimKey::Domain(domain)).then_some(claim.value)
                })
                .min();
            Decision::Return(IdResult::LowestClaim(watermark))
        }
    })
}

fn existing_claim(loaded: &LoadedIds, owner: Owner, claim: Claim) -> Result<Option<IdResult>> {
    Ok(loaded
        .state
        .matching_claim(owner, claim)?
        .map(|ordinal| IdResult::Reserved {
            id: ReservationId { owner, ordinal },
            value: claim.value,
        }))
}

fn claim(loaded: &LoadedIds, owner: Owner, claim: Claim) -> Result<Decision> {
    if let Some(result) = existing_claim(loaded, owner, claim)? {
        return Ok(Decision::Return(result));
    }
    let ordinal = loaded.state.next_ordinal(owner)?;
    Ok(Decision::Append(
        Change::Claim {
            owner,
            ordinal,
            claim,
        },
        IdResult::Reserved {
            id: ReservationId { owner, ordinal },
            value: claim.value,
        },
    ))
}

fn action_owner(action: IdAction) -> Option<Owner> {
    match action {
        IdAction::ReserveRow { owner, .. }
        | IdAction::ClaimExplicitRow { owner, .. }
        | IdAction::ClaimSequence { owner, .. }
        | IdAction::ReserveLogicalTable { owner }
        | IdAction::Savepoint { owner }
        | IdAction::ReleaseOwner { owner } => Some(owner),
        IdAction::ReleaseAfter { mark } => Some(mark.owner),
        IdAction::LowestLiveSequenceClaim { .. } => None,
    }
}

fn validate_action(action: IdAction, generation: u64) -> Result<()> {
    if let Some(owner) = action_owner(action) {
        if owner.epoch == 0
            || owner.begin == 0
            || owner.begin >= super::super::shared_authority::MAX_TIMESTAMP
        {
            return Err(invalid("invalid ID reservation owner"));
        }
    }
    let domain = match action {
        IdAction::ReserveRow { domain, .. } | IdAction::ClaimExplicitRow { domain, .. } => {
            Some((domain, false))
        }
        IdAction::ClaimSequence { domain, .. } | IdAction::LowestLiveSequenceClaim { domain } => {
            Some((domain, true))
        }
        _ => None,
    };
    if let Some((domain, sequence)) = domain {
        if domain.generation != generation
            || domain.table >= 0
            || domain.table == i64::MIN
            || domain.sequence != sequence
        {
            return Err(invalid("ID action belongs to another domain or generation"));
        }
    }
    if matches!(action, IdAction::ReleaseAfter { mark } if mark.next_ordinal == 0) {
        return Err(invalid("invalid ID savepoint ordinal"));
    }
    Ok(())
}

fn invalid(message: &str) -> LimboError {
    LimboError::InvalidArgument(message.into())
}
