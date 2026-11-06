# Shared ID Reservations Technical Spec

> **For agentic workers:** Use `dmi-superpowers:executing-plans` to implement this TSP task by task. The current deliverable is the design/interface proposal; implementation starts after the engine owner and root review it.

**Goal:** Reserve automatic row IDs, sequence values and internal negative table IDs across overlapping native PHP workers without collisions, stale sequence watermarks, or reuse of retired logical identities.

**Architecture:** A private `shared_ids` component maintains a dynamic reservation journal in two stable data-bank inodes plus a small stable selector inode. All reads, reconciliation, reservation changes and bank selection run under the existing publication authority. Durable committed database state supplies row/sequence facts; journal records retain live transaction claims and the never-reused negative logical-ID floor.

**Tech Stack:** Existing Rust core I/O, `GuardedFile` authority leases, CRC codec patterns, native Linux OFD snapshot ownership and the approved ext-family storage policy. No new dependency or public API.

## Global constraints

- Do not change `database/mod.rs`, `execute.rs`, authority/manifest formats or public URI behavior in this component's initial implementation.
- The integration owner implements engine/VM hooks and checkpoint generation mapping. The I/O lease owner controls `shared_authority.rs` until that work is integrated.
- Existing marked databases never initialize missing reservation state implicitly. Add all three new sidecars to offline initialization and strict open together before enabling this protocol revision.
- No global row counter and no fixed table-count array. Storage and memory grow with live domains/reservations, subject to explicit resource errors and the configured history budget.
- Snapshot identity is `(epoch, begin_timestamp)` from a real retained registration, not a PID. Authority ownership and current recovered manifest state are prerequisites.
- Ordinary row maximum, AUTOINCREMENT committed floor, sequence state and logical table-ID floor are distinct concepts.
- Runtime operations yield on I/O. No synchronous filesystem calls or polling sleeps in allocation paths. Outstanding mutation I/O retains publication ownership until actual backend retirement.
- This component does not establish end-to-end PHP concurrency acceptance by itself.

## Storage decision

Use `ids-a`, `ids-b` and `ids-selector` inside the existing identity namespace. All three names refer to permanently retained files; compaction never renames, replaces or unlinks an inode. The strict runtime opener opens all three without `Create` and checks their native identities/capabilities. A missing file fails closed even when it would have been the inactive bank.

Two data files keep compaction bounded without growing fixed-address mmap regions or accumulating holes at ever-increasing file offsets. A fixed authority-header table array imposes an unrelated permanent table limit. Appending reservations to the user logical log would instead require an additional durable metadata-frame protocol in the user commit log; this design keeps those commit semantics separate.

Each data bank has an immutable checksummed identity/generation header followed by
page-aligned immutable snapshot/journal records. `ids-selector` contains two
separately addressed 4096-byte slots. Each selector record carries magic/version,
database identity, selector sequence and an active `BankRef`, plus an optional
prepared successor `BankRef`. A reference contains bank number/generation,
database/log generation, checkpoint timestamp, selected record end, stream CRC and
compacted snapshot boundary. Encoding and arithmetic are checked and bounded.

An append writes and synchronizes data first, then writes/synchronizes selector
slot A, then writes/synchronizes the same new selector in slot B. Only after both
copies are durably synchronized may the operation acknowledge its reservation.
An incomplete update always leaves a previous acknowledged selector or a new
selector pointing to already-synchronized data. A lost acknowledgement may leave a
conservative extra reservation. A damaged selector copy after acknowledgement
cannot erase that acknowledged tail because the matching other copy retains it.

Reopen decodes both selector slots and selects the highest valid sequence; equal
sequences with different contents are corruption. Both slots invalid fail closed.
It then validates the referenced bank and the entire selected prefix. If that
validation fails, it must not try a lower selector sequence or another complete
bank. Data-bank headers never independently elect an active bank. Thus corruption
of the header in a previously active data bank cannot be mistaken for an
incomplete inactive replacement. Bytes beyond the selected tail were not
acknowledged and may be truncated under authority; selected-prefix corruption
never authorizes truncation or rollback to an older tail.

Before reusing/truncating an old bank, both selector slots must durably select its
successor and no longer reference the old bank. A canceled operation that left
only one updated slot must finish mirroring the selector before bank reuse. A
partially built inactive bank does not change the selector at all.

For a cross-database-generation remap, first mirror-sync a selector containing the
old active reference and the complete prepared successor reference. Return that
durable preparation proof to the checkpoint coordinator before its generation
publication. Ordinary allocation uses only the reference matching the fully
recovered manifest/checkpoint identity; an uncommitted prepared generation is
ineligible. After the database generation becomes durable, mirror-sync a selector
making the successor active and clearing the old reference before reusing the old
bank. Aborting an unpublished prepared remap similarly mirror-syncs its removal
before overwriting that bank. This keeps the checkpoint recovery evidence explicit
without inferring committed state from a higher data-bank generation.

An idle process rechecks bank generation and selected tail under authority before every operation. Retaining an inode is not permission to keep using a cached active-bank choice.

## Domain and transaction identity

`IdDomain` contains `(database_generation, logical_table_id, kind)`, where `kind` distinguishes table row IDs and a sequence backing table. Names are not identities: DROP/CREATE of the same name must not inherit reservations. The current engine reconstructs a checkpointed table ID as the negative physical root number, so a bare table ID is insufficient across generations.

At a quiescent checkpoint the coordinator supplies a complete old-to-new mapping for every live table and sequence domain. Targets must be unique; dropped domains are explicitly retired. The replacement bank snapshot is bound to the target database generation and checkpoint boundary. It cannot serve old-generation callers merely because its allocator bank generation is higher. The recovered manifest/checkpoint transition selects when that remap becomes usable. Old idle handles must observe the new generation before another allocation.

The global logical-ID floor is independent of these per-generation domains. It is the numerically lowest consumed negative ID, moves only downward, and includes successful reservations even when their transaction later rolls back or dies. The next ID is below both this durable floor and all logical/physical-root-derived IDs reported by checkpoint reconciliation. `i64::MIN` exhaustion returns `DatabaseFull`; never wrap, reuse an ID, or replace a previously established floor with a higher one.

A reservation owner is the registration's epoch and globally assigned begin timestamp. A monotonically increasing per-owner ordinal identifies each reservation and savepoint boundary. Rolling back a savepoint releases claims after its mark but never rewinds the ordinal; later reservations cannot accidentally reuse an old release token.

## Narrow interface proposal

All types below are private to `mvcc`. `RecoveryIdentity` comes from the manifest/checkpoint coordinator, and `IdOwner` is obtained from a retained `SnapshotRegistration` belonging to that authority. A proposed narrow authority accessor returns live `(epoch, begin_timestamp)` registrations after kernel liveness checks; it does not expose PID-based guesses or clear recovery state.

```rust
struct IdOwner { epoch: u64, begin_timestamp: u64 }
struct ReservationId { owner: IdOwner, ordinal: u64 }
struct SavepointMark { owner: IdOwner, next_ordinal: u64 }
enum DomainKind { Row, Sequence }
enum SequenceDurability { OuterTransaction, AutonomousInner }
struct IdDomain { database_generation: u64, table_id: MVTableId, kind: DomainKind }
struct RecoveryIdentity {
    database_generation: u64,
    checkpoint_timestamp: u64,
    prefix: PublishedPrefix,
}
enum IdAction {
    ReserveRow { owner: IdOwner, domain: IdDomain, autoincrement: bool },
    ClaimExplicitRow { owner: IdOwner, domain: IdDomain, value: i64 },
    ClaimSequence { owner: IdOwner, domain: IdDomain, value: i64, durability: SequenceDurability },
    ReserveLogicalTable { owner: IdOwner },
    Savepoint { owner: IdOwner },
    ReleaseAfter { mark: SavepointMark },
    ReleaseOwner { owner: IdOwner },
    LowestLiveSequenceClaim { domain: IdDomain },
}
enum IdQuery {
    RowFacts { domain: IdDomain },
    RowExists { domain: IdDomain, value: i64 },
    LogicalFloor,
}
enum IdFact {
    RowFacts { maximum: Option<i64>, autoincrement_floor: i64 },
    RowExists(bool),
    LogicalFloor(i64),
}
trait CommittedIdOracle: Send {
    fn identity(&self) -> RecoveryIdentity;
    fn poll(&mut self, guard: &AuthorityGuard, query: &IdQuery) -> Result<IOResult<IdFact>>;
}
impl SharedIds {
    fn open_existing(
        banks: [Arc<GuardedFile>; 2], selector: Arc<GuardedFile>,
        database_id: FileId, history_budget: u64,
    ) -> Result<Arc<Self>>;
    fn start(
        self: &Arc<Self>, guard: AuthorityGuard, identity: RecoveryIdentity,
        oracle: Box<dyn CommittedIdOracle>, io: Arc<dyn IO>, action: IdAction,
    ) -> Result<IdOperation>;
}
impl IdOperation {
    fn poll(&mut self) -> Result<IOResult<IdOutcome>>;
}
struct IdOutcome { guard: AuthorityGuard, result: IdResult }
enum IdResult {
    Reserved { id: ReservationId, value: i64 },
    Marked(SavepointMark),
    Released,
    LowestClaim(Option<i64>),
}
```

`ClaimSequence` records a value already allocated and descriptor-validated by the existing
engine sequence transaction, before its commit can publish. It does not derive a cursor from
unordered outstanding claims: a committed value plus old outstanding values cannot distinguish
successive CYCLE wraps. The existing transaction and row-conflict logic retains responsibility
for ascending/descending increments, min/max limits and CYCLE behavior. `SequenceDurability`
comes from that execution path: an exclusive outer transaction (including ordinary `BEGIN`)
skips the autonomous transaction and rolls sequence changes back; `BEGIN CONCURRENT` uses an
independently committed inner transaction. The journal preserves that distinction by tracking
outstanding claims without treating them as a second authoritative cursor.

The operation owns the guard, oracle and bound files across yields. Oracle reads receive the
held guard and retain its lease through backend completion retirement. Success returns the
guard to the enclosing transaction state machine; it does not call `finish` on its behalf.
Failure/drop retains ownership through submitted backend mutations using `GuardedFile`, and
forces subsequent reconciliation rather than claiming an unpublished reservation succeeded.
Ordinary row-ID overflow uses the existing IO random source and the same 100-attempt positive
lower-half candidate range as `op_new_rowid`, checking each candidate against both committed
rows and live claims under the same authority. AUTOINCREMENT exhaustion never uses that fallback.

Before evaluating an action, require exact equality between oracle identity, supplied recovered identity and the held authority's published prefix. An oracle behind the current manifest may neither release a dead owner's claims nor issue a replacement ID. The caller imports committed frames before constructing this view; the component rejects a stale view rather than silently querying an old transaction snapshot.

The oracle is an integration seam for existing pager/MVCC reads, not a new public callback. Its tests use deterministic committed facts, while engine acceptance must exercise real row/index/sequence state. The owner must supply the real adapter before this feature is enabled.

Compaction has a separate owned operation because the pause must survive every yield:

```rust
struct DomainRemap { source: IdDomain, target: Option<IdDomain> }
struct CheckpointDomains {
    target: RecoveryIdentity,
    mapping: Vec<DomainRemap>,
    logical_floor: i64,
}
impl SharedIds {
    fn start_compaction(
        self: &Arc<Self>, guard: AuthorityGuard, pause: CheckpointPause,
        current: RecoveryIdentity, domains: CheckpointDomains,
        oracle: Box<dyn CommittedIdOracle>,
    ) -> Result<IdCompaction>;
}
impl IdCompaction {
    fn poll(&mut self) -> Result<IOResult<PreparedIdBank>>;
}
struct PreparedIdBank {
    guard: AuthorityGuard,
    pause: CheckpointPause,
    target: RecoveryIdentity,
    bank_generation: u64,
}
```

`PreparedIdBank` proves the replacement snapshot and selector preparation completed synchronization;
it does not publish the database generation. The coordinator retains its guard and
pause through the corresponding manifest/checkpoint transition. A same-generation
compaction can become selected through mirrored selector publication; a remapped bank remains ineligible
until that database generation is durably selected. The compactor first checks real
live registrations under the guard rather than trusting an external boolean.

## Allocation and release behavior

1. Under authority, load the selected bank and reconcile live owners against actual snapshot registrations. Apply the current committed oracle before removing dead claims: a dead process may have committed a durable frame before dying.
2. For a normal automatic row ID, use the greater of the committed current table maximum and every live claim for that table, then checked increment with the existing positive-row-ID rules. Do not turn normal row IDs into an AUTOINCREMENT historical floor. For AUTOINCREMENT also honor its durable committed backing-state floor; rollbacks cannot lower that committed floor.
3. Explicit insertion claims participate in the same live claim set before local execution publishes their result. Another live owner's conflicting claim returns the defined write-conflict result. Existing committed-row existence and ordinary uniqueness checks still run; this journal is not an alternate unique index.
4. On normal-row `i64::MAX`, use the existing bounded random-row-ID attempt policy. Check each candidate against the current committed oracle and every live explicit/automatic claim under the same authority before persisting it. AUTOINCREMENT exhaustion returns `DatabaseFull`; it never uses the random fallback.
5. Sequence allocation uses the descriptor, the current committed backing state and live reservations, with checked increment and the existing overflow/cycle semantics. For `AutonomousInner`, register the outer owner claim durably BEFORE the inner transaction can publish the advanced backing state; the inner commit then persists that state independently of outer rollback. For `OuterTransaction`, rollback/savepoint release permits reuse according to the reverted backing state; never persist a stronger cursor floor merely because a journal reservation was once issued. Track live allocated values by owner even when sequence execution changes the backing row locally. Ascending non-CYCLE safe-scan watermarks are the minimum of the committed next boundary and all live allocated values across processes. Other sequence forms retain their existing conservative watermark rule; do not advertise an ascending-prefix guarantee for descending/CYCLE sequences.
6. Persist a reservation change and synchronize both copies of its selector before returning its value. Gaps after cancellation or lost acknowledgement are allowed; returning the same live claim to another owner is not.
7. Commit retirement first establishes the durable user-log/manifest outcome and refreshes the committed oracle, then releases owner claims. Rollback/savepoint release is journaled under authority before reporting completion. A failed release retains conservative claims until recovery; it must not report them released. A savepoint or rollback before the first ID claim is a successful no-op; the runtime must not append a release for an unknown journal owner merely because a transaction savepoint exists.
8. Reclaiming dead or released row/sequence claims does not raise the negative logical-ID floor. Active sequence claims constrain watermarks until successful retirement or proven owner death.

Per-owner ordinals and journal/bank generations fail with `DatabaseFull` on overflow. Invalid domains, mismatched registrations, conflicting mappings or stale generation proofs fail without submitting writes. Allocation failures during decoding/preparation occur before a new tail is published; retry sees the previous selected state.

## Crash and acknowledgement table

| Last completed stage | Selected state after death | Required action before allocation |
| --- | --- | --- |
| Reservation prepared only in memory | Previous tail | Discard local candidate; never return it |
| Partial record write, or full record before data sync | Previous tail | Ignore/truncate bytes beyond selected tail |
| Record data synced, no new selector | Previous tail | Discard unacknowledged tail; no exposed ID depends on it |
| First new selector write partial/invalid | Previous valid selector | Retain prior state; never infer success from file length |
| New valid selector visible, mirror sync/response uncertain | New selected tail is safe because its data was synced first | Conservatively retain the reservation; caller gets no success without completed sync |
| Both selector copies synced, caller receives ID | New tail | Preserve the live owner's claim; do not reuse it |
| User transaction manifest committed, reservation release absent | Committed oracle plus old claim | Reconcile committed outcome before release/reclaim |
| Release record/selector partial | Previous selected claims | Keep conservative ownership until a valid release tail is selected |
| Inactive bank truncated or snapshot partially written | Previous complete compatible bank | Ignore incomplete replacement bank |
| Replacement snapshot synced, no valid selecting witness | Previous complete compatible bank | Ignore or finish replacement under authority |
| Complete bank selected by mirrored witness for the same database generation | New compacted bank | Reload selection; retain both inode handles |
| Mirrored witness records prepared next-generation bank; old manifest still selected | Old-generation bank | Checkpoint recovery completes/rolls back the coordinated generation transition; normal allocation stays closed |
| New database generation durably published; witness still carries both references | Matching complete remapped bank | Reject stale-generation callers; reload remapped domains and persistent logical floor |
| Successor-only witness mirrored; old bank truncated | New compatible bank | Continue from new bank; never recreate a missing inode |

A higher bank generation alone never overrides the selector witness, a missing checksum, an incomplete selected stream, or an incompatible database-generation proof. A highest valid selector whose selected data is corrupt causes failure; lower selector records are not recovery fallbacks after that validation failure. Previously selected-prefix corruption fails closed rather than reusing IDs from an older bank. Crash tests must distinguish an incomplete unpublished replacement from corruption of a bank already selected by the durable database generation.

## Compaction and resource bounds

`CheckpointPause` and `AuthorityGuard` must both be retained, and the coordinator must prove no live registrations. Compaction rejects a live owner; it is not allowed to erase reservations merely because its own process has no transactions.

After committed recovery and proven quiescence, the compact snapshot stores only
the persistent logical-ID floor, including when no claims remain. Every compacted
bank must encode that preserved floor explicitly; an empty replay
(default floor `-1`) is not a valid replacement for previously consumed IDs.
Current row maxima, AUTOINCREMENT floors and
sequence state remain authoritative in the synchronized DB/log through the exact
recovered generation/prefix oracle; do not duplicate those counters into allocator
authority. The coordinator still validates the complete domain remap and lowers
the preserved floor below every consumed logical/physical-root-derived identity.
Pending or unacknowledged autonomous inner sequence commits must be recovered
before quiescence is accepted; lack of live registrations alone does not prove
that their durable outcomes have been reconciled. Check the complete mapping, fallible allocation budget and encoded size before touching the inactive bank. Truncate only the inactive bank, write/sync its snapshot, then publish/sync both copies of the selecting or prepared-successor witness. Retire the old active content only after the matching database generation is durable and both selector slots no longer reference that bank. If this work is same-generation garbage compaction, the generation proof remains unchanged.

The minimum initialized metadata footprint is four 4096-byte pages: two selector slots,
one active bank header and its permanent logical-floor record; an unused bank may be empty.
Compaction also needs room for a replacement bank before the old bank can be reclaimed.
Repeated rolled-back reservations consume this same bounded budget even while long-lived
readers prevent compaction. Pressure returns bounded BUSY without evicting readers or claims.

The configured history budget accounts for both reservation bank sizes, selector bytes and the
current recovered logical prefix end offset; calculate the total under authority,
using the supplied `RecoveryIdentity` rather than a stale process-local estimate. Before exceeding it, return bounded contention and request a quiescent checkpoint; do not silently grow an unlimited reservation tail. If the irreducible compact state itself exceeds the configured budget, return an explicit resource error. This is a tunable byte budget, not a permanent number-of-tables limit. Peak compaction allowance includes both old and new snapshots; preflight must not start a transition that cannot retain the last complete bank.

Operation admission also reserves `max(0, 8192 - inactive_bank_physical_bytes)` for a
floor-only replacement bank. This is separate from factual retained bytes; already
retained inactive-bank space counts toward the same total and is not reserved twice.
Both ID appends and logical-log commit preflight must use `required_budget_bytes`
plus the prospective logical prefix. Without this reserve an active bank could fill
the cap and make quiescent compaction permanently impossible. Offline initialization requires a separate `MIN_HISTORY_LIMIT` of 32KiB: the
16KiB initial files, 8KiB replacement capacity, one 4KiB claim and a small 4KiB
logical frame. This does not guarantee any-sized write fits. `MIN_ID_BUDGET_BYTES`
is 24KiB after compaction, before adding a claim or logical frame. A single frame
that cannot fit even that empty-generation minimum returns TooBig; retained-history
pressure returns bounded BUSY. Neither error promises that checkpointing can always
make room. The factual namespace footprint remains 16KiB.

The ordinary append preflight conservatively counts existing physical tail bytes, including
abandoned unacknowledged bytes, before truncation. It can therefore return BUSY before it
can reclaim that tail. Recovery or quiescent compaction must provide bounded reclamation;
ordinary append must not relax the live-history budget to make progress.

Full transaction rollback can retire its snapshot registration without a mandatory journal
append; later reconciliation treats its claims as dead. Savepoint rollback must preflight
release capacity before local rollback reports success. If pressure prevents recording a
release, the engine must not report that the reservation claims were released.

Read-only participants can inspect reservation watermarks and liveness under authority but do not mutate reservation banks or repair tails. Mutating reconciliation is deferred to a writer/recovery owner; unavailable writable recovery returns an explicit error. Existing authority coordination bookkeeping remains governed by the approved read-only metadata policy.

## Implementation tasks and review gates

### Task 1: Journal/selector codec and selection

**Blocked by:** Owner/root approval of the interface, three new critical sidecars, generation binding and crash table.

**Files:** Create `core/mvcc/shared_ids.rs` with codec/state tests; add its private module declaration.

- [ ] Implement checked record/header encoding and decoding, selected-tail replay and compatible-bank selection.
- [ ] Test truncated lengths, invalid CRC, equal-selector-sequence conflicts, generation/sequence overflow, unknown domains and invalid owner ordinals. Exercise one corrupt selector copy after mirrored acknowledgement, both corrupt copies, and a corrupt data-bank header named by a valid selector.
- [ ] Build old bank + partially written higher-generation replacement with unchanged witness; selection must retain the selected old bank. Corrupt a selected prefix with an older complete bank available and require failure, not fallback. A newer valid selector pointing at bad data must not fall back to the older valid selector.
- [ ] Exercise arbitrary dynamic domain counts beyond 64 and 1024 to prove there is no authority-slot-derived table cap.
- [ ] Run `cargo test -p turso_core --features conn_raw_api shared_ids::codec`; observe a deliberately broken tail-selection control fail, restore, rerun, then commit.

### Task 2: Guarded reservation state machine

**Blocked by:** Task 1 and integrated `GuardedFile`; agreed live-registration accessor and real oracle identity contract.

**Files:** `core/mvcc/shared_ids.rs`; narrowly coordinated authority accessor only after lease-owner review.

- [ ] Implement owned guard/bindings and yielding read, append, data-sync, first-selector-write/sync and mirror-selector-write/sync phases.
- [ ] Test two owners reserving the same table before either commits, per-table independence, explicit claims, AUTOINCREMENT floors, random fallback collision/exhaustion and negative-ID exhaustion.
- [ ] Test savepoint release followed by another allocation: old release tokens must not affect new ordinals. Test stale oracle and stale generation rejection before writes.
- [ ] Test descending/CYCLE sequence behavior and the cross-process low-watermark case: a later committed high sequence must not let a reader skip a lower still-live allocation.
- [ ] Inject each allocation/short-write/sync error and cancel during every submitted mutation. A peer cannot acquire publication ownership until backend retirement; retries expose only a selected complete tail.
- [ ] Run focused shared-ID and guarded-file suites, including behavioral negative controls, then commit.

### Task 3: Quiescent bank compaction and generation remap

**Blocked by:** Task 2; checkpoint coordinator provides complete remap, pause token, no-live-owner proof and durable generation transition.

**Files:** `core/mvcc/shared_ids.rs`; component tests. Engine coordinator hooks remain owner-owned.

- [ ] Implement inactive-bank snapshot publication with both inode identities retained; reject duplicate target mappings, missing live domains and live reservations.
- [ ] Test DROP/recreate name, physical-root reuse, old-to-new logical-ID remapping, stale idle handles and persistent decreasing logical floor.
- [ ] Inject death at every tabled compaction boundary and every partial header boundary. Select only a complete bank compatible with recovered database state.
- [ ] Repeatedly compact a bounded live dataset and assert bounded allocated/file bytes with no inode changes; test insufficient peak budget without destroying old state.
- [ ] Run native independent-process reservation/death/restart tests and focused I/O fault tests, then commit.

### Task 4a: Offline initialization and strict sidecar preparation

**Blocked by:** Validated selector/record codecs, `InitialIds` initialization API and owner review.
This private namespace preparation does not depend on completed compaction and does not enable
runtime allocation. Root owns `mode_transition.rs` and `shared_files.rs`; this component agent
continues to own shared-ID codecs/policy.

**Files:** Offline mode initialization and `shared_files`.

- [ ] Introduce all three sidecars and protocol revision atomically in offline initialization/strict reopen. Marked files missing either bank or the selector fail closed; no compatibility fallback creates them.
- [ ] Obtain a validated logical-ID floor from the materialization backend, with no unsafe default for upgrades; initialize both mirrored selector slots through the canonical codec helper.
- [ ] Preflight `INITIAL_RETAINED_BYTES` before materialization or destructive stale-namespace cleanup; retain identity-checked files for runtime adapters.
- [ ] Record focused native initialization/open and old-binary/crash evidence without claiming global-ID allocation is enabled.

### Task 4b: Full private runtime integration and acceptance

**Blocked by:** Tasks 1–3, Task 4a and owner review. This is an integration-owner task, not permission for this component agent to edit engine/VM files.

**Files:** Checkpoint coordinator, row-ID/sequence/DDL call sites and PHP acceptance harness.

- [ ] Wire real imported committed oracle, transaction registration ownership, savepoints, commit/rollback retirement and bounded checkpoint requests. Logical-log commits and ID appends use the same combined retained-byte budget.
- [ ] Verify with simultaneous independent PHP writers before either commit, explicit IDs, rollback, process death, restart, DDL/checkpoint remapping and a delayed lower sequence commit.
- [ ] Record exact source/runtime identities and native evidence. Private component tests alone cannot close the global-ID or full shared-mode acceptance requirement.

## Selector codec milestone evidence

The first implementation slice contains the full-page selector codec, immutable
bank-header validation and witness-first selection. Eight focused local tests pass,
including every truncated selector-slot length and 4096 generated roundtrip/bit
corruption cases. Selection refuses corrupt referenced data even when an older
complete bank exists; prepared successor selection requires an exact recovered
database generation and checkpoint timestamp. Both-invalid selector copies and
equal-sequence conflicts fail closed. The initial mirrored-selection test failed
against the loading stub before implementation. Focused clippy and formatting pass
with the existing baseline lint allowances. Root independently reviewed the
selection/data-validation ordering.

This slice performs no runtime I/O and does not prove selector synchronization,
reservation acknowledgement, record replay, compaction or global-ID integration.
`slots_agree` reports matching read bytes only; it is not a durable bank-reuse proof.
The remaining Task 1 record/replay checks and Tasks 2–4 remain open.

## Sequence compatibility audit

The preserved provenance-verified CLI (runtime `eda0afbc4`, same production runtime
as `4ccf547a4`, SHA256
`7f178cb517da8f7171f0a2ba8d8ba45007108ddb36f7dc4f40389aa70c0cfe5f`)
confirmed these MVCC behaviors with fresh temporary databases:

| Transaction | nextval, outer rollback, nextval | nextval, savepoint, nextval, rollback-to, nextval, outer rollback, nextval |
| --- | --- | --- |
| `BEGIN` | `1, 1` | `1, 2, 2, 1` |
| `BEGIN CONCURRENT` | `1, 2` | `1, 2, 3, 4` |

`op_sequence_begin_inner_tx` skips the autonomous transaction for an exclusive
outer transaction. `op_sequence_register_allocation` attributes the in-flight
value to the saved outer transaction before inner commit publication, while
`op_sequence_track_allocation` updates the watermark afterward. Shared claims must
preserve that ordering, including before an inner commit can become visible to
another process. Releasing the outer claim changes the visibility watermark; it
does not undo an already committed autonomous inner sequence value.

## Neutral journal codec milestone evidence

Sixteen focused local shared-ID tests pass after adding canonical claim/release
record encoding and selected-prefix replay. The suite includes 2048 distinct table
domains, cross-owner row collisions, repeated claims by one owner with partial
savepoint release, owner retirement, non-reused ordinals, sequence claim minimums,
selected-tail exclusion and rejection of live claims in a quiescent snapshot.
Record tests cover signed boundaries, every truncated page length, every single-bit
corruption position, stale generation and invalid record/owner order. Temporarily
resetting the logical floor during savepoint release produced the expected
`-1` versus `-100` failure; restoration passed the full suite. Focused clippy and
formatting pass with the existing baseline lint allowances. Root independently
reviewed this codec milestone and the sequence compatibility refinement.

Replay state is private and discarded on error; its mutation helper is not an
atomic live-state update API. No actual journal I/O, mirrored-sync acknowledgement,
liveness reclamation or runtime allocation is enabled by these tests. Those remain
Tasks 2–4, including real committed-oracle and checkpoint integration.


## Owned journal loader milestone evidence

Twenty-five focused shared-ID tests pass: seventeen codec/replay checks and eight
owned-loader checks. The loader validates the held authority prefix, selects the
witness before reading its bounded bank prefixes, validates both referenced banks,
and retains the authority and file bindings for the next operation. Constructor
callers must supply existing-only files from the validated database identity
namespace; authority binding alone does not establish path provenance.

Loader tests cover short reads, dropped operations, explicitly aborted returned
completions, wrong-authority guards, stale prefixes, corrupt selected data, EOF and
backend errors. A backend that releases completion callbacks only from `IO::step`
reproduced a progress failure with a finished yield. Using the authority's pending
retirement completion makes that regression pass, and prevents success or authority
release before the backend releases its submitted reads. Focused library clippy
passes with the existing baseline lint allowances.

The live-owner timestamp boundary test also rejects the authority's reserved
maximum timestamp, while leaving recovered checkpoint timestamp decoding unchanged.
No reservation append, durable acknowledgement, compaction, dead-owner reclamation
or real allocator/VM integration is enabled by this loader milestone.


## Owned journal append milestone evidence

Thirty-one focused shared-ID tests pass after adding owned append publication.
The first durable-append test failed against the unimplemented operation. The
implemented operation stages replay, sequence and byte-budget checks before any
mutation, trims only the active bank's unacknowledged tail, synchronizes the new
record, then writes and FULL-syncs each selector copy. It returns its owned state
and guard only after backend I/O retirement. Pending generation transitions reject
ordinary append. The caller still supplies a policy-validated change; this layer
does not choose or authorize automatic IDs.

Six append tests cover mirrored durable bytes, seven mutation failure boundaries,
all seven delayed operations, abort at every operation with authority held until
backend retirement, repeated append using the same bindings, and preflight errors
with no writes. Failure tests reconstruct selection from durable byte images and
require a complete old or new floor. Budget coverage includes the inactive bank's
physical size. Root independently reviewed preflight and durability ordering.

This is deterministic component evidence, not process-crash or real-filesystem
acceptance. The reservation policy, committed-oracle adapter, liveness reconciliation,
quiescent compaction and coordinated sidecar/runtime initialization remain open.


### Sequence claim minimum integration boundary

`LowestLiveSequenceClaim` reports only the numeric minimum of live claimed values.
It is not a complete safe-scan boundary or an ordering over CYCLE/descending
allocations. The existing `MvStore::sequence_watermark` combines this kind of
active-allocation minimum with the current sequence boundary. Its
`first_unsafe_sequence_watermark` helper specifically targets ascending non-CYCLE
CDC scans. Runtime integration must preserve that descriptor/current-boundary logic.
The `SequenceDurability` argument identifies the caller's existing transaction path;
the journal does not itself commit or roll back sequence cursor mutations.


## Reservation policy milestone evidence

Forty focused shared-ID tests pass, including nine policy tests using the actual
snapshot registration API with deterministic file and committed-oracle fixtures.
The first two-owner reservation test failed against the policy stub. Two owners
now obtain distinct automatic row IDs before either commits. Tests cover dead-owner
reconciliation, committed maxima and AUTOINCREMENT/logical floors, negative row
starts, exhausted AUTOINCREMENT, the existing random fallback's 100-attempt bound,
savepoint-before-first-claim no-op, and permanent floor/ordinal preservation.

Supplied sequence values cover CYCLE-shaped repetitions, negative increments and
signed limits in both caller routing modes. These tests show that the journal
records supplied values; they do not prove engine sequence descriptor validation or
transaction-mode integration. Read-only lowest-claim queries exclude dead owners
without appending cleanup. Delayed oracle read tests cover cancellation and success
while backend callbacks retain the authority lease. Stale oracle identities and
unregistered owners fail before mutation. Focused library clippy passes with the
existing baseline allowances; changed Rust files pass formatting.

Root reviewed row/logical decisions and required the distinction between a numeric
claim minimum and the engine's complete sequence scan boundary. Native multi-process
acceptance, the real database oracle/VM hooks, coordinated sidecar initialization and
quiescent compaction remain required before enabling the feature.


## Offline initialization helper

`InitialIds::new(database_id, generation, logical_floor)` creates a canonical
8192-byte bank A snapshot and 8192-byte mirrored selector; bank B is an existing
empty inode. `INITIAL_RETAINED_BYTES` is 16KiB. The helper uses the same codecs and
validation as reopen, accepts a caller-supplied materialized floor, and performs no
I/O. Its stub failed the roundtrip test before implementation. All 42 focused tests
pass, including invalid floor rejection and accounting for both retained bank tails.

`LoadedIds::retained_bytes` reports physical ID metadata bytes under its held guard;
`IdOutcome` carries that count through policy handoff to avoid rebinding the same
files to a still-live lease. It excludes logical bytes. Operation admission will
add separate floor-only compaction headroom; a namespace that fits 16KiB alone
need not have enough budget to reserve or compact. Task 4a may consume these helpers
without claiming Task 4b or compaction is complete.


## Same-generation compaction milestone evidence

`LoadedIds::compact_floor_only(identity, committed_floor)` continuously retains its
authority, requires no live registrations and no pending generation transition,
and preserves the minimum of the permanent and prefix-matched committed floor.
It freshly mirror-syncs the current witness before reusing the inactive bank,
synchronizes a floor-only successor, mirror-syncs its selection, then truncates and
synchronizes the old bank. It neither changes the database generation nor remaps
domains. The coordinator owns admission pausing/deadlines while waiting for
quiescence; the operation keeps publication authority through all submitted I/O.

All 50 focused tests pass. Seven compaction tests cover actual reclamation at a
fixed 32KiB limit, repeated bounded file sizes with retained inode identities,
readers/stale identities/invalid floors/sequence exhaustion rejected before writes,
all fourteen mutation failure boundaries, and cancellation/backend retirement at
every mutation. Durable image selection always retains a complete old or new floor.
The initial reclamation test failed against the stub. A separate causal control
omitted the initial second-selector synchronization: interrupted bank reuse plus
one damaged selector then failed with a selected-header mismatch. Restoring both
current-witness synchronizations made that test and the complete suite pass.
Focused library clippy and changed-file formatting pass with baseline allowances.

These deterministic component tests do not prove native multi-process operation,
real committed-oracle integration, or cross-generation checkpoint remapping. The
prepared-successor/finalization protocol and complete domain remap remain required
parts of Task 3; this helper cannot substitute for them.
