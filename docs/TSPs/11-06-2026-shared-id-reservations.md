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
struct IdDomain { database_generation: u64, table_id: MVTableId, kind: DomainKind }
struct RecoveryIdentity {
    database_generation: u64,
    checkpoint_timestamp: u64,
    prefix: PublishedPrefix,
}
enum IdAction {
    ReserveRow { owner: IdOwner, domain: IdDomain, autoincrement: bool },
    ClaimExplicitRow { owner: IdOwner, domain: IdDomain, value: i64 },
    ReserveSequence { owner: IdOwner, domain: IdDomain },
    ReserveLogicalTable { owner: IdOwner },
    Savepoint { owner: IdOwner },
    ReleaseAfter { mark: SavepointMark },
    ReleaseOwner { owner: IdOwner },
    SequenceWatermark { domain: IdDomain },
}
enum IdQuery {
    RowFacts { domain: IdDomain },
    RowExists { domain: IdDomain, value: i64 },
    SequenceFacts { domain: IdDomain },
    LogicalFloor,
}
struct SequenceFacts {
    value: i64, is_called: bool, increment: i64, minimum: i64, maximum: i64, cycle: bool,
}
enum IdFact {
    RowFacts { maximum: Option<i64>, autoincrement_floor: i64 },
    RowExists(bool),
    SequenceFacts(SequenceFacts),
    LogicalFloor(i64),
}
trait CommittedIdOracle: Send {
    fn identity(&self) -> RecoveryIdentity;
    fn poll(&mut self, query: &IdQuery) -> Result<IOResult<IdFact>>;
}
impl SharedIds {
    fn open_existing(
        banks: [Arc<GuardedFile>; 2], selector: Arc<GuardedFile>,
        database_id: FileId, history_budget: u64,
    ) -> Result<Arc<Self>>;
    fn start(
        self: &Arc<Self>, guard: AuthorityGuard, identity: RecoveryIdentity,
        oracle: Box<dyn CommittedIdOracle>, action: IdAction,
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
    Watermark(Option<i64>),
}
```

`SequenceFacts` carries the existing descriptor's current value, `is_called`, increment, minimum, maximum and cycle behavior. It is not an ascending-only replacement for the current sequence contract. The operation owns the guard, oracle and bound files across yields. Success returns the guard to the enclosing transaction state machine; it does not call `finish` on its behalf. Failure/drop retains ownership through any submitted backend mutations using `GuardedFile`, and forces subsequent reconciliation rather than claiming an unpublished reservation succeeded.

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
5. Sequence allocation uses the descriptor and all earlier journal allocation steps, with checked increment and the existing overflow/cycle semantics. Track live allocated values by owner even when sequence execution changes the backing row locally. Ascending non-CYCLE safe-scan watermarks are the minimum of the committed next boundary and all live allocated values across processes. Other sequence forms retain their existing conservative watermark rule; do not advertise an ascending-prefix guarantee for descending/CYCLE sequences.
6. Persist a reservation change and synchronize both copies of its selector before returning its value. Gaps after cancellation or lost acknowledgement are allowed; returning the same live claim to another owner is not.
7. Commit retirement first establishes the durable user-log/manifest outcome and refreshes the committed oracle, then releases owner claims. Rollback/savepoint release is journaled under authority before reporting completion. A failed release retains conservative claims until recovery; it must not report them released.
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

Reconstruct a compact snapshot containing current live domains, required sequence state, AUTOINCREMENT floors and the persistent logical-ID floor. Check the complete mapping, fallible allocation budget and encoded size before touching the inactive bank. Truncate only the inactive bank, write/sync its snapshot, then publish/sync both copies of the selecting or prepared-successor witness. Retire the old active content only after the matching database generation is durable and both selector slots no longer reference that bank. If this work is same-generation garbage compaction, the generation proof remains unchanged.

The configured history budget accounts for both reservation bank sizes, selector bytes and the
current recovered logical prefix end offset; calculate the total under authority,
using the supplied `RecoveryIdentity` rather than a stale process-local estimate. Before exceeding it, return bounded contention and request a quiescent checkpoint; do not silently grow an unlimited reservation tail. If the irreducible compact state itself exceeds the configured budget, return an explicit resource error. This is a tunable byte budget, not a permanent number-of-tables limit. Peak compaction allowance includes both old and new snapshots; preflight must not start a transition that cannot retain the last complete bank.

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

### Task 4: Coordinated private integration and acceptance

**Blocked by:** Tasks 1–3 and owner review. This is an integration-owner task, not permission for this component agent to edit engine/VM files.

**Files:** Offline mode initialization, `shared_files`, checkpoint coordinator, row-ID/sequence/DDL call sites and PHP acceptance harness.

- [ ] Introduce all three sidecars and protocol revision atomically in offline initialization/strict reopen. Marked files missing either bank or the selector fail closed; no compatibility fallback creates them.
- [ ] Wire real imported committed oracle, transaction registration ownership, savepoints, commit/rollback retirement and bounded checkpoint requests.
- [ ] Verify with simultaneous independent PHP writers before either commit, explicit IDs, rollback, process death, restart, DDL/checkpoint remapping and a delayed lower sequence commit.
- [ ] Record exact source/runtime identities and native evidence. Private component tests alone cannot close the global-ID or full shared-mode acceptance requirement.
