# Shared MVCC authority technical spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a private recoverable authority for independent MVCC processes, without enabling database access through it yet.

**Architecture:** A separate 4096-byte mapped logical coordination file uses fixed-width little-endian atomic fields. Linux OFD byte locks serialize state publication and identify live snapshot registrations. A guard interrupted before explicit completion leaves recovery required; callers cannot read trusted publication state or register new snapshots until validated recovery supplies the prefix.

**Tech Stack:** Existing `IO`, `File`, `SharedWalMappedRegion`, native mapped atomics, Rust subprocess tests. No new dependency.

## Global constraints

- Preserve all existing public mode guards and formats.
- The coordination mapping is recoverable metadata, never evidence of durable log bytes by itself.
- Scope constructor support to 64-bit Linux with an IO backend supporting shared mappings and OFD locks.
- No PID-based liveness guesses, shared Rust heap objects or silently ignored lock errors.
- Hold no Rust mutex across kernel waits or yielded database I/O.
- Every acquisition is nonblocking. Higher-level transaction/open code owns bounded busy deadlines.

## Task 1: Cross-process ownership and death recovery

**Blocked by:** None; the committed-prefix decoder is already independently reviewed.

**Files:**
- Create: `core/mvcc/shared_authority.rs`
- Modify: `core/mvcc/mod.rs`

**Interfaces:**
- `SharedMvccAuthority::open(io: &Arc<dyn IO>, path: &str, database_id: FileId, history_limit: u64) -> Result<Arc<Self>>`
- `SharedMvccAuthority::try_acquire(self: &Arc<Self>) -> Result<AuthorityGuard>`
- `AuthorityGuard::requires_recovery() -> bool`
- `AuthorityGuard::recover(prefix: PublishedPrefix) -> Result<()>`
- `AuthorityGuard::published() -> Result<PublishedPrefix>`
- `AuthorityGuard::finish(self) -> Result<()>`
- `PublishedPrefix { committed_timestamp: u64, end_offset: u64, running_crc: u32 }`

- [ ] Add real subprocess tests and deterministic nonfunctional constructor for the first Linux red run. The stub is confined to this private unused module and must be replaced before final review.
- [ ] Parent runs `cargo test -p turso_core --lib mvcc::shared_authority::tests -- --nocapture` on Linux against the tests/stub commit. Expected failure: opening the unimplemented authority, not compilation or subprocess setup.
- [ ] Replace the stub with the mapped authority. Layout offsets are magic0, version8, device16, inode24, epoch32, next timestamp40, committed timestamp48, log offset56, log CRC64, recovery flag72, history limit80, slot count88; slots start128 and occupy32bytes each, with64 slots. All fields use aligned64-bit native atomics storing little-endian values.
- [ ] Byte lock0 covers mapping lifetime; lock1 covers publication; locks2–65 cover snapshot registrations. Hold a local atomic ownership flag as well, because the same OFD can reacquire its own lock. Clear that flag if acquiring the kernel lock fails.
- [ ] Initialize only a zero/unpublished mapping while holding exclusive lifetime ownership. Publish magic last. Reject incompatible nonzero magic/version/identity/limit without rewriting bytes. Downgrade lifetime ownership to shared only after initialization. Exclusive reopen clears dead registrations and requires recovery.
- [ ] Set recovery required before exposing every publication guard. An explicit successful finish clears it before releasing ownership. Dropping an unfinished guard releases ownership but retains the flag. Surface unlock failure and prohibit subsequent use of that authority.
- [ ] Verify independent contention and killed-owner recovery, then commit tested code with the exact Linux CI source revision.

## Task 2: Shared snapshot lifetime, timestamps and checkpoint exclusion

**Blocked by:** Task 1

**Interfaces:**
- `AuthorityGuard::register_snapshot() -> Result<SnapshotRegistration>`
- `AuthorityGuard::oldest_snapshot() -> Result<Option<u64>>`
- `AuthorityGuard::allocate_timestamp() -> Result<u64>`
- `AuthorityGuard::check_append_size(bytes: u64) -> Result<()>`
- `SnapshotRegistration { begin_timestamp, epoch, prefix }` plus private slot ownership.

- [ ] Add a child process that registers a snapshot, releases publication ownership, signals readiness over a pipe and remains alive until explicitly released or killed.
- [ ] Register a parent snapshot while the child snapshot is active; timestamps must differ and the minimum must include the child. Kill the child, then verify only the parent's snapshot remains. Drop the parent snapshot and require no active snapshots.
- [ ] Claim a free kernel slot under publication ownership, record epoch/begin/prefix, and keep its byte lock until registration drops. Track same-OFD slots locally so probes cannot misclassify a sibling snapshot as dead. Clear a slot before unlocking it. A dead process is recognized only by successfully obtaining its former slot lock.
- [ ] Reserve timestamp zero and reject exhaustion at the existing62-bit MVCC timestamp limit. Recovery may advance but never rewind allocation while live snapshots remain.
- [ ] Reject a single encoded append larger than the configured history limit with `TooBig`; reject total retained bytes beyond the limit with `Busy`, without changing the published prefix. Actual append and checkpoint integration remain later stages.
- [ ] Test slot exhaustion, abandoned local guards, incompatible live versions and complete reopens. Run the full Linux authority suite and independent review before wiring any database path.

## Delivery boundary

The authority accepts a recovered prefix only from future verified log recovery.
Tests use synthetic prefixes to exercise coordination, not to claim log durability.
This stage does not implement live row installation, global unique validation,
row/sequence reservation, crash-safe mode migration, checkpointing or PHP opt-in.
Those requirements remain mandatory in the parent protocol proposal.
