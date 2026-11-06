# Independent PHP worker MVCC protocol proposal

This proposal extends the approved requirements in
`docs/PRDs/11-06-2026-independent-php-worker-concurrency.md`.
It is based on source `ab057ba49`. No runtime implementation or acceptance
result is claimed. Issue #7 and PR #8 remain incomplete.

## Existing implementation and missing contracts

| Boundary | Existing source | Required change |
| --- | --- | --- |
| Open | `core/database.rs`, `DatabaseOpts`, `effective_open_flags_for_path`, database bootstrap | Separate validated protocol option; mode agreement before ordinary locking is bypassed |
| Begin | `core/mvcc/database/mod.rs`, `begin_tx_with_schema_generation_and_checkpoint_read_guard` | Import an agreed committed prefix and register the snapshot atomically against checkpoint admission |
| Clock | `core/mvcc/clock.rs`, `MvccClock` | Allocate shared transaction/begin/commit identities; keep publication atomic with begin |
| Replay | `core/mvcc/database/mod.rs`, `maybe_recover_logical_log`, `recover_process_frame` | Apply peer frames while preserving active local versions and transaction schemas |
| Append | `core/mvcc/database/mod.rs`, `CommitState`; `persistent_storage/mod.rs`, `DurableStorage` | Global append ownership, peer conflict validation, successful sync before publication |
| Allocation | `core/mvcc/cursor.rs`, `start_new_rowid`; `database/mod.rs`, `RowidAllocator`, sequence maps | Reserve IDs across live processes, reconcile explicit IDs and recovery |
| Checkpoint | `core/mvcc/database/checkpoint_state_machine.rs` | Global admission, election, durable generation publication and recovery |
| PHP | `bindings/c/src/lib.rs`, `ParsedFilename`, `parse_uri_filename`, `sqlite3_open_v2` | Explicit URI option, extended errors, bounded busy handling |

The existing recovery entry point is not a live-replay API. It resets the local
clock, publishes the database/connection schema and restores the writer offset.
Calling it during an active local transaction could rewind local state or change
the schema that the transaction uses. Its row and index decoding can be reused
only after separating decoding from version installation and schema publication.

The existing `.tshm` authority describes physical WAL frames. Its reader slots
cannot represent logical snapshots or commit positions. Its operating-system
lock and mapping primitives are reusable; its frame numbers must retain their
existing meaning.

## Approved behavior decisions

The user approved these policies on 2026-11-06 during the implementation session.

1. Use an explicit URI option through the C compatibility interface. Ordinary
   opens remain unchanged. Initial support is 64-bit Linux and validated local
   filesystems. Reject unsupported backends, custom durable storage, mixed live
   modes and unsupported option combinations before persistent mutation.
2. Use quiescent checkpoints initially: briefly stop transaction admission,
   wait within the caller's busy deadline for active transactions to finish,
   then checkpoint and publish a new generation. Long readers retain their
   snapshots and cause bounded contention rather than being silently aborted.
3. Enforce a configurable retained-log byte limit. When a checkpoint cannot
   progress because of active snapshots, reject additional commits with bounded
   `BUSY`. Do not grow the log without bound. A transaction whose encoded frame
   alone exceeds the limit fails explicitly without partially writing it.
4. Read-only participants in the new mode may update coordination metadata to
   register their snapshots. Database, WAL and logical-log contents remain
   read-only. Reject the opt-in if coordination metadata cannot be written.
   Ordinary read-only opening and private TEMP behavior keep their current
   guarantees.

These are observable policies added to the original PRD. The URI spelling,
default log limit, slot capacity and binary layout are implementation details
to pin in the executable specification.

## Authority and ownership

Create a separate versioned logical coordination layout, without placing Rust
pointers, heap containers or process-private mutexes in mapped bytes. Fields
include database identity, protocol version, epoch, committed log position and
CRC, last committed timestamp, checkpoint state and participant registrations.
Each registration has an incarnation, snapshot timestamp, imported position and
operating-system lifetime lock. Kernel lock ownership establishes liveness;
PID equality alone cannot establish that a participant survived.

Use explicit byte encoding for durable records and aligned native atomics only
for fields whose mapping and publication semantics are validated. A mapping is
a cache of recoverable state, never sole evidence that an append is durable.
Readers of several fields need a coherent publication protocol; separately
loading atomics is insufficient.

Acquire locks in one order: participant admission, commit ownership, then the
existing physical checkpoint/WAL ownership. Same-process coordination must obey
the same rules. Never hold a Rust mutex over yielded I/O. Ownership guards must
survive yielded state-machine steps and release on cancellation and errors.

Mode negotiation must exclude old binaries that know only existing WAL/MVCC
modes. Opening a new sidecar alone does not accomplish this: the ordinary
database lifetime lock and existing coordination-file version checks must be
included in the proof. Test an old executable as an independent participant.

### Old binaries and persistent mode selection

Existing fork binaries inspect the physical `.tshm` lifetime lock even for
ordinary read-only opens, through
`Database::reject_live_multiprocess_wal_for_legacy_open`. Retaining that lifetime
mapping can exclude those binaries. Binaries predating that check cannot be
excluded from read-only access by multiple workers holding shared database locks.

A persistent new `RawVersion` value is proposed to exclude those older readers:
`Database` already rejects unknown header read/write versions before MVCC
bootstrap. All existing versions remain readable by the new implementation.
An opted-in database requires explicit safe downgrade before older binaries can
reopen it. The user approved this compatibility policy on 2026-11-06. No header
value has yet been allocated or changed in the implementation.

A marker rejects future old opens; it cannot revoke an already-open old read-only
handle that holds no database lock. The user approved requiring an explicit
offline upgrade with every older worker stopped, including those unlocked
readers. Ordinary shared-mode URI opening must reject an existing old-format
database rather than silently upgrading it. The transition command/procedure
must state this stopped-worker prerequisite; an exclusive database lock alone
cannot verify it.

For a newly created database, the final pathname must not expose an ordinary or
empty header before the new marker is durable. Publish a fully initialized,
marked inode atomically without overwriting a racing creator, and synchronize
its identity-specific sidecars and parent directory. The concrete publication
implementation and old-reader race tests remain required.

Upgrade and downgrade must follow these transitions:

| Transition | Required ownership and durable action | Crash recovery |
| --- | --- | --- |
| Start upgrade | Explicit offline action after all older workers are stopped; exclusive ordinary database lock and no protocol participants | Before any durable marker, ordinary old-format recovery remains valid |
| Prepare upgrade | Finish ordinary MVCC recovery/checkpoint; synchronize database and sidecars | Reopen through ordinary recovery if the marker is still old |
| Publish new marker | Write matching read/write versions through the pager and synchronize the complete checkpoint | Old readers reject a durable new marker; new readers reconcile incomplete physical WAL work |
| Enable protocol admission | Create/validate coordination epoch only after marker durability | No shared-mode commit can precede durable old-reader exclusion |
| Start downgrade | Close admission, wait for all transactions, acquire exclusive ordinary database ownership | Marker remains new; only protocol-aware recovery is allowed |
| Materialize downgrade | Checkpoint all acknowledged commits, synchronize main database, clear logical history and finish WAL cleanup | Marker remains new until every committed row is represented in old-readable storage |
| Publish old marker | Update read/write versions through ordinary pager commit and complete its synchronization | A surviving old marker is safe because materialization preceded it; ordinary WAL recovery handles the marker transaction |
| Finish downgrade | Remove only unused logical coordination metadata after all protocol handles leave | Idle old-generation workers must fail mode validation before any new operation |

The upgrade/downgrade commands and error mapping need explicit tests with an old
executable, including an old read-only handle already open before a shared-mode URI request.
That ordinary request must reject implicit upgrade. The explicit offline
transition is exercised only after the operator has stopped the older workers.
Changing a marker directly with a byte write is not an implementation of this
transition protocol.

## Begin and live replay

Under admission and commit coordination, capture a complete committed prefix
and register a snapshot that prevents checkpoint reclamation. Import through
that prefix before making the transaction usable. The registration remains
live during yielded reads. No transaction may observe half of a replayed frame.

Decode a whole frame against its before/after schema before installing it.
Reserve required allocations before changing visible version chains. On
allocation failure, retain the previous imported boundary and make retry
idempotent; never mark a partially installed frame imported. Preserve local
uncommitted versions and old snapshots while adding committed peer versions.
Local GC must preserve versions required by all local transactions; cross-process
log retention follows registrations, not this local GC calculation.

Schema-changing transactions use exclusive global transaction admission.
Existing local statements retain their captured schema until transaction end.
Idle statements reprepare after the schema generation changes. Internal negative
table IDs also need shared allocation; reserving ordinary row IDs alone is
insufficient.

## Commit and recovery ordering

1. Acquire global commit ownership within the busy deadline. If the previous
   owner died, reconcile complete durable frames and the published boundary
   before accepting another append.
2. Import peer commits since the transaction's snapshot. Validate its complete
   write set against intervening versions, deletes and index keys using SQL
   collation and NULL semantics. Do not replay the explicit transaction's SQL.
3. Allocate its ordered commit timestamp and serialize one atomic log frame.
   Check the retained-log limit before starting the append.
4. Append at the validated shared offset with the corresponding running CRC.
   Short writes and failed completions must not advance the committed prefix.
5. Complete required synchronization before publishing the committed prefix or
   returning success. Initial durable acceptance uses `synchronous=FULL`.
   Any weaker supported mode must retain its documented durability semantics
   and cannot be used to claim crash durability.
6. Publish the committed boundary, finalize local versions, then acknowledge.
   The success callback must never run before this sequence completes.

A fully valid frame may survive even when its caller did not receive success;
this is a permitted ambiguous commit. An acknowledged frame may never vanish.
After owner death, validate the frame chain from the last trusted boundary and
truncate only an incomplete tail under exclusive append ownership. Never treat
a corrupt frame inside the acknowledged prefix as an ordinary tail.

### Durable publication record

The mapped authority's timestamp, offset and CRC fields are written separately.
They are safe to consume under a completed publication guard, but they are not a
trusted recovery boundary after owner death. `recover(prefix)` in the private
authority component deliberately accepts a prefix supplied by a future verified
recovery caller; its process tests use synthetic prefixes and prove no log
durability property.

The private manifest component uses separate 4096-byte aligned slots at offsets
0 and 4096. Each slot contains a 128-byte checksummed record and zero padding;
publication writes the entire inactive block. Each record
has a format version, record sequence, database identity, log generation/salt,
committed timestamp, end offset, running log CRC and record checksum. Write only
the inactive slot after the logical-log write and synchronization complete.
Check the exact write count and synchronize the manifest before shared
publication and acknowledgement. Never overwrite the latest valid record while
preparing its successor. A complete but unacknowledged newer record may be
adopted after validating its referenced log prefix; an incomplete successor must
leave the preceding acknowledged boundary intact.

The active mode marker requires a valid manifest. Empty or missing manifests
must not silently initialize an existing opted-in database. Initial upgrade
creates and synchronizes its first record before making the new mode durable.
Recovery first reconciles physical WAL/checkpoint metadata: a durable checkpoint
may already contain every transaction in an older manifest. Otherwise, it
validates the manifest's committed log prefix strictly and treats only later
bytes as a tentative suffix. This ordering is required before the current
synthetic recovery API can be used by real transaction code.

Failure tests must interrupt the inactive-slot write at every byte, delay/fail
both synchronization operations, kill the publisher before and after each
completion, and reopen through a fresh process. The external acknowledgement
ledger remains the acceptance oracle. The manifest follows the filesystem's
successful-write-and-fsync durability contract; checksums do not repair arbitrary
later media corruption.
The torn-write model preserves previously synchronized, separately addressed
blocks; placing both records in one sector would not satisfy it.

The yielding loader owns publication authority through slot selection. It reads
short completions to the expected record length and rejects unexpected EOF.
The publisher checks the full block byte count, then waits for manifest sync.
Both read and write/sync completion callbacks retain authority ownership until
the backend retires the operation. A cancelled completion's `finished` flag is
not evidence that the kernel has stopped using its buffers. In the io_uring
backend, `get_key` retains the completion with `Arc::into_raw`; only processing
the actual operation CQE consumes that reference. The cancellation CQE does not.
Callbacks never extract the authority guard on abort or error. Only observing a
successful manifest sync transfers it to the durable result.

Local manifest tests cover delayed reads in seven-byte chunks, every short
block-write count, failed sync, missing active-mode manifests and repeated polls
after terminal results. Native Linux authority-retention validation and actual
process-death/acknowledgement-ledger tests remain pending at this stage. The
component is private and has no engine commit caller yet.

The process-local group-commit queue cannot independently own the shared append
position. Initial protocol commits use the single global publisher; later group
commit optimization needs its own proof of ordering and error propagation.

## Row IDs and sequences

Allocate automatic row IDs and sequence values from shared reservations while
transaction execution still overlaps. Reconcile committed explicit row IDs
before allocating. Track reservations by table/sequence identity and participant
incarnation. Owner death releases its live reservation ownership without
invalidating committed high-water marks. AUTOINCREMENT's committed floor must
survive recovery. Overflow and random-rowid fallback require global collision
checking rather than process-local probing.

Sequence visibility watermarks must include allocations in every live process;
otherwise a consumer can permanently skip a late commit with a lower sequence
value. Rollback, savepoints and process death must release those allocations.

## Checkpoint and bounded reclamation

The elected checkpointer closes admission and obtains a globally quiescent
snapshot. If the deadline expires, it reopens admission and returns contention;
it must not leave future transactions blocked. Active readers keep their data.

Once quiescent, import the complete committed prefix, perform the existing
logical-to-WAL-to-database checkpoint with its required synchronization order,
and durably publish a generation boundary. Only then reclaim logical history.
Idle workers compare their epoch before beginning another transaction, discard
stale pager/schema/version state and rebuild from the new checkpoint. A sleeping
idle worker cannot retain authority to write an old log offset.

Owner death at each checkpoint stage must recover through the physical WAL and
logical metadata without trusting a half-published mapping. Reclamation and
generation publication need a concrete crash-state table before implementation;
the existing checkpoint callback pair alone is not that protocol.

### Checkpoint crash-state table

The following transitions run with transaction admission closed and global
commit ownership held. No active transaction remains. The existing checkpoint
states `SyncDbFile`, `TruncateLogicalLog`, `FsyncLogicalLog` and `TruncateWal`
establish the durable order; a shared epoch cannot replace their synchronization.

| Last completed transition | Durable evidence after owner death | Recovery before reopening admission |
| --- | --- | --- |
| Admission closed; no checkpoint writes | Old main database and complete logical prefix | Recover abandoned ownership, import old prefix, reopen admission |
| Physical WAL frames written without commit | Old main database, incomplete WAL transaction, retained logical prefix | Discard uncommitted WAL tail and replay the full logical prefix |
| Physical WAL commit synchronized | Committed WAL includes materialized rows and checkpoint timestamp; logical prefix retained | Complete WAL recovery, retaining the logical log until database sync succeeds |
| Database backfill synchronized | Main database contains checkpoint timestamp and rows; WAL and logical prefix may both remain | Skip logical frames at or below checkpoint timestamp; continue reclamation |
| Logical log truncated but truncation not synchronized | Main database durable; old, short or empty logical file may survive | Reconstruct from durable database/WAL metadata; never treat the old mapped offset as an append position |
| Logical truncation synchronized, WAL retained | Durable checkpoint plus empty logical log | Complete WAL cleanup before publishing the next generation |
| WAL cleanup completed, shared epoch not published | Durable database checkpoint; logical log empty | Rebuild local store and publish a fresh epoch under global ownership |
| New epoch published | Completed checkpoint and coherent mapped epoch | Idle workers discard prior generation state before beginning |

An empty logical file after checkpoint is represented by a zero-length prefix;
the committed-prefix decoder is constructed only after a complete header exists.
The first writer creates that header under global ownership. A read-only worker
must not create it merely to represent an empty log.

Each transition needs a process-kill point both before and after its completion
is observed, plus short-write/sync-failure injection. A failed checkpoint must
not publish a newer shared epoch until recovery has reconciled physical and
logical files. If recovery itself fails, admission stays closed and callers
receive the error within their deadline.

## Delivery and verification sequence

| Stage | Deliverable | Required negative and positive evidence |
| --- | --- | --- |
| A | Versioned authority and participant lifetime | Independent processes contend; death releases ownership; unknown versions and mixed modes reject without modifying files |
| B | Bounded committed-prefix decoding and atomic live replay | A frame beyond the published prefix is invisible; allocation failure leaves replay retryable; active local reader and writer survive peer replay |
| C | Begin/commit integration and conflict validation | Barriers prove overlap; fresh snapshots see peer commits; old snapshots remain stable; same-row, delete and unique-key conflicts cannot both succeed |
| D | IDs, sequences and schema coordination | Concurrent allocations differ; rollback/restart preserves committed floors; explicit IDs, overflow and sequence watermarks are covered |
| E | Checkpoint, epoch reload and reclamation | Long reader delays checkpoint; log cap bounds growth; release permits reclamation; sleeping worker reloads; death at every persistent transition recovers |
| F | C/PHP opt-in and process acceptance | SQLite3 and PDO processes run full CRUD, deadlines, rollback, read-only/TEMP/ATTACH and external acknowledgement-ledger recovery |
| G | Regression and equal-durability comparison | Storage issue #14 integrated; default modes remain compatible; original issue #11383 rolling restarts exercised; compare actual PHP workers with SQLite |

Each stage needs executable tests that first fail against its preceding source,
reviewable commits and independent review. Stages A–E remain inaccessible to
ordinary public opens until their protocol tests pass. Existing rejection of
MVCC with experimental multiprocess WAL remains in place.

Extend `core/multiprocess_tests.rs` for native subprocess orchestration and
`tests/integration/` for fault-injected storage behavior. Extend
`bindings/c/tests/compat/` for URI and error mapping. Add the PHP acceptance
driver alongside the binding tests, with line-delimited control messages over
pipes and bounded waits instead of sleeps as synchronization.

The external supervisor records a transaction UUID and exact row payload only
after a worker reports successful commit, then flushes and synchronizes its
ledger before selecting that worker for a kill. Fresh processes compare every
acknowledged transaction's complete row set and run integrity checks. Unreported
transactions may be wholly present or absent; partial transactions are always a
failure. Record seeds, process topology, durability settings and kill points so
failures can be reproduced. Thread-only results and reopened handles in the same
process cannot substitute for this evidence.

## Current execution limits

Private components implemented so far:

- The committed-prefix log reader passed 73 native logical-log tests with
  `conn_raw_api`, including generated plain/encrypted boundaries and delayed
  short reads. Independent review found two edge cases (buffered truncation and
  missing committed headers); both were reproduced as failing tests and fixed.
- The private shared authority at `5b5814ccd` passed six Linux tests (five
  substantive cases plus its subprocess entry point), and eight strict-prefix
  tests passed in the same [validation run](https://github.com/HundredBillion/turso/actions/runs/42237976884).
  Its preceding [red run](https://github.com/HundredBillion/turso/actions/runs/42237152419)
  compiled and failed the three initial behavioral tests at the deliberately
  unimplemented constructor. It covers ownership, process death, snapshot slots,
  limits and incompatible participants. It does not connect authority to database
  transactions or establish durable commit recovery.

The development host is macOS. The approved initial target is Linux, so native
macOS success cannot establish support. A local Docker Linux aarch64 runner is
available. Heavy builds and benchmarks are coordinated with the storage agent.
The committed-prefix reader has native unit-test evidence; that evidence does
not establish Linux cross-process support.

Storage issue #14 currently owns changes to read-only activation and I/O guards.
Integrate its verified commit before final combined runtime validation. Do not
copy or independently rewrite those guards while that work is in progress.
