# Shared committed-ID oracle

This is the private adapter required by the approved shared-ID reservation protocol. It does not enable a public mode or establish full PHP-worker acceptance.

## Owned seams

The component owns `mvcc/read_pinned_file.rs`, a narrow per-instance WAL read-file override, `database/shared_oracle.rs`, and the eventual `mvcc/database/shared_id_oracle.rs` adapter. The runtime owner supplies the checked temporary transaction constructor and exact refresh proof; engine/VM call sites remain owner-owned.

The temporary read transaction must see the latest imported committed prefix, excluding the requester's older snapshot and local writes. It uses a local unique TxID with begin timestamp equal to checked `prefix.committed_timestamp + 1`. No extra global reader slot is consumed: continuously held publication authority already excludes checkpoint/publication, and the local transaction protects local GC. Unresolved local Preparing transactions require recovery/BUSY before construction because ordinary MVCC cursors intentionally support speculative reads. Cleanup must not clear the requester's connection transaction or schema.

## Physical read lifetime

`ReadPinnedFile` captures an authority pin inside each physical read completion's callback before submission. The pin survives caller cancellation, callback invocation, and operation drop until the backend destroys its last physical completion reference. The wrapper is read-only and preserves descriptor identity. Pin/submission errors abort the caller completion so a dedicated pager cannot retain locked pending pages indefinitely.

A completion group alone is insufficient: completion/abort can detach a child's group reference before the backend releases that child. The pin therefore belongs to the physical callback, as it does for guarded mutations.

A private WAL instance can install an identity-checked read-file override. All five data-read paths use it: individual frames, batches, raw frames, existing-frame conflict validation, and checkpoint payload reads. Identity is checked before destination pages are locked, and a later coordination identity change fails closed. Other WAL instances retain their ordinary files and behavior.

The dedicated oracle pager validates the supplied main and WAL handles against the runtime's retained identities, copies the initialized plain-page layout, and owns independent page-cache and read-mark state. The runtime must supply the pinned wrappers and a source pager from the same database. Shared mode excludes page codecs/encryption; the factory rejects them explicitly rather than guessing a transform context. This factory does not register a snapshot or alter the outer connection.

## Oracle facts

Use `MvccLazyCursor::new` with the temporary explicit TxID. Exact rowid seek supplies RowExists; the committed cursor's last row supplies the maximum. Resolve domain names and roots from committed schema rather than the outer connection schema or speculative root-map entries.

MVCC AUTOINCREMENT follows the existing implicit sequence allocation path (`translate/insert.rs`), with sqlite_sequence synchronized as a mirror. Preserve `ClaimSequence(value)` around that assigned cursor value; do not substitute `ReserveRow { autoincrement: true }` using only the mirror. The private policy's AUTOINCREMENT-floor seam needs authoritative backing-sequence reconciliation if a future runtime path actually uses it. In the ordinary non-MVCC fallback, sqlite_sequence uses the first matching row in rowid order, NOCASE name comparison, and AddImm integer coercion (`vdbe::execute::extract_int_value`); those rules do not make it the MVCC cursor. The logical floor combines committed schema/root bounds with the separately durable reservation floor.

## Validation sequence

1. Make the physical-retirement regression fail with a passthrough read, then capture the pin and test success, caller abort, backend submission error, stale lease, and mutation rejection.
2. Exercise all WAL read paths through the override; verify ordinary peers do not inherit it, wrong identities fail, and rejection does not leave pages locked. Run a causal routing control.
3. Run the dedicated pager's retained-identity/read-state test on native Linux; keep this distinct from the portable wrapper tests.
4. After the runtime constructor interface is reviewed, test actual committed oracle facts against old outer snapshots, local insert/delete/DDL, unresolved Preparing state, AUTOINCREMENT duplicate/case/type cases, and delayed main/WAL reads. Verify cleanup leaves the outer transaction unchanged.
5. Integrate the actual reservation call sites only after the facts and I/O retirement contract pass. Cross-generation checkpoint finalization still requires the coordinator's opaque durable proof.

## Cancellation argument and current evidence

On oracle cancellation, drop the dedicated cursor before removing the temporary
transaction. Physical main/WAL callbacks retain buffers and PageRefs, not cursor
or version-chain state. The dedicated pager is never reused by another oracle.
Its read mark may therefore end during cleanup: callback-captured authority pins
still prevent global checkpoint/materialization until all physical reads retire,
and late page results have no consumer. No extra reader-scope cycle is necessary.
This argument depends on actual runtime checkpoint admission obeying authority;
the final adapter cancellation test must also verify the outer transaction remains
untouched.

The wrapper regression failed with a passthrough pread because a peer acquired
authority while the backend retained the physical read. Capturing a pin in the
physical callback made both wrapper tests pass. A routing negative control made
WAL reads bypass the configured override; the five-read assertion failed with
zero instead of five. Restoring routing made the same test pass. The native pager
fixture deliberately uses raw retained handles to isolate layout/identity and
read-state validation; it does not prove end-to-end pinned oracle construction.
The runtime must validate that the supplied guard belongs to its own authority
before constructing either read wrapper.

The factory copies source page size and reserved-byte count, selects WAL presence
from `source.has_wal()`, and disables checksum interpretation when reserved bytes
do not match the checksum layout. It rejects uninitialized layout and encoded
pages. Codec/encryption support is outside the already approved shared-mode scope.

Portable validation: all 100 WAL tests and both read-pin tests pass. The factory's
native test is Linux-only and explicitly ignored pending hosted execution; it was
not exercised by those portable filters. The source pager's generation, layout,
and WAL presence must match the coordinator's freshly validated runtime context;
handle identity alone does not establish that provenance for an arbitrary pager.

The factory also rejects a source pager whose SharedPagerState identity differs
from the database's state, even if page size and reserved bytes match. A narrow
Pager getter compares the retained Arc identity; no path lookup or test-only WAL
downcast is used. Refreshed generation and correct authority remain runtime proofs.

Test clippy passes with allowances for existing unused imports, unfulfilled lint
expectations, type complexity, redundant clones, and detached futures. An initial
strict library invocation stopped on the pre-existing JSON cache's unfulfilled
`new_without_default` expectation; no source allowance was added there. Changed
Rust files pass formatting and whitespace checks.
