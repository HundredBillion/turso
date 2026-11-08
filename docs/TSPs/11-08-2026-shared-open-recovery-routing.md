# Recover completed checkpoints before shared open allocates a Store

Status: proposed implementation seam for parent review. Core Ready is independently accepted at222 and the complete C/helper release at774d17d835cc9ad9d656f30f61a67d604f26c2bb (runtime90, native42516139888 and42517774139). This branch preserves that release. The next verified parent union is f4c3aa8604f87e33cccf9d650cdb0e1fc095654c (e35/Core/C plus accepted OFD); merge it before production and record the actual merged SHA. Do not import unaccepted experiments.

## Outcome

A normal shared Core open that encounters a genuinely completed checkpoint with actual matching unfinished target IDs must drive the existing real recovery/Ready chain before constructing any Store. The returned Database must work through its normal subsequent connect call. A healthy shared open with live readers continues through ordinary admission and existing bootstrap without requiring global quiescence. Public C/PDO URI activation is unchanged and remains disabled until the full intent/crash integration is accepted.

## Current source and failure

- Database::open_async delegates shared options directly to open_shared_async; this path currently bypasses the ordinary DATABASE_MANAGER registry. Preserve that behavior in this slice rather than invent a second registry protocol.
- open_shared_async creates Runtime, immediately begins a read-pinned ordinary open and polls only its manifest. A Normal record does not reveal prepared target IDs. do_open_async then constructs/attaches a Store during header validation.
- bootstrap_shared_nonblock eventually reads IDs, rejects has_pending_checkpoint, and aborts. The Runtime is already permanently Attached, so the fresh-only recovered constructor cannot subsequently repair that same Runtime. Retrying on an error string or adopting an arbitrary attached Store is forbidden.
- The accepted normal recovery chain already consumes a real paused fresh selection, actual physical base/IDs, two durable normal records, epoch/finalization, logical reclamation and exactly-one-Store construction, then certifies and returns actual Ready.
- Connection::Drop performs no automatic checkpoint; an unused recovered connection has no transactions or pending I/O after certification. Drop it after Ready when the public open needs only its Database. Do not call close merely to discard this connection, because close can checkpoint.

## Routing is read-only classification, not recovery evidence

Add a narrow existing-codec selector inspection under the original ordinary admission and publication authority, before any Store allocation. Implement it inside shared_ids/io_protocol as a private child, reading the actual retained selector only (two fixed pages). Reuse select_witness for CRC, sequence, database identity and prepared-transition validation. Create a read-only selected-manifest view of the actual retained selector and wait for backend retirement, including after callback. GuardedFile validates its original authority association, then delegates the existing ReadPinnedFile normal-manifest constructor; do not modify the selector binding slot, because dropping/rebinding under the same still-active lease is rejected and would break subsequent ID bootstrap/allocation. Do not read whole ID banks just to classify every healthy open; full bank/journal validation remains in the existing actual bootstrap/recovery paths.

The constructor accepts the actual retained SharedIds and actual LoadedManifest; no arbitrary File/record/timestamp or caller-authored routing flags. The output carries only the original manifest/guard and a sealed route classification. It never clears mapped recovery, mirrors metadata, mutates banks, returns LoadedIds or establishes Ready.

For a Normal record:

- Healthy: finalized active selector matches the selected generation, has no prepared bank, and its actual checkpoint timestamp is bounded by the committed prefix. At an empty prefix require the timestamp to match that target. A nonzero prefix with ordinary ID journal changes is healthy; differing selector slots alone are not a checkpoint-recovery signal.
- Completed recovery candidate: the actual empty Normal target has a matching prepared target bank. Determine the prepared target from real encoded fields, never target-minus-one or a timestamp-only proof.
- Unrelated/mismatched prepared identity or inconsistent selector/manifest fields fail closed before Store construction. Do not reinterpret malformed metadata as a recovery request.
- Manifest INTENT is a distinct typed decision reserved for the intent successor. Terminal downgrade remains a separate explicit route/error. No error-message parsing.

Tail bytes alone also never trigger recovery: an acknowledged prefix of zero ignores complete, corrupt or torn unacknowledged bytes, including healthy read-only opens. Do not truncate that tail or classify from its size. Normal mapped recovery-required/incarnation flags alone do not force global checkpoint recovery: ordinary bounded bootstrap can recover healthy state while other live readers remain valid. Initial/healthy empty-prefix namespaces with finalized IDs stay ordinary regardless of bytes beyond the acknowledged endpoint.

## Ordinary versus paused ownership

Introduce a sealed yielding initial-open driver and outcome:

```rust
pub(crate) enum SharedInitialOpenResult {
    Healthy(SharedOpenState),
    Recovered(Arc<Database>),
}
impl SharedMvccRuntime {
    pub(crate) fn begin_initial_open(
        self: &Arc<Self>, options: OpenOptions,
    ) -> Result<SharedInitialOpen>;
}
impl SharedInitialOpen {
    pub(crate) fn poll(&mut self) -> IOResultOr<SharedInitialOpenResult>;
}
```

Keep its dependent recovery-stage implementation in a private child under the normal recovery/bootstrap module, where existing sealed stage types are accessible without exporting generic intermediate capabilities. Re-export only this driver/outcome along the existing crate-private boundary. The selector probe stays in the ID module. Add one optional driver field to OpenDbAsyncState and narrow open_shared_async wiring; do not modify the other agent's checkpoint or intent-materializer code.

1. Acquire ordinary admission before publication authority, load the actual manifest and inspect the actual selector. No snapshots quiescence, Store allocation, mapped recovery or mutable I/O occurs here.
2. Healthy selection transfers the SAME original admission/manifest/guard into a read-pinned SharedOpenState (using a small from-loaded helper, with existing main/WAL read bindings). Continue the current do_open_async/bootstrap path. The guard remains continuous through that bootstrap, so IDs cannot become prepared between classification and attachment.
3. A completed candidate first waits for all probe I/O to retire, then drops classification admission/authority. This is only a routing hint, so releasing it does not discard a proof. Originally content-readonly Runtime rejects clearly before any recovery mutation/Store allocation.
4. Acquire admission pause BEFORE authority and freshly load/classify the actual manifest/selector under that new lease. Do NOT call the existing explicit begin_checkpoint_recovery before reclassification, because it checks snapshot quiescence too early for a candidate that another worker has already completed.
5. If the fresh paused classification is now healthy, release pause/guard after retirement and restart ordinary admission/classification with a bounded restart budget. Never acquire admission while already holding authority or treat an old classification as readiness. If contention/races exhaust the budget, return bounded BUSY without mutation.
6. Only a fresh actual completed candidate checks native snapshot quiescence and becomes the existing sealed CheckpointRecoverySelection with that SAME actual manifest/guard/pause. From here the lease/pause remain continuous through existing physical inspection, IDs, confirmation, finalization, reclamation and Ready. Use the original frozen caller options; capture RecoveryOpenConfig once at the existing physical-reader boundary. No fresh Store is attached before reclamation/reservation.
7. Consume actual Ready, drop its unused private connection (no transactions or I/O), retain the actual Database, and return it directly through shared open. Set the outer state consistently to completed/actual db so later polls cannot allocate another Store. A subsequent ordinary Database.connect must initialize/certify its own private Pager through the existing constructor.

The current request path/io/options identity checks remain. Errors/cancellation discard the actual owning stage before Runtime/request fields and leave no replay readiness. Once attached, the old Runtime remains permanently claimed; retry after complete cancellation creates a genuinely new Runtime rather than resetting its claim. Backend completions retain native authority after callbacks and whole-state cancellation as in the accepted existing modules.

INTENT later enters the same fresh paused-selection decision as a typed successor owned by11558. Its SOURCE Store continuation must remain Owned all the way to final Ready; this driver must not relabel it Fresh, construct a second Store, recapture a different config Arc, or relax the existing generic attached-Store rejection.

## Task 1: genuine public-open counterexample and routing probe

- [ ] Add an actual completed-checkpoint fixture retaining actual pending IDs, then discard every old Store/Database/Runtime/proof/guard/pause before calling Database::open with shared options. First SQL uses a new normal connect call and must see the acknowledged completed row and commit another update. Observe the current exact missing-public-recovery failure natively before production.
- [ ] Add healthy empty/nonzero-prefix public-open cases with another live reader (same process and real peer process where existing harness fits). They must open/connect without checkpoint pause or forcing the reader out. Also verify complete/corrupt/torn unacknowledged tails under an empty prefix are ignored without content changes by read-only opens, including another live reader. Test typed mismatched selector/manifest rejection.
- [ ] Implement the bounded actual two-slot read-only selector probe with original guard/native file identity and callback/retirement ownership; no LoadedIds escape or global quiescence. Obtain parent source review.

## Task 2: pre-Store shared-open driver and full normal recovery

- [ ] Implement ordinary healthy transfer and paused fresh reclassification, bounded healthy restart, original-readonly refusal, and explicit normal/intent/downgrade decisions before Store construction. Do not route by mapped recovery flags or error strings.
- [ ] Drive the accepted actual normal chain to Ready, drop the unused private connection safely, return actual Database through open_shared_async and verify normal connect/post-open SELECT/UPDATE/INSERT/commit plus independent peer observation.
- [ ] Test matching prepared IDs and healthy finalized IDs with ignored unacknowledged tail bytes, both selector slot parities, captured caller options/native extension/readonly behavior, and no second Store claim. The normal public C URI remains disabled in this slice.

## Task 3: races, cancellation and integration

- [ ] Exercise candidate-to-healthy reclassification when another legitimate participant finishes and commits after the initial probe; use actual state transitions/I/O boundaries, not fake records or proof flags.
- [ ] Inject selector/manifest callback/submission errors and cancellation before callback and after callback before backend retirement. No Store/replay/commit may escape early; native authority remains BUSY until actual retirement. Exercise cancellation during actual subsequent recovery with the already accepted transport fixtures.
- [ ] Causal controls: bypass candidate routing (genuine public counterexample fails), force healthy into quiescent recovery (live-reader case fails), omit read-retirement/actual reselection (the corresponding real race/ownership test fails). Restore exact source before positive/lint runs.
- [ ] Run targeted native public-open/probe/normal-recovery/constructor/ID/watermark cohorts, selected Core/C/SDK/Windows checks appropriate to the final parent union, strict lint/fmt and independent whole-source review. Preserve the accepted Ready/C release and integrate on a separate branch. Public PDO/URI activation and the SOURCE/ACK intent successor require their separate final acceptance, not a test-helper-only success claim.


Parent correction: neither nonempty logical-file length nor invalid bytes after acknowledged end is durable checkpoint evidence. The first draft proposed tail-only recovery; that is rejected and removed before production. Matching prepared target IDs are the initial Normal signal; INTENT is a later separate typed authoritative branch. Tests explicitly preserve readonly/live-reader opens with complete, corrupt and torn tails at endpoint zero.


Baseline exact merged source94f425dd9dfc866aad01a11049869d67d413588e (contains parentf4) native42520094911 SUCCESS: healthy live-reader1 and readonly complete/corrupt/torn-tail1 PASS, genuine pending-ID public-open0/1RED at current bootstrap diagnostic, strict Core tests Clippy1m26s/fmt/integrity PASS. Local union libcheck9.97s. No production routing was present for this observed counterexample. Parent approved continuing the source-audited probe/driver seam and requested explicit real table/row-ID allocation after healthy probe; added to the healthy case.
