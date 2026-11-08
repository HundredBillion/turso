# Shared pager generation and connection admission Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent a shared-mode connection from using cached pages from another completed physical generation, including connections initialized while checkpoint admission is paused.

**Architecture:** Keep the physical generation tag on each Pager, including aliases shared by private helper connections. Capture a normal durable manifest under admission/publication ownership before shared constructor I/O; retain physical read callbacks through backend retirement. Validate or reset a pager under the selected authority before registering a snapshot. Ordinary connection paths retain their existing behavior.

**Tech Stack:** Rust 1.88, existing native Linux OFD authority/admission, GuardedFile read pins, existing nonblocking pager initializer and genuine completed-checkpoint proofs.

## Global Constraints

- Source PRD: docs/PRDs/11-06-2026-independent-php-worker-concurrency.md. User-approved quiescent checkpoint and offline-upgrade policies remain unchanged.
- Base: 20248792d7f34a67e450876e49d47538f9c641c4, combining verified staging7df8 with generationebadc.
- Start verification on supported 64-bit Linux and local filesystems; do not claim other platforms or PDO acceptance.
- No public mode activation, fabricated physical/completion proof, or admission release from the incomplete checkpoint tail.
- Preserve SharedPagerState layout/initialization data. Reset only idle connection-private state; reject active, dirty, or unfinished pager operations.
- Never assign a constructor pager whichever generation happens to be current after unprotected I/O. Same-generation logical commits do not change the physical base.
- Parent owns sequence-replay descriptor changes. Agent11554 owns staged physical base/replay/FinalizedIds checkpoint tail. This component owns Pager, Database constructor, pre-snapshot check, and narrowly coordinated bootstrap certification.
- At most two heavy builds, two jobs each; coordinate local target ownership. Use isolated source and validation branches with exact SHA/count checks.

### Task 1: Shared constructor admission and physical read ownership

**Blocked by:** None

**Files:** core/database.rs; new core/database/shared_connection.rs; core/mvcc/database/shared_runtime.rs; new private runtime connection child; native connection test child.

**Interfaces:** Shared-only yielding constructor state acquires a read-pinning SharedOpenState before `_init_nonblock`. Existing ordinary `_connect` and `connect_async` behavior stays unchanged. Initial shared Database open also uses read pins before raw-header I/O. State drop retains actual backend authority through pending completion captures.

- [x] Add native bounded-connect-during-pause regression through existing connection APIs; observe RED without constructor admission.
- [x] Add shared-only cooperative constructor state and capture the selected manifest generation before any header read.
- [x] Use GuardedFile read-pin bindings only for initial shared open and constructors; retain existing ordinary fresh-transaction binding policy.
- [x] Verify pending initial-open and new-connection header cancellation before callback and after callback/before retirement. Competing authority must remain Busy until retirement; retry succeeds afterward.
- [x] Add focused causal removal of read pinning, observe the exact early-authority-release failure, restore and verify.

### Task 2: Pager-owned physical generation

**Blocked by:** Task 1

**Files:** core/storage/pager.rs; private runtime connection child; pager/constructor native test child.

**Interfaces:** Pager tag stores physical database identity and durable generation/checkpoint base together. It starts unknown; aliases observe one tag. Runtime certification validates the selected normal record against the certified replay physical base before setting a constructor tag. Do not derive checkpoint base from a later committed prefix.

- [x] Add an old-generation idle-pager regression using genuine completed checkpoint publication.
- [x] Add private reset-and-tag operations that preserve SharedPagerState and reject active/dirty aliases without changing their transaction state.
- [x] Test a helper sharing an active Arc<Pager>: return Busy without reset or outer-transaction loss.
- [x] Verify tag assignment follows captured authority and cannot label an old in-flight constructor as a later generation.

### Task 3: Admission and checkpoint-tail certification

**Blocked by:** Tasks 1 and 2

**Files:** core/mvcc/database/shared_refresh.rs; core/mvcc/database/shared_runtime.rs; private runtime connection child.

**Interfaces:** SharedRefresh::poll_begin checks the actual pager tag after refreshed selection and before snapshot registration. Bootstrap certification occurs only after real base/replay/ID validation. `SharedMvccRuntime::certify_completed_pager(connection, proof, guard) -> Result<()>` works while replay is None, validates the original completed target authority and source pager ownership, resets idle private state, and sets the target tag. It publishes no replay state and releases no authority or admission pause.

- [x] Wire the pre-snapshot pager check; retain existing schema-generation/reprepare guards.
- [x] Certify initial bootstrap under its validated ownership before promotion/post-bootstrap queries.
- [x] Expose the completed-proof certification seam to the independently owned tail; late failure remains fail-closed.
- [x] Verify cancellation, retry, foreign source/authority and active-pager rejection without constructing opaque proof values in tests.

### Task 4: Combined validation and independent review

**Blocked by:** Tasks 1-3

- [x] Run focused portable pager/generation/constructor tests and ordinary connection regressions on pinned Rust1.88.
- [x] Run genuine native initial-open/constructor/idle-generation tests with exact counts and causal controls.
- [x] Run shared runtime/admission suites and strict library Clippy with bench,conn_raw_api; format and inspect diff.
- [x] Obtain independent whole-component review; integrate only tested source commits. Never merge validation workflows or activate public shared mode.


## Verified scope and evidence

Implementation source ef7a2adfbf243f1a43c8b2830c79e5aeb40884ac passed native Linux run 42434445275: six constructor/admission/cancellation tests, one genuine completed-proof pager certification test, fourteen generation tests, strict Rust1.88 library Clippy with bench,conn_raw_api, and formatting. Production-identical 97283c368 passed native run 42434201098 with two causal controls: removing generation reset failed both stale-cache and active-alias tests; omitting the completed target tag failed the genuine proof test. Restored exact source passed again. Local full core library validation passed 2816 tests, with 40 ignored.

Initial-open and cached-constructor physical-header cancellation tests retain actual backend completions before callback and after callback. Both native ownership exclusion and direct retry after release are checked. Removing read ownership and removing InitState read-lock cleanup each produced the intended native failure before exact-source restoration passed. Constructor tests also distinguish the physical checkpoint timestamp from a later same-generation logical commit timestamp.

The completed certifier checks incarnation readiness; the checkpoint tail separately supplies finalized ID-selector ownership through FinalizedIds. A cert-only test may retain prepared IDs because it publishes no replay or admission readiness.

This component checks private pager state after a local Store/replay installation. Reloading an idle process whose Store/replay still belongs to an earlier generation remains a separate integration gate, including when later commits have overwritten the empty completed record. Generation mismatch remains closed to admission; this component manufactures no completed proof and rewinds no published prefix. Public shared mode remains disabled.


Root independently reviewed the whole production component through ef7a2 with no blocking finding. Subsequent portability review found two Unix cfg boundaries: the new snapshot hook and the existing native completion method referencing MaterializedCheckpointProof. Source 0b37ca84557e4bd2f1b34c67cc50c263e7a47840 adds matching guards; native Windows Rust1.88 run 42435377822 passed the library typecheck, independent removal controls producing E0609 and E0412, restored typecheck, and formatting. The follow-up cfg changes received independent review from the checkpoint-tail owner. Unix behavior remains the same as the native-tested implementation.

The final union must still verify the combined ID, watermark and saved-outer-transaction suites after merging the checkpoint tail. Component validation does not replace that integration gate or enable public shared mode.
