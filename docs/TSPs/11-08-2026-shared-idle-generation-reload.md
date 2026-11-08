# Idle shared Store generation reload Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task by task.

**Goal:** Let an idle process rebuild its local Store/replay state from a newer completed physical database and its current acknowledged logical prefix, including when later commits have overwritten the empty completion record. Preserve readers already using that newer generation.

**Architecture:** Hold normal admission and the freshly selected publication authority. Read and validate the current physical base and selected active ID bank through retained read-only files. Produce distinct local-base evidence for a local-only Store installation; do not manufacture or reuse a CompletedCheckpointProof. Install only with no local transactions, then replay the selected bounded prefix before publishing local readiness. Foreign snapshots are preserved.

**Source PRD:** docs/PRDs/11-06-2026-independent-php-worker-concurrency.md.

**Starting component:** verified pager milestone 91c315fdcc86f131251878bdaa251bc0b0fe528a. Implementation base is c41249e5f39e0938c8f163666b04ceb33cae7b9e, the checkpoint owner's verified ready-handoff union including current replay-descriptor and ID/watermark milestones. Native readiness run 42478500793 passed four ready cases, both causal removal controls, restored cases, broad MVCC and strict lint before this work starts.

## Constraints

- This is local Store/replay refresh, not checkpoint recovery or physical publication.
- Fresh selection must be a normal record, without pending checkpoint or downgrade.
- Use the selected active ID generation/checkpoint timestamp and actual retained physical inspection. Never derive the physical timestamp from a later logical commit timestamp.
- Preserve global published prefix and foreign native snapshot registrations. Do not require or clear global snapshot quiescence for local-only installation.
- A genuine checkpoint is still quiescent and uses the existing completed proof/finalized ID/ready-handoff protocol.
- Local installation requires zero local transactions and existing local checkpoint/GC exclusion. Allocate preparation before publishing clocks/maps.
- Keep runtime context unavailable across partial mutation or cancellation. A retry must reselect durable state and rebuild safely.
- Physical reads retain authority through actual backend completion release, including cancellation after callback.
- Public mode remains off. Fork source pushes are authorized; no PR8 update until verified union.
- Root owns concurrency target; performance owns review-split target. This component uses hosted Linux validation unless a local target is explicitly reserved.

## File structure

- `core/mvcc/database/shared_idle_generation_tests.rs`: genuine subprocess fixture and old/new reader counterexample; declared by the existing runtime test module.
- `core/mvcc/shared_ids/current_generation.rs`: read-only active-bank selection and validation, using existing ID codecs/load infrastructure; no native snapshot eviction.
- `core/mvcc/database/shared_current_base.rs`: retained physical inspection and sealed current-base evidence.
- `core/mvcc/database/shared_current_prefix.rs`: shared admission, preparation and constructor driver, preserving private replay until completion.
- `core/mvcc/database/shared_generation_reload.rs`: distinct local-only installation entrypoint sharing preparation/publication mechanics with the existing completed-checkpoint installer without weakening that installer's proof requirements.
- `core/mvcc/database/shared_runtime.rs`, `shared_refresh.rs`, and `core/database/shared_connection.rs`: normal admission and constructor handoff integration, preserving ordinary paths and platform guards.

Use pinned Rust1.88, two jobs, debug disabled, no incremental compilation. Native selector for the initial counterexample is `shared_idle_generation_native_reloads_after_checkpoint_and_later_commits` with `--ignored --test-threads=1 --nocapture`; exactly one parent test should execute. The helper runs only by its full exact test path in a subprocess. Store exact source SHA and raw baseline/final/control logs before claiming success.

### Task 1: Genuine cross-process counterexample

**Blocked by:** Verified combined ready-handoff base.

- [x] Start an old-generation child process and warm its idle Store before the parent checkpoint.
- [x] Run the actual completed checkpoint and verified private ready handoff.
- [x] Commit twice afterward; assert the empty completion record has been overwritten and latest selected prefix is nonzero.
- [x] Keep a foreign new-generation reader active while the old child tries to refresh.
- [x] Observe exact baseline failure before changing production refresh code; preserve native ownership and durable-byte assertions.

### Task 2: Selected current-base evidence and local installation

**Blocked by:** Task 1.

- [x] Add a read-only selected active-ID inspection path without global snapshot eviction.
- [x] Bind current physical base evidence to the selected normal manifest, active ID database generation/checkpoint timestamp, actual retained files and continuous authority.
- [x] Add a distinct local installation seam, preserving the existing completed-checkpoint installation preconditions.
- [x] Check zero local transactions and local mutation/GC exclusion, validate preparation freshness, and keep all allocation before publication.
- [x] Cover damaged metadata, mismatched physical timestamp/generation, stale preparation and local active transactions without publishing readiness.

### Task 3: Admission/replay integration and cancellation

**Blocked by:** Task 2.

- [x] Drive local reload before ordinary shared snapshot registration on a generation mismatch, including new constructors where necessary.
- [x] Replay the selected bounded current prefix from the validated physical base; publish matching local schema/storage/replay/pager readiness only after success.
- [x] Preserve global prefix and foreign snapshot registrations; prove old reader view remains valid.
- [x] Cancel before/after physical read callbacks and after partial local mutation; retain backend authority and retry from fresh durable selection.
- [x] Prove the genuine counterexample passes and removal of the new reload path fails it again.

### Task 4: Verification and review

**Blocked by:** Tasks 1-3.

- [x] Run focused native old-peer/nonzero-prefix/foreign-reader tests and causal controls on exact pinned source.
- [x] Run combined IDs, watermark, saved-outer-transaction and generation suites.
- [x] Run appropriate ordinary/core regressions, strict Rust1.88 library lint and formatting; verify platform cfg boundaries.
- [x] Obtain independent whole-component review and push the verified component milestone, keeping public activation disabled.


## Verified component

Engine source `3942c92fc97654b389541f49821cb7623fcf4a27` includes the published `a3fa3709e9916353c2af44f82786773c66dd56a9` lifecycle, budget-fixture and oracle-layout dependencies. The merge was clean; all fifteen incoming changed files remained byte-identical to the published dependency. The final component received independent source review, including owner validation before stable replay and preservation of caller-defined table functions before private schema replacement.

The genuine baseline at `9459876e3abcfc382511d7b2a34fb63d4a810d51` failed the old-process read with an incompatible generation error in native run [42480322978](https://github.com/HundredBillion/turso/actions/runs/42480322978). It used a real finalized checkpoint and ready handoff, two later commits replacing both empty completion records, and an active foreign new-generation reader.

Final native and platform validation:

- [42487910358](https://github.com/HundredBillion/turso/actions/runs/42487910358): Linux full core **2,900 passed, 0 failed, 113 ignored**; broad MVCC **856 passed, 0 failed, 65 ignored**; strict Rust 1.88 library Clippy, formatting and source integrity; Windows library check with `bench,conn_raw_api`.
- The same run passed the old connection/new constructor cases, partial logical replay cancellation, physical header/page-one cancellation before and after callback, final-release failure/retry, and rejection of a wrong Store or connection before replay and on resume. The final partial-replay trace was physical timestamp **10**, partial private frontier **12**, original selected frontier **15**, and freshly selected retry frontier **17**. Allocation clocks never decreased; foreign snapshots and durable bytes remained unchanged.
- Current-ID inspection **4**, physical inspection **3**, closed local installation **2**, existing generation controls **14**, ID protocol **79 passed / 3 ignored**, native constructor controls **6**, and completed-checkpoint ready handoff **4** passed.
- [42488559703](https://github.com/HundredBillion/turso/actions/runs/42488559703): five separate source-removal controls failed their intended assertion with exactly **0 passed / 1 failed**, then every restored case passed. These cover Store ownership, source-only function preservation, retry from a lower physical base, conservative physical-file ID bounds, and readiness withdrawal after failed authority release. Raw control logs are preserved in artifact `idle-generation-controls-3942`.
- [42489397857](https://github.com/HundredBillion/turso/actions/runs/42489397857): the original counterexample passed, disabling the new-generation reload branch returned `Busy` instead of the acknowledged row (**0 passed / 1 failed**), and restoring the exact source passed again. This confirms the reload path remains necessary with the merged dependencies.
- [42489084990](https://github.com/HundredBillion/turso/actions/runs/42489084990): native ID opcodes **13**, deferred watermark I/O **1** (30 internal cases), scoped lifecycle **8**, lifecycle I/O **2** including saved outer transactions, preparation **3** plus preparation I/O **1**, stale preparation **1**, watermark identity **1**, and schema publication **3** (16 internal cases).

An earlier validation script allowed `tee` to hide two full-core failures in old 128 KiB budget fixtures. Those runs are not full-core success evidence. The final source uses the already verified 192 KiB fixtures from the published dependency, and final validation uses explicit Bash with `set -euo pipefail`; the full parent result was read directly. Artifact glob quoting was also corrected before preserving the raw control logs.

Public activation remains disabled. Generation changes for attached database IDs remain rejected; the implemented reload is for the main shared database. The existing completed-checkpoint proof and its global-quiescence requirements remain unchanged. Integration with the separate fresh-confirmation/recovery component must preserve both its known-identity loader and the current-generation loader target.
