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
- `core/mvcc/database/shared_idle_reload.rs`: current-base inspection/evidence and resumable local reload ownership.
- `core/mvcc/database/shared_generation_reload.rs`: distinct local-only installation entrypoint sharing preparation/publication mechanics with the existing completed-checkpoint installer without weakening that installer's proof requirements.
- `core/mvcc/database/shared_runtime.rs`, `shared_refresh.rs`, and `core/database/shared_connection.rs`: normal admission and constructor handoff integration, preserving ordinary paths and platform guards.

Use pinned Rust1.88, two jobs, debug disabled, no incremental compilation. Native selector for the initial counterexample is `shared_idle_generation_native_reloads_after_checkpoint_and_later_commits` with `--ignored --test-threads=1 --nocapture`; exactly one parent test should execute. The helper runs only by its full exact test path in a subprocess. Store exact source SHA and raw baseline/final/control logs before claiming success.

### Task 1: Genuine cross-process counterexample

**Blocked by:** Verified combined ready-handoff base.

- [ ] Start an old-generation child process and warm its idle Store before the parent checkpoint.
- [ ] Run the actual completed checkpoint and verified private ready handoff.
- [ ] Commit twice afterward; assert the empty completion record has been overwritten and latest selected prefix is nonzero.
- [ ] Keep a foreign new-generation reader active while the old child tries to refresh.
- [ ] Observe exact baseline failure before changing production refresh code; preserve native ownership and durable-byte assertions.

### Task 2: Selected current-base evidence and local installation

**Blocked by:** Task 1.

- [ ] Add a read-only selected active-ID inspection path without global snapshot eviction.
- [ ] Bind current physical base evidence to the selected normal manifest, active ID database generation/checkpoint timestamp, actual retained files and continuous authority.
- [ ] Add a distinct local installation seam, preserving the existing completed-checkpoint installation preconditions.
- [ ] Check zero local transactions and local mutation/GC exclusion, validate preparation freshness, and keep all allocation before publication.
- [ ] Cover damaged metadata, mismatched physical timestamp/generation, stale preparation and local active transactions without publishing readiness.

### Task 3: Admission/replay integration and cancellation

**Blocked by:** Task 2.

- [ ] Drive local reload before ordinary shared snapshot registration on a generation mismatch, including new constructors where necessary.
- [ ] Replay the selected bounded current prefix from the validated physical base; publish matching local schema/storage/replay/pager readiness only after success.
- [ ] Preserve global prefix and foreign snapshot registrations; prove old reader view remains valid.
- [ ] Cancel before/after physical read callbacks and after partial local mutation; retain backend authority and retry from fresh durable selection.
- [ ] Prove the genuine counterexample passes and removal of the new reload path fails it again.

### Task 4: Verification and review

**Blocked by:** Tasks 1-3.

- [ ] Run focused native old-peer/nonzero-prefix/foreign-reader tests and causal controls on exact pinned source.
- [ ] Run combined IDs, watermark, saved-outer-transaction and generation suites.
- [ ] Run appropriate ordinary/core regressions, strict Rust1.88 library lint and formatting; verify platform cfg boundaries.
- [ ] Obtain independent whole-component review and push the verified component milestone, keeping public activation disabled.
