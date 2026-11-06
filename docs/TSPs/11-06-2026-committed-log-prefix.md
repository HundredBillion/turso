# Committed logical-log prefix technical spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Supply strict, bounded frame decoding for the approved cross-process MVCC replay protocol.

**Architecture:** Extend the existing logical-log reader with an optional immutable committed boundary. Preserve the current tolerant recovery behavior when no boundary is supplied. This is one independently reviewable part of protocol stage B, not the completed cross-process mode.

**Tech Stack:** Rust, existing asynchronous `File`/`IO` traits, `MemoryIO` and delayed short-read fixtures.

## Global constraints

- Preserve existing formats, default recovery behavior and MVCC/multiprocess rejection.
- Do not read or expose frames beyond the supplied committed boundary.
- An invalid or incomplete frame inside that boundary is corruption, never ordinary end of log.
- Verify the ending running CRC before returning the boundary's final frame.
- The caller remains responsible for retaining the captured log generation while reading.

## Task 1: Restrict the reader to the committed prefix

**Blocked by:** None

**Files:**
- Modify and test: `core/mvcc/persistent_storage/logical_log.rs`

**Interfaces:**
- Consumes: the existing log writer's `offset`/`running_crc` pair and `StreamingLogicalLogReader::next_frame()`.
- Produces: `StreamingLogicalLogReader::with_committed_prefix(self, end_offset: u64, running_crc: u32) -> Result<Self>`.

- [ ] Add a test that writes a first frame, captures `(end_offset, running_crc)`, writes a second frame and requires exactly the first frame to be returned. Repeat EOF twice and assert the reader's final offset and CRC match the captured prefix. The fixture uses `MemoryIO`, `LogicalLog::log_tx`, `LogRecord::for_test` and `make_test_row_version`, all existing helpers in this test module.
- [ ] Run `CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p turso_core committed_prefix --lib` and observe the reader exposing the second frame before adding the restriction.
- [ ] Add optional `(usize, u32)` prefix state. The builder rejects offsets below `LOG_HDR_SIZE`, offsets not representable as `usize`, offsets beyond current file size and readers that already began reading. A common file-size refresh uses the captured end instead of later file growth; a physical size below the end is corruption. All existing file-size refresh sites use this helper.
- [ ] At a complete frame ending at the prefix, compare its CRC before returning it. At EOF require both exact end position and matching CRC. Apply the same checks to recovery-frame, test-record and portable-frame entry points so none bypass the boundary.
- [ ] Rerun the test and temporarily remove the builder call to verify the assertion fails for the original unbounded behavior; restore it and rerun.
- [ ] Run existing logical-log tests, formatting and `git diff --check`, then commit the tested implementation.

## Task 2: Validate failure and repeat paths

**Blocked by:** Task 1

**Files:**
- Modify and test: `core/mvcc/persistent_storage/logical_log.rs`

**Interfaces:**
- Consumes: Task 1's builder and all reader entry points.
- Produces: regression coverage for the strict boundary; no additional public API.

- [ ] Add one failing test at a time for a cut-through-frame boundary, wrong ending CRC, physical truncation after construction, malformed committed trailer and header-only prefix.
- [ ] Confirm the malformed tail remains tolerated by the ordinary reader while the committed reader rejects it. Repeat strict reads after an error to ensure no successful EOF hides the corruption.
- [ ] Exercise the existing `SlowReadFile` fixture with delayed short reads and a frame larger than the 4096-byte read chunk. Drive pending completions explicitly; no sleeps.
- [ ] Run the complete logical-log module tests and the focused failure tests with the prefix checks temporarily removed. Restore checks and rerun before committing.

## Evidence and delivery boundary

Record exact commands, output and commits in the execution ledger. No PHP
cross-process result, shared authority, live version installation, allocation,
checkpoint reclamation or issue completion follows from these reader tests.
Those remain separate stages in `docs/design/php-worker-mvcc-protocol.md`.
