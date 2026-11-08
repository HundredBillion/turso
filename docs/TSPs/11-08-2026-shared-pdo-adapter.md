# Shared PDO adapter technical spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task.

**Goal:** Connect the approved explicit PDO URI opt-in to the verified native shared engine and prove independent PHP worker behavior.

**Architecture:** Pass validated URI options into Core `OpenOptions::with_shared_mvcc`; Core remains responsible for marker validation, coordination and recovery. Report actual engine mode through a read-only pragma. The acceptance supervisor explicitly initializes its new fixture using the offline operator before starting shared workers. No ordinary open upgrades an existing database.

**Tech Stack:** Rust 1.88, SQLite C ABI, PHP PDO, Python worker supervisor, Linux native filesystem.

## Global constraints

- Initial PHP support is PDO URI only, as approved; ordinary SQLite3 behavior remains unchanged.
- Shared opens require an existing, explicitly initialized database. CREATE permission never authorizes an implicit upgrade or unverified file creation.
- Existing databases require the approved offline operation with all older workers stopped.
- Keep default C dialect and options, errors, read-only restrictions and URI validation.
- No SQL retries, fallback to ordinary WAL, synthetic capabilities or alternate shared flags.
- No separate PRD was supplied; source requirements are the approved issue #7 design and existing PHP acceptance assertions.
- Keep this private integration separate until common open recovery and interrupted completion have passed their native gates.

### Task 1: Real C URI and capability

**Blocked by:** None for private implementation and focused tests; public integration requires recovery gates.

**Files:** `bindings/c/src/lib.rs`, `sqlite/parser/src/ast.rs`, `core/pragma.rs`, `core/translate/pragma.rs`, `bindings/c/README.md`, `docs/sql-reference/pragmas.mdx`.

**Interfaces:** Consume `OpenOptions::with_shared_mvcc(history_limit)` and existing URI parser output. Produce real `sqlite3_open_v2` shared opens and query-only `PRAGMA turso_mvcc_mode`, reporting `shared`, `private` for ordinary MVCC, or `off` when MVCC is disabled.

- [ ] Add an actual C URI regression using `recovered_checkpoint_for_test` only to prepare real marked files; close fixture owners before `sqlite3_open_v2` and assert actual SQL read/write, shared capability, checkpoint, readonly reopen and write rejection.
- [ ] Run the focused regression on native Linux against the current C refusal and retain the expected failure with positive test count.
- [ ] Replace the blanket refusal only in this private branch: retain VFS checks and use shared Core options for named native files. Preserve ordinary memory, temp, permissions and error mapping paths.
- [ ] Add pragma enum and metadata, query actual selected store mode, reject assignments, and cover ordinary/private/shared cases with existing Rust harnesses.
- [ ] Run focused C tests, full C compatibility tests, strict Core/C lint and formatting; document existing-only initialization and platform support.

### Task 2: Independent PDO acceptance

**Blocked by:** Task 1 plus verified common-open and interrupted-checkpoint recovery.

**Files:** `bindings/c/tests/php_workers/run.py`, `bindings/c/tests/php_workers/README.md`, and validation-only workflow.

**Interfaces:** Add `--shared-admin-cli PATH` for the shared Turso profile. Initialize an ordinary empty fixture, close and reap its worker, invoke the actual offline upgrade with explicit acknowledgement, then start existing shared PDO workers.

- [ ] Require the initializer argument only for shared Turso acceptance; preserve stock SQLite negative controls and ordinary PDO profiles.
- [ ] Record initializer command, result, worker lifetime boundary and exact tested binaries in the supervisor evidence; stop on any initialization error.
- [ ] Run supervisor unit tests and Ruff, then actual mapped-library PDO acceptance with independent PIDs, simultaneous transactions, stable readers, conflicts, IDs, rollback/savepoints, readonly/temp/attach checks, fsynced acknowledgements and killed-process recovery.
- [ ] Run stock SQLite negative control and ordinary Turso controls on the same host, inspect exact final summaries and retain artifacts.
- [ ] Obtain independent source review and combined Linux/Windows/PDO checks before integrating or removing the PR draft status.
