# Independent PHP worker acceptance

These tests use real PHP child processes and the public PDO/SQLite3 C-compatibility
boundary. They do not implement shared MVCC, change URI parsing, remove current
mode guards, or establish production readiness. The full profile must fail until
the engine implements and verifies the shared protocol. Linux is the initial
platform: library identity uses `/proc/self/maps`, deadlines use pipe readiness,
and crash controls use SIGKILL.

## Profiles and mode attestation

* `sqlite-wal-control`: stock SQLite process/control/oracle checks. It deliberately
  expects the second WAL writer to return BUSY, then starts a distinct transaction
  after the first completes. Its output always says `acceptance: false`.
* `sqlite-shared-negative-control`: proves that ignored URI options cannot turn
  ordinary SQLite into a passing shared-mode test. Expected exit is **78** with
  `status: unsupported`, never acceptance success.
* `turso-single-worker-control`: actual loaded Turso library, ordinary opens,
  rollback/savepoints and sequential worker-death/recovery checks. No overlapping
  writer capability is claimed.
* `turso-wal-control`: the full ordinary-WAL control scenarios, if supported by
  the current engine's process admission. Failure is evidence of that control's
  limits, not permission to weaken shared-mode acceptance.
* `shared-acceptance`: all scenarios below, requiring successful execution of
  two disjoint write transactions before either commits. Every opened participant
  must report `PRAGMA turso_mvcc_mode` as exactly `shared`. A missing capability
  produces exit 78 before durability configuration or data-changing SQL.

The proposed URI contract, coordinated with the protocol owner, is
`file:/absolute/path?turso_mvcc=shared&turso_mvcc_history_limit=67108864`, plus
`mode=ro` for read-only participants. Ordinary existing databases must not upgrade
implicitly: a separate fixture checks refusal and unchanged files. The principal
fixture is a fresh database, avoiding unsupported online conversion. An explicit
offline-upgrade acceptance scenario awaits its actual public API.

PHP 8.3 PDO forwards URI opens successfully. PHP 8.3 `SQLite3::open` expands every
non-memory filename before calling `sqlite3_open_v2`; a `file:` URI therefore
fails before reaching the intended parser. This was reproduced against stock
SQLite and is visible in [PHP 8.3.6 source](https://github.com/php/php-src/blob/php-8.3.6/ext/sqlite3/sqlite3.c#L105).
The SQLite3 ordinary controls use plain paths and explicit read-only flags.
Shared SQLite3 URI opening remains a clear failure until an approved explicit
binding API exists. No filename rewrite or ordinary-mode fallback is used.

## Run

Install PHP CLI with PDO_SQLITE and SQLite3 extensions. All commands below run in
the same Linux environment as the worker binaries and database files.

```
python3 -m unittest discover -s bindings/c/tests/php_workers -p 'test_*.py'
php -l bindings/c/tests/php_workers/worker.php
python3 bindings/c/tests/php_workers/run.py --profile sqlite-wal-control --api pdo --output /new/control-pdo
python3 bindings/c/tests/php_workers/run.py --profile sqlite-wal-control --api sqlite3 --output /new/control-sqlite3
python3 bindings/c/tests/php_workers/run.py --profile sqlite-shared-negative-control --api pdo --output /new/ignored-options
```

For a freshly built Turso C library, arrange the loader exactly as the existing
PHP storage validation does. Keep the symlink directory dedicated to the test:

```
cargo build -p turso_sqlite3
mkdir /new/turso-loader
ln -s "$PWD/target/debug/libturso_sqlite3.so" /new/turso-loader/libsqlite3.so.0
LD_LIBRARY_PATH="/new/turso-loader:$PWD/target/debug" python3 bindings/c/tests/php_workers/run.py \
  --profile turso-single-worker-control --api pdo \
  --library "$PWD/target/debug/libturso_sqlite3.so" --output /new/turso-control
LD_LIBRARY_PATH="/new/turso-loader:$PWD/target/debug" python3 bindings/c/tests/php_workers/run.py \
  --profile shared-acceptance --api pdo \
  --library "$PWD/target/debug/libturso_sqlite3.so" --output /new/shared-acceptance
```

The supervisor and every Turso worker compare the exact mapped library path and
SHA-256, require `turso_version()`, and reject an unexpected stock SQLite mapping.
A PHP process launched with a shell/container wrapper is not substituted for the
worker: reported PHP PID must equal the subprocess PID. When using a container,
run the Python supervisor inside it, so signals target the actual PHP children.

## Observable scenarios

The supervisor sends one command per worker, correlates every response by ID,
and never automatically retries an SQL statement or transaction. Explicit BEGIN
and write responses act as barriers; no sleeps create transaction overlap.
Response deadlines default to five seconds, busy timeout to 100 ms, and reported
SQL failure time is bounded separately. Every response records one attempt.

Coverage includes private CRUD and own writes; disjoint pending writes with an
independent reader; long-reader snapshots across peer commits; fresh-reader
visibility; BUSY checkpoint under a long reader and success after release;
rollback versus peer commit; savepoints; update/delete and unique conflicts;
concurrent automatic IDs and sequence persistence; read-only main and attachment
write rejection; writable private TEMP; and durable-file hashes. Stock SQLite
may create an empty WAL on a read-only open, so only that control permits a new
zero-byte WAL; existing bytes must remain unchanged. Turso profiles require the
recorded main/WAL/log files to remain byte-identical, excluding coordination
sidecars from the content-preservation comparison.

Each recovery transaction has two exact independently specified rows. The
supervisor records and fsyncs its intent before work, and records/fsyncs an ACK
only after a successful COMMIT response. Crash cases are:

1. SIGKILL only after the supervisor's ACK record is synchronized.
2. SIGKILL after one uncommitted row: no row may recover.
3. Worker self-SIGKILL after FULL COMMIT returns but before sending its reply:
   this is a lost external acknowledgment, with a known completed commit.
4. SIGKILL after a command-start event for COMMIT: the IO phase is unknown, and
   the unreported transaction may recover completely or not at all.

The surviving old reader must retain its snapshot; it is then killed as well.
A genuinely new PHP process checks the persisted ledger, exact acknowledged
payloads, all-or-none unreported rows and `integrity_check`. The controller never
uses recovered database rows to manufacture expected values. The ledger is
outside the workers and survives their deaths. A single-worker Turso control
omits the concurrent reader but retains the fresh-process recovery checks.

These are process-death controls, not power-loss injection. They do not prove
specific append/fsync/checkpoint interruption points or full retained-log-cap,
upgrade/downgrade, unsupported-filesystem or mixed-version acceptance. Native
fault-injection and protocol-owner tests must supply that evidence. No performance
acceptance is implied by control success.

## Current evidence

Real PHP 8.3.6/PDO and SQLite3 stock controls passed in a disposable Ubuntu 24.04
container. This includes response barriers, conflicts, read-only/TEMP/ATTACH,
forced deaths, lost acknowledgment, fresh recovery and integrity checks. The
ignored-options negative control exited 78 as required. Supervisor negative tests
reject silent workers, stale response IDs, incomplete responses, lost acknowledged
rows, partial unreported transactions and mismatched ACK payloads.

The controls exposed two harness defects before completion: fetching a zero-column
SQLite3 result could execute a statement twice, and command construction replaced
the requested crash operation with ordinary SQL. Skipping row fetch for zero
columns and preserving the explicit operation fixed those observed failures.
Fresh Turso PHP controls and the full shared-mode expected failure still require
a matching Linux Turso library; a macOS library cannot be used in the local Linux
container. Full shared-MVCC acceptance remains RED until the engine is wired.

## SQLite3 vendor-flag feasibility (not an implemented API)

A diagnostic confirmed that PHP 8.3.6 forwards `6 | 0x10000000` unchanged as
`0x10000006` to `sqlite3_open_v2` for a plain filename. Stock SQLite ignored the
bit, returned SELECT 42, and returned no shared-mode capability. This is evidence
for a possible explicit flag API, not evidence that any current Turso flag enables
shared MVCC. The supervisor's optional `--shared-open-flag 0x10000000` passes that
flag for shared tests and uses a plain SQLite3 filename, but still requires the
same mode attestation. With stock SQLite it must exit 78.

`probe_open_flags.c` is an isolated diagnostic interposer. It removes GNU loader
DEEPBIND only so its symbol hook can observe PHP's actual call; it must not be used
for acceptance or performance runs. Reproduce separately with:

```
cc -shared -fPIC -Wall -Wextra -Werror probe_open_flags.c -ldl -o /tmp/probe-open-flags.so
LD_PRELOAD=/tmp/probe-open-flags.so php -r '$db=new SQLite3("/tmp/flags.db",6|0x10000000); echo $db->querySingle("SELECT 42"),"\n";'
```

The public flag assignment, durable-limit configuration and release support are
owned by the engine implementation and user scope decision. This test code changes
no production interface and never treats stock SQLite's ignored bit as support.
