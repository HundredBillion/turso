# Proposed versioned loadable custom-index interface

Status: design proposal, not an implemented or stable ABI. Existing native extensions and database files are unchanged. This proposal follows the internal dense IVF and SQL-maintained JSON postings experiments; their limitations are release gates, not assurances of production readiness.

## Findings that constrain the interface

The existing Rust `IndexMethod` interface supplies attachment, query-pattern matching, transaction-aware backing storage and cursor lifecycle hooks. `extensions/core/src/lib.rs::ExtensionApi` exposes scalar/aggregate functions, virtual tables and VFS, but no custom-index registration. Rust traits, AST enums, allocator types and references cannot be exported as a stable native ABI.

The dense prototype initially declared a different backing schema from its stored key layout; MVCC reopen lost entries. Explicit persistent field declarations fixed it. An extension must declare its complete durable layout to the engine, and reopen/recovery must exercise that declaration. Passing ordinary insert/query tests is insufficient.

Dense retrieval must apply tenant/permission constraints before top-k. The 100k synthetic experiment showed large differences in recall between large and very small tenants at the same probe count. Approximation quality and eligibility are separate contracts. Public exact SQL must not silently become approximate because an index exists: approximate search needs an explicit query/API opt-in and documented recall controls.

JSON postings make broad membership lookup possible but amplify writes and storage. Known-path expression indexes remain a simpler baseline. An index extension must supply a cost estimate that includes selectivity, persistent size and candidate work; a successful pattern match alone does not justify selection.

## Registration and compatibility

Add a separate negotiated index-extension entry point rather than changing the existing ExtensionApi struct in place. The engine requests an interface by identifier and ABI major version; a response includes struct byte size, ABI major/minor and capability bits. Callers access only fields covered by the declared size. Unknown required capabilities and unsupported major versions fail before attachment or writes; optional trailing fields can extend a compatible minor version.

Export only C-layout fixed-width fields, opaque handles, explicit byte spans and function pointers with fixed calling convention. No Rust enums, Vec, Arc, exceptions or borrowed AST pointers cross the boundary. SQL query templates are copied and parsed by the engine. Parameter/result values use tagged engine-owned views valid only for the documented callback duration; retained values require an explicit copy.

The dynamic library remains loaded until every registration, attachment, pending operation and cursor referencing it is destroyed. Destructors run exactly once and outside engine registry locks. Native extensions remain trusted in-process code, not a security sandbox.

## Smallest first capability set

The first bridge supports only transaction-aware engine backing storage. It does not support external index files with plugin-owned recovery, external network writes, custom checkpoint protocols or a new VFS. Existing internal ExternalTransactional support does not establish a safe public protocol for those capabilities.

The engine owns transaction admission, snapshot identity, commit/rollback, savepoints, WAL/MVCC durability and crash recovery. Maintenance must account for conflict actions such as INSERT OR REPLACE and UPDATE OR REPLACE: implicit deletion can bypass ordinary SQL delete triggers, as the postings experiment demonstrated. Define index notifications by the actual row changes, not by an assumption that every removal invokes a user delete trigger. The extension declares backing tables/indexes and operates through opaque engine handles bound to the current transaction and snapshot. It cannot commit independently or return rows from a different snapshot. Statement abort, rollback and savepoint notifications clear transient cursor state; durable undo remains the engine's responsibility.

An attachment declares method identity, durable format version, supported read versions, columns, complete backing schema, immutable configuration and supported transaction modes. Configuration for vector indexes includes dimensions, numeric encoding, distance metric and centroid/model identity. Query controls such as probe count should be query-scoped when supported, rather than requiring index rebuilds. Persisted format identity is independent of ABI version.

Opening an index with an unavailable plugin or unsupported format fails clearly before mutation. Format migration is an explicit rebuild into a new backing layout, validated against the old index before an atomic catalog switch. Retain the old layout until active readers release it; interrupted rebuilds must be discoverable and safely removable. No automatic in-place rewrite on open.

## Execution, ownership and cancellation

Each operation returns a tagged outcome: complete, row, pending, ordinary error, or cancellation. Pending work has an opaque operation token; the engine polls/resumes it through its existing IO scheduler. Callbacks cannot retain stack addresses or borrowed value spans. Cancellation is a request, not proof that underlying IO has retired: operation state, buffers, plugin ownership and required locks remain alive until retirement is confirmed. Publication must not acknowledge durable state before engine synchronization completes.

A cursor is serialized by default; separate cursors can run concurrently only under declared thread-safety capabilities. Reentrant SQL from callbacks is excluded initially to avoid hidden transaction and lock cycles. Structured error codes distinguish missing values, invalid arguments, conflicts, BUSY, OOM, corruption and cancellation; copied diagnostic text supplements rather than replaces the code. Panics/exceptions must never unwind through the ABI.

All engine-budgeted allocations use supplied fallible allocate/reallocate/free callbacks with explicit alignment and ownership. Per-operation limits cover candidate visits, retained bytes, result count and time/cancellation checks. Budget exhaustion returns a defined error; it must not silently return fewer eligible exact results. The current dense prototype materializes and sorts candidates, so bounded-memory top-k and cooperative cancellation need internal validation before exposing this contract publicly.

## Planner and result contracts

Registration identifies supported predicates, orderings, provided result expressions, residual predicates, uniqueness and whether rows are materialized. The engine verifies captures and retains residual filters. All eligibility predicates that can affect top-k, including residual WHERE conditions as well as security/tenant filters, must be enforced before extension-owned truncation. Otherwise the engine must apply LIMIT after a complete correctly ordered stream, or reject that plan. A global top-10 followed by a residual status filter does not implement an exact filtered LIMIT 10. LIMIT pushdown must also respect joins, DISTINCT and aggregation; unsupported combinations remain engine-evaluated or are excluded from the match. Approximate retrieval declares approximation explicitly; exact fallback remains available.

Cost estimates use current statistics and query parameters, not a constant preference for the custom index. Expose estimates for rows visited, expected eligible candidates and memory. Diagnostics identify the chosen method and relevant search controls. Deterministic tie-breaking uses a stable document identity.

## Release gates

Before implementing a public bridge, retain passing internal evidence for rollback/savepoint behavior, concurrent disjoint/conflicting writers, delete/update, schema changes, reopen, statement cancellation and crashes at write/sync/checkpoint boundaries. Add a native conformance plugin exercised across supported compiler/OS combinations and ABI version mismatches, loader/unloader lifetime, allocator failures and malformed registration metadata.

Measure representative dimensions and distributions, tenant selectivity, update cost, memory, disk growth and filtered recall against exact scans and ordinary indexes. The synthetic 32-dimensional result is not a representative embedding benchmark. Validate sustained application requests against current main before using extension support as evidence that Redis can be removed.

Decision: keep the bridge as a proposal now. The experiments justify the need for precise lifecycle, format and planner contracts, but do not yet justify freezing a public ABI or changing the JSONB/database format. No data migration is required by the present optimization or prototypes; a future incompatible format requires its own measured benefit and reviewed migration plan.
