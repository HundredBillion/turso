# MVCC commit manifest technical spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve a verifiable acknowledged-log boundary independently of transient mapped fields.

**Architecture:** A separate manifest has two 128-byte records in separate 4096-byte aligned blocks. A publisher writes and synchronizes only the inactive record after synchronizing the logical log, then publishes shared state and acknowledges. Recovery chooses a valid latest record, validates the durable checkpoint boundary and referenced log prefix, and only then considers a tentative suffix.

**Tech Stack:** Existing File/Completion/IOResult state machines, CRC32C, MemoryIO, forced-short-I/O fixtures and real Linux subprocesses.

## Global constraints

- Existing modes remain unchanged and all public cross-process MVCC guards stay enabled.
- A valid manifest is mandatory once the new on-disk mode marker is durable.
- Initial upgrade synchronizes its first manifest record before publishing the mode marker.
- Never overwrite the current valid record while publishing its successor. Slot offsets are 0 and 4096; each write is confined to its own aligned 4096-byte block. The supported storage failure model permits a torn in-flight block write but preserves previously synchronized, separately addressed blocks. Media corruption or devices that violate successful synchronization are outside this guarantee.
- Within one log generation an unchanged end offset must retain both the committed timestamp and running CRC.
- Successful logical-log synchronization precedes manifest write; successful manifest synchronization precedes shared publication and acknowledgement.
- A short write, failed sync or dropped publishing state cannot return a successful publication token.
- Checkpoint-aware recovery must distinguish an acknowledged prefix already materialized in the database from one still required in the logical log.

## Task 1: Record codec and recovery selection

**Blocked by:** None; source-level authority component is independently reviewed and Linux-tested.

**Files:**
- Create: `core/mvcc/commit_manifest.rs`
- Modify: `core/mvcc/mod.rs`

**Interfaces:**
- `ManifestRecord::new(sequence, database_id, log_generation, log_salt, prefix) -> Result<ManifestRecord>`.
- `ManifestRecord::encode() -> [u8; 128]`.
- `ManifestRecord::decode(bytes: &[u8]) -> Result<Option<ManifestRecord>>`.
- `select_latest(records: [Option<ManifestRecord>; 2], database_id: FileId) -> Result<(usize, ManifestRecord)>`.

- [ ] Write a failing round-trip/selection test before implementing the codec.
- [ ] Encode fields explicitly little-endian: magic0..8, version8..12, record length12..16, sequence16..24, device24..32, inode32..40, log generation40..48, salt48..56, committed timestamp56..64, log offset64..72, log CRC72..76, reserved zero76..124, record CRC124..128. Compute CRC32C over bytes0..124.
- [ ] Return no record for an incomplete slot, an all-zero slot or failed record checksum. A complete checksummed incompatible version, invalid identity or impossible record is an error, not a fallback to an older slot.
- [ ] Require positive record sequence and log generation, representable62-bit timestamps, and either a zero-length/zero-CRC empty log or an offset at least the logical-log header size.
- [ ] Reject two records with the same sequence but different contents. Reject a newer record that rewinds its generation/committed timestamp or changes salt/rewinds offset within the same generation.
- [ ] Use generated plain integer fields plus exhaustive single-byte truncations and single-bit corruption of encoded records. Test a valid old slot with every truncated successor, incompatible version with a recomputed checksum, both slots invalid and crossed database identities.
- [ ] Run `cargo test -p turso_core --lib mvcc::commit_manifest::tests`, record red/green evidence and obtain source review.

## Task 2: Exact writes and durable publication

**Blocked by:** Task 1

**Interfaces:**
- A yielding load state returns the latest valid record and its slot.
- A yielding publish state owns the global authority guard through write and sync, and returns a durable-publication token only after both succeed.

- [ ] Read both slots with exact-count handling across short reads; an active-mode file with no valid record is corruption.
- [ ] Write the next sequence into the inactive slot and inspect the completion's byte count. Preserve the current record on any short write or write failure.
- [ ] Issue manifest sync only after the complete record write succeeds. Return the publication token only after observing successful sync; an error or state drop leaves authority recovery required.
- [ ] Verify every write split, delayed write completion, delayed/failed sync and cancellation. The previously acknowledged record must remain selectable after every failed successor.
- [ ] Add actual process-death boundaries around manifest write/sync and recover from a fresh process using an independently synchronized acknowledgement ledger.

## Integration boundary

The codec alone does not establish durable publication. The writer alone does not
establish engine commit ordering. A real caller must synchronize the logical log,
hold global authority, install complete versions, and acknowledge only after the
manifest and shared publication succeed. That caller, checkpoint reconciliation
and safe mode upgrade/downgrade remain explicit subsequent tasks.
