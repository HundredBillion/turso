# Concrete offline upgrade backend

Implements the approved offline-upgrade prerequisite from the independent PHP-worker PRD. Private implementation only; public PDO activation and shared-mode downgrade remain separate integration tasks.

1. Reproduce a real ordinary MVCC database upgrade through a concrete backend, retaining committed rows and a conservative logical-ID floor. Start with a failing constructor scaffold in the existing mode-transition harness.
2. Open existing native storage without registry reuse, retain an OFD exclusion lock, and reject unsupported platforms/filesystems, symlinks, incompatible files, and live participants before mutation. The operator must already have stopped older workers; locks cannot prove absence of unlocked old readers.
3. Use the ordinary engine to recover and FULL-checkpoint data. Capture the consumed logical-ID floor and materialized timestamp before switching to WAL for the pager transaction that publishes marker 254. Initialize and synchronize the existing identity-based namespace before the marker. Preserve leases through all completion retirement.
4. Add fresh staging initialization using the existing durable no-replace publisher. Do not enable downgrade of an already shared runtime until the shared checkpoint driver exists.
5. Validate native Linux upgrade/reopen, existing data and indexes, source rejection without mutation, retained lock lifetime, and fresh publication. Use failure controls around marker publication and namespace initialization. Review independently before integration.

## Explicit private operation entry point

The approved offline prerequisite applies to every operation. Add `OfflineOperation` and `execute_offline` in `mode_transition/operation.rs`: callers must select upgrade, prepared-marker recovery, or fresh publication explicitly. Do not infer an upgrade from URI opening. Reject a history limit below `MIN_HISTORY_LIMIT` before opening or creating files, then dispatch to the already verified concrete backends. Keep the module private to MVCC until the complete public acceptance contract passes. Downgrade continues to require the separate quiescent materialization proof.

- [ ] Reproduce invalid-budget fresh creation through the entry point, verifying that no staging files appear.
- [ ] Implement budget preflight and exact dispatch without fallback between operations.
- [ ] Run the portable preflight test and native mode-transition suite; independently review the dispatch and prerequisite propagation.
