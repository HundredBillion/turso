# Explicit native shared-MVCC operations

These operations use native Linux64 file identities and OFD locks on supported
filesystems. They never upgrade a database just because it is opened. Stop all
older workers before selecting an offline operation and acknowledge that fact
with `--all-older-workers-stopped`. The protocol also checks current participants;
a live transaction can return BUSY rather than being evicted or retried.

Existing ordinary SQLite DELETE/WAL and Turso MVCC databases use the same explicit operation:

```sh
tursodb data.db --shared-mvcc-offline upgrade-existing --all-older-workers-stopped
```

An existing rollback journal, shared transition namespace, or foreign nonempty
logical log is rejected before conversion. Recover a hot rollback journal with
SQLite first. An acknowledged SQLite WAL is recovered under the retained native
main-file exclusion; the operator does not replace it with an empty file.

To initialize and publish a new database without replacing an existing file, use
separate staging and destination paths:

```sh
tursodb data.db --shared-mvcc-offline publish-new --shared-mvcc-staging staging.db --all-older-workers-stopped
```

The default history limit is 67108864 bytes. If a different limit was selected
at initialization, supply the same `--shared-mvcc-history-limit BYTES` on later
shared opens and offline operations. A structural minimum does not guarantee
space for a write; claims, cleanup debt and the logical log share this budget.

Open an already marked database intentionally:

```sh
tursodb data.db --shared-mvcc 'SELECT * FROM items;'
tursodb data.db --shared-mvcc --readonly 'SELECT * FROM items;'
```

`--shared-mvcc` accepts a native filesystem path, not a URI. It does not create
missing files or shared metadata. Ordinary opens continue to reject the shared
marker. C/PDO shared URI activation is a separate acceptance gate; these CLI
options do not enable it.

To return to ordinary WAL after stopping older workers:

```sh
tursodb data.db --shared-mvcc-offline downgrade-existing --all-older-workers-stopped
```

The operator obtains an actual completed checkpoint and finalized ID proof,
installs that physical generation, publishes a terminal downgrade intent, then
uses a normal WAL transaction and FULL checkpoint on the same main inode.
Success includes namespace and obsolete-log cleanup. It does not copy or replace
the database, synthesize a checkpoint proof, or retry SQL.

Interrupted operations require the matching explicit recovery action:

```sh
tursodb data.db --shared-mvcc-offline resume-prepared-marker --all-older-workers-stopped
tursodb data.db --shared-mvcc-offline resume-downgrade-marker --all-older-workers-stopped
tursodb data.db --shared-mvcc-offline resume-downgrade-cleanup --all-older-workers-stopped
```

Prepared-marker recovery is for an interrupted upgrade. Downgrade-marker
recovery requires a valid existing terminal intent and its exact files; it does
not recreate a missing or replaced WAL. Cleanup recovery requires an already
checkpointed ordinary WAL main file and validates surviving terminal metadata
before removing files. Unknown or replaced sidecars cause an error. An error
can occur after the ordinary marker was committed; preserve the files and use
the diagnostic to select recovery rather than repeating initialization.

Do not combine offline operations with SQL, read-only opens, custom VFS,
encryption, server modes, or experimental shell options. Such combinations are
rejected before opening the normal shell connection.
