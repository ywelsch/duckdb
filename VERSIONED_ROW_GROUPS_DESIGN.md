# Versioned row groups: structural MVCC for a table's storage

Design note, 2026-11-26. Context: the ALTER prototypes on `yw/alter-ckpt-shared-collection` (option 4) and the
vacuum prototypes on `yw/vacuum-checkpoint-concurrent-inserts` / `yw/vacuum-checkpoint-concurrent-deletes`.

## Problem

Two kinds of change replace the *structure* of a table's storage while older transactions still need the old one:

- **ALTER TABLE** changes the columns: an older snapshot must keep reading the old columns, an uncommitted ALTER
  must see its own.
- **Compaction** (vacuum merges) changes which row groups exist and so the row ids: an older snapshot, and a
  transaction that already read row ids, must keep seeing the old ones.

Today each is handled by a proxy:

| What | Proxy today |
|---|---|
| ALTER on main | copy the row groups, hold the checkpoint lock until commit |
| ALTER in option 4 | one shared tree; dropped columns stay *hidden*; "a non-concurrent checkpoint runs" stands for "nobody reads the hidden column any more"; computed columns (ALTER TYPE, volatile defaults) take the database-wide shared vacuum lock |
| Compaction on main | database-wide vacuum lock; FULL checkpoints downgraded while an older snapshot is open |
| Compaction, vacuum branch, pushed commits ("variant 2") | per-row-group snapshot check, per-merge seals; each table has a row id version bumped by a merge; a transaction that read row ids keeps reading the tree kept at the bump; a stale writer gets a conflict error |
| Compaction, vacuum branch, uncommitted simplification ("variant 3") | reader registration per statement only; a statement that names `rowid` holds the shared vacuum lock until its transaction ends, so no merge happens under it; no conflict error |

All of these approximate one thing: **readers use a version of the table's structure that matches their
snapshot**, the way they already pick a catalog entry. Variant 2 already is a small version chain: trees kept per
bump, pinned by readers, dropped when the last reader goes.

## Requirements this has to meet

From the ALTER work (user, 2026-11-24):

1. An ALTER statement or commit should preferably not wait for a running checkpoint.
2. A plain CHECKPOINT must not fail because of an ALTER; a checkpoint must never fail a commit.
3. A storage format change is allowed.
4. The altered table should preferably stay compactable while the ALTER is open.

From the vacuum work: row ids are stable within a transaction (for row ids it has seen).

Two consequences: a stale writer must not get a conflict error that only exists because a checkpoint ran (that is
the reason variant 3 replaced variant 2's error), and anything that keeps a table out of merges while an ALTER or a
transaction is open costs requirement 4.

## Model

- **Row level (unchanged).** Inserts, deletes and updates are versioned per row through the version info
  (`RowVersionManager`) and update segments.
- **Structure level.** A *layout version* of a table is a pair: a **tree** (the list of row groups and so the row
  ids) and a **column map** (which positions of the tree's row groups are the version's columns, in order). ALTER
  creates a new column map over the same tree; compaction creates a new tree.

Keeping the column map separate from the tree is what option 4 does: physical columns are shared and positional,
versions differ in the map. It avoids per-version copies of `RowGroup` objects, which every copy-based design so
far paid for (below). A physical column that no live version maps any more is dead; it can be left out the next
time a checkpoint builds those row groups (a rewrite or a merge creates a new tree anyway), which bounds the width
growth of today's hidden columns without copies.

### Stamps and visibility

A layout version carries a stamp like a `CatalogEntry::timestamp`: the creating transaction's id while
uncommitted, its commit id after. A reader uses the newest version `CatalogSet::UseTimestamp` would accept for it
(`catalog_set.cpp:540`); a checkpoint uses the version visible at its bound. A version is released once a newer
one is visible to every active transaction, as in `CatalogSet::CleanupEntry` (`catalog_set.cpp:509`).

### Where the catalog analogy breaks

- **Versions without a transaction.** A checkpoint that merges creates a version, but has no transaction and no
  undo entry: its stamp has to be taken at publish (a commit id, or the publish time compared with transaction
  starts), and its cleanup needs a trigger other than undo cleanup (the lowest active start passing the publish).
- **Committed versions beneath an uncommitted head.** A checkpoint publishes a committed version while an
  uncommitted ALTER's version is the head. The catalog's write-write rule would refuse one of them. Here the
  publish goes underneath and the uncommitted version is rebased onto it (today's `AlignCheckpointColumns`).
- **Cleanup does not free blocks by itself.** Releasing a version drops block handles; the blocks a checkpoint
  freed only become reusable at the next header write after that (seen in the option 4 block reuse test: two
  checkpoints, each needing a WAL write, before the space is reused).

## Operations

| Operation | Effect |
|---|---|
| Scan, fetch | the version visible to the transaction (tree + map pinned together: positions from `GetStorageIndex` must come from the same version as the tree) |
| Append | into the head's tree, which must be the appender's version (else the existing "table altered" conflict) |
| Delete, update | row level, on shared objects, with row ids of the head tree (see "stale writers") |
| ALTER | new column map; adds physical columns (constant or computed) to the shared tree when needed |
| Checkpoint, rewrite only | writes the version at its bound; rewritten row groups replace the old ones in that tree, carried into newer column maps (today's publish); older readers keep the objects they pinned |
| Checkpoint, merge | publishes a new tree, stamped at publish; older readers keep the pre-merge tree |
| Uncommitted version during a publish | rebased onto the published tree |

## Mapping from the prototypes

| Existing piece | In the model |
|---|---|
| option 4: one `RowGroupCollection` per table | owner of the version chain |
| option 4: `physical_columns` per `DataTable` | the column map, moved into the collection's version |
| option 4: `type_versions` / `current_types` | the physical layout of a tree |
| option 4: `AddSharedColumn`, `ComputeSharedColumn` | creating a column map plus physical columns |
| option 4: `AlignCheckpointColumns` | rebasing an uncommitted version at publish |
| option 4: hidden columns, `HiddenColumnData` | dead physical columns, dropped when a checkpoint rebuilds the row groups |
| vacuum variant 2: row id version, kept trees, reader registration | the tree chain; stamps replace the bump counter, snapshot visibility replaces "registered at first read" |
| vacuum: per-row-group snapshot check, per-merge seals | merge planning and installing a tree, unchanged |
| vacuum: durability latch | still needed (WAL row ids) |

## Hard parts

1. **Per-version copies of row groups are what failed before.** A design where each version has its own `RowGroup`
   objects sharing `ColumnData` is main's copying ALTER, the rebase prototype and option 5. A checkpoint rewrite
   creates new `ColumnData` for rewritten columns, so every version holding the old objects has to switch, and
   writes on the old objects have to follow. What that cost: the rebase prototype needed five rules plus a write
   ordering (an UPDATE on a copy before its rebase gave a heap-use-after-free in `WALWriteState::CommitEntry`);
   option 5 failed WAL replay after a merge (the copy kept pre-merge row ids); DAT-2253 is a reverted append
   truncating a shared tree. The model above avoids copies for ALTER (column maps over one tree) and only creates
   new trees on compaction, where the vacuum branch already handles the switch (seals swap the live tree; readers
   keep the old one).
2. **Stale writers.** A transaction that read row ids before a merge and then deletes or updates them. A conflict
   error violates requirement 2 (variant 2 did that, variant 3 removed it). Remaining options: translate through
   a row-id map the merge records (sources are contiguous minus the vacuumed rows); keep row groups a transaction
   has read row ids from out of merges (variant 3's effect, at the cost of compaction).
3. **WAL row ids.** Writes are logged with head row ids; a merge is durable only once its checkpoint wrote the
   header, so a commit that wrote into a freshly merged tree waits for that header (the vacuum branch's latch).
4. **Indexes** store head row ids. An older reader's index lookup returns head row ids; rows it can see but that a
   merge moved need translation, or index lookups of stale readers fall back to a scan (variant 2 does that).
5. **Rebasing an uncommitted ALTER onto a merge.** A constant column is refilled; a column computed by a
   deterministic expression can be recomputed for the merged rows from their source columns; a column computed by
   a volatile default (`random()`, `nextval`) cannot, its values must move with their rows (row map) or that
   table's row groups stay out of merges while the ALTER is open (requirement 4, for that table only).
6. **Statistics** per physical column, so shared columns share them; the table statistics of a version are the
   statistics of its mapped columns.
7. **Interface.** Paths that ask for "the" row groups (`GetRowGroups()`) become "the version visible to this
   transaction / to this checkpoint's bound". Scan states already hold a `shared_ptr<RowGroupSegmentTree>`
   (`scan_state.hpp:276`); the map would be pinned with it.

## Suggested order

0. **Option 4 without the global proxies (no new structure).** Stamp each hidden column with the commit that hid
   it; swap it for the placeholder, and let merges skip its contents, once the lowest active start is past that
   stamp, instead of "the checkpoint is non-concurrent". Replace the database-wide vacuum lock of computed ALTERs
   by a per-table mark that keeps that table's row groups out of merge planning while the ALTER is open. Both work
   on today's main and keep working once the vacuum branch drops the database-wide downgrade.
1. **Trees as versions (compaction).** Generalize variant 2's kept trees into the collection's chain with stamps,
   pinned by snapshot; decide the stale-writer rule (translation or merge exclusion, not an error). Durability
   latch stays.
2. **Column maps as versions (ALTER).** Move option 4's per-`DataTable` map into the collection's versions, drop
   dead physical columns when a checkpoint rebuilds row groups; rebase uncommitted ALTERs onto merges where the
   column can be recomputed, keep the table out of merges where it cannot.

### Acceptance tests (the cases that broke earlier designs)

- WAL replay with no checkpoint after a merge that ran during an open ALTER
  (`alter_drop_column_shared_replay.test`; option 5's version `alter_concurrent_checkpoint_replay.test` failed
  with "row 245760 is not in any of its 2 segments").
- `alter_based_on_unfinished_insert_fails.test_slow` (append revert racing a shared ALTER).
- The paused-publish tests: `alter_{add_column,drop_column,type}_shared_paused.test_slow`.
- An UPDATE by the ALTER's transaction after a checkpoint rewrote its row group (the rebase prototype's
  use-after-free, `alter_table_open_transaction_checkpoint.test` on `yw/alter-open-transaction-checkpoints`).
- `alter_concurrent_checkpoint_stress.test_slow`, the vacuum branch's `vacuum_checkpoint_transaction_row_groups*`
  and `vacuum_checkpoint_open_snapshot*` tests, and the option 4 block reuse tests.
