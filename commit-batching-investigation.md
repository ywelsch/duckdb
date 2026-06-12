# Commit Batching (Group Commit) in DuckTransactionManager — Investigation & Prototype Benchmark

*Date: 2026-06-11. Investigated at commit `73a804ef72` on `main`. Machine: macOS (Darwin 24.6.0), Apple SSD.*

**Goal:** avoid one fsync per write transaction in `DuckTransactionManager::CommitTransaction` to better handle large volumes of small write transactions.

## TL;DR

- Every write transaction performs its own WAL fsync, **while holding both the global WAL mutex and the global `transaction_lock`** — commits are fully serialized end-to-end.
- A durable-commit benchmark confirms it: ~220 TPS pinned to the device sync rate, flat across 1–16 threads, with commit latency growing linearly with thread count (74 ms at 16 threads). Classic group-commit motivation profile.
- The no-fsync ceiling is ~30k TPS (also flat across threads → a secondary ~33 µs lock-serialized commit path exists, but the fsync is the 100× bigger prize).
- Group commit at 16 concurrent committers ≈ 16 × 214 ≈ **3,400 TPS, a ~15× win**, growing with concurrency until the ~30k ceiling.
- Incidental finding: **`HAVE_FULLFSYNC` is never defined by the build system**, so the `F_FULLFSYNC` branch in `LocalFileSystem::FileSync` is dead code and macOS commits are not power-loss durable (plain `fsync()` only).
- The WAL format and replay already support batched syncs — no format change needed for group commit.

## Current commit path (where the fsync happens)

Call chain for a write transaction commit:

1. `DuckTransactionManager::CommitTransaction` (`src/transaction/duck_transaction_manager.cpp:284`) takes `transaction_lock`, decides whether to checkpoint, then drops `transaction_lock` and grabs the **global WAL mutex** (`storage_manager.GetWALLock()`, line ~329), holding it until the commit is fully done.
2. `DuckTransaction::WriteToWAL` (`src/transaction/duck_transaction.cpp:207`) serializes the undo buffer into the WAL. Entries are buffered in memory (`ChecksumWriter` / `BufferedFileWriter`), so appending is cheap. If the transaction optimistically wrote row groups, it additionally fsyncs the **database file** (`GetBlockManager().FileSync()`, `duck_transaction.cpp:227`) — a second fsync, but only for larger transactions.
3. Back under `transaction_lock`, `DuckTransaction::Commit` (`duck_transaction.cpp:248`) commits the undo buffer in memory and calls `commit_state->FlushCommit()` → `WriteAheadLog::Flush()` (`src/storage/write_ahead_log.cpp:567`), which writes a `WAL_FLUSH` marker entry and calls `writer->Sync()` → `handle->Sync()` — **the fsync**, executed under both locks.

Because the fsync sits under `transaction_lock`, it doesn't just serialize commits — it also blocks `StartTransaction`, rollbacks, and everything else touching the transaction manager.

## What works in our favor

- **WAL format/replay already supports batched syncs.** Replay (`src/storage/wal_replay.cpp:203,526`) treats each `WAL_FLUSH` entry as a transaction boundary and commits at every marker; a torn/unsynced tail is detected via checksums and silently truncated (`successful_offset` logic, ~line 520). We can keep one `WAL_FLUSH` marker per transaction and fsync once per *batch*.
- **WAL appends are memory-buffered** (`BufferedFileWriter`), so serializing appends under the WAL mutex is cheap; only `Sync()` is expensive.

## Design sketch: group commit (leader/follower)

1. **Split `WriteAheadLog::Flush()`** into "write `WAL_FLUSH` marker + push buffer to OS (`writer->Flush()`)" under the WAL mutex, and "`handle->Sync()`" outside it. Track `written_offset` and `synced_offset` plus a condition variable.
2. **Restructure `CommitTransaction` into three phases:**
   - *Phase A (under WAL mutex):* append WAL entries + `WAL_FLUSH` marker; record `my_offset = written_offset`. Release mutex.
   - *Phase B (group sync):* if `synced_offset >= my_offset`, done. Otherwise one thread becomes sync leader, fsyncs once (covering everything appended before it), advances `synced_offset`, broadcasts; followers wait on the condvar. This is the batching.
   - *Phase C (under `transaction_lock`):* assign commit id, `undo_buffer.Commit` / `storage->Commit`, remove from active transactions. All in-memory, fast.
3. **Visibility ordering (the subtle part).** Today visibility and durability are atomic because the fsync sits under `transaction_lock`. Doing B before C (durability before visibility) avoids the "visible but not durable" anomaly for concurrent readers and is the safe default.
4. **Failure handling / WAL truncation.** `SingleFileStorageCommitState::RevertCommit` (`src/storage/storage_manager.cpp:658`) aborts a failed commit by *truncating* the WAL — only valid because the failing transaction is always the last writer under the held WAL mutex. With group commit, anything that can fail after the WAL append must be checked **before** releasing the WAL mutex in Phase A (or an abort/rollback marker entry would be needed = format change). Cleaner path: move failure-prone parts of the commit (local-storage flush, constraint checks) before/inside Phase A so a written `WAL_FLUSH` marker implies the commit can no longer fail. Audit what can throw in `DuckTransaction::Commit`.
5. **Checkpoint interactions.**
   - `WALStartCheckpoint` (`src/storage/storage_manager.cpp:250`) swaps `wal` to a `.wal.checkpoint` file under the WAL mutex: group-commit state (offsets, condvar, waiters) must be per-WAL-object or drained at the swap; pending waiters must be synced before the swap.
   - The `skip_wal_write_due_to_checkpoint` path (`duck_transaction_manager.cpp:313`) holds the WAL mutex through an entire checkpoint — waiters just queue behind it, which is correct.
   - Checkpoint-decision logic re-takes `transaction_lock` after the WAL write; the interleaving needs review since Phase C moves later.
6. **Second fsync for optimistic data** (`duck_transaction.cpp:227`, DB-file `FileSync` when row groups were optimistically written): rarely triggers for small transactions; could be folded into the same batching scheme later.

### Simpler alternative/complement: relaxed durability

Group commit only helps when **multiple connections commit concurrently**. A single connection issuing thousands of small autocommit inserts gains nothing — each commit still waits one fsync. For that workload: application-level batching, or an opt-in async-durability setting (à la PostgreSQL `synchronous_commit = off` / SQLite `synchronous = NORMAL`): ack COMMIT after the WAL write to the OS, fsync in background every N ms / N bytes. Crash loses at most the last window of acked transactions; the WAL-prefix property keeps the DB consistent and replay already handles the torn tail. Much smaller change; pairs with existing `RecoveryMode::NO_WAL_WRITES` plumbing (`attached_database.hpp:37`). DuckDB currently has nothing between "full fsync per commit" and "no WAL at all".

### Estimated scope (full group commit)

- `src/transaction/duck_transaction_manager.cpp` — three-phase restructuring of `CommitTransaction` (bulk of the work)
- `src/transaction/duck_transaction.cpp` — split `WriteToWAL`/`Commit` so the sync point is separable; move failure-prone work ahead of the flush marker
- `src/storage/write_ahead_log.cpp` + header — split `Flush()`; add `written_offset`/`synced_offset` + condvar
- `src/storage/storage_manager.cpp` — `FlushCommit`/`RevertCommit` semantics; WAL-swap draining in `WALStartCheckpoint`
- Tests: crash-recovery around batched syncs (torn tails between markers), concurrent commit stress, checkpoint-during-group-commit; `make test_configs` for lock-ordering changes

Risk concentrates in the failure-truncation path (4) and lock-ordering with checkpointing (5); the offset/condvar mechanics are straightforward.

## Prototype benchmark

### Methodology

- Benchmark program: `/tmp/duckdb_commit_bench/commit_bench.cpp` — N threads, each with its own `Connection`, doing single-row autocommit `INSERT`s via prepared statements into one table of a persistent DB. `SET checkpoint_threshold='100GB'` to keep auto-checkpoints out of the measurement. Reports TPS and average commit latency. Row count verified at the end.
- Raw device probe: `/tmp/duckdb_commit_bench/fsync_probe.c` — small append + sync loop.
- DuckDB release build with three env-var-gated hacks (inert when unset):
  - `DUCKDB_BENCH_SKIP_WAL_SYNC` — skip the WAL `Sync()` in `WriteAheadLog::Flush()` (no-fsync ceiling), in `src/storage/write_ahead_log.cpp`
  - `DUCKDB_BENCH_FULLFSYNC` — force `F_FULLFSYNC` in `LocalFileSystem::FileSync` (true durable commit cost on macOS), in `src/common/local_file_system.cpp`
  - `DUCKDB_BENCH_SYNC_DELAY_MS=<n>` — plain `fsync()` plus an artificial n-ms delay in `LocalFileSystem::FileSync`, simulating slow storage (HDD, network/cloud block devices), in `src/common/local_file_system.cpp`

  Both are marked `PROTOTYPE BENCHMARK HACK - DO NOT COMMIT` and live only in the local working tree.

### Raw device sync cost (Apple SSD)

| op | latency | rate |
|---|---|---|
| `fsync()` (no power-loss durability on macOS) | 0.026 ms | ~38,000/s |
| `F_FULLFSYNC` | 4.7 ms | ~214/s |

### Durable baseline (forced `F_FULLFSYNC` per commit)

| threads | TPS | avg commit latency |
|---|---|---|
| 1 | 223 | 4.5 ms |
| 2 | 225 | 8.9 ms |
| 4 | 232 | 17.2 ms |
| 8 | 217 | 36.8 ms |
| 16 | 217 | 73.6 ms |

Pinned to the device sync rate, zero scaling, latency linear in thread count → every commit queues behind every other commit's fsync.

### No-fsync ceiling (`DUCKDB_BENCH_SKIP_WAL_SYNC`)

| threads | TPS | avg commit latency |
|---|---|---|
| 1 | 23,900 | 0.042 ms |
| 2 | 31,500 | 0.064 ms |
| 4 | 30,800 | 0.13 ms |
| 8 | 30,300 | 0.26 ms |
| 16 | 30,100 | 0.53 ms |

Flat from 2→16 threads → a secondary lock-serialized commit path of ~33 µs/commit exists behind the fsync.

### Simulated slow storage (`DUCKDB_BENCH_SYNC_DELAY_MS=10`, ~10 ms per sync)

| threads | TPS | avg commit latency |
|---|---|---|
| 1 | 81 | 12.3 ms |
| 2 | 82 | 24.4 ms |
| 4 | 83 | 48.3 ms |
| 8 | 83 | 96.7 ms |
| 16 | 82 | 195.8 ms |

(Effective per-sync cost is ~12.3 ms: 10 ms requested delay plus ~2 ms `usleep` scheduler overshoot.)

Same shape as the F_FULLFSYNC run, just scaled: throughput pinned at 1/sync-latency regardless of concurrency, and per-commit latency is exactly N × sync-latency — at 16 threads each commit waits ~196 ms behind the queue. Two takeaways:

1. **Throughput scales inversely with sync latency, concurrency contributes nothing.** 0.026 ms sync → ~24k TPS, 4.7 ms → ~220 TPS, 12.3 ms → ~82 TPS. The commit path adds only ~40 µs on top of each sync, so for any realistic durable device the fsync is >99% of the commit.
2. **The slower the device, the bigger the group-commit win.** The projected group-commit throughput is N × (1/sync-latency) until the ~30k TPS lock ceiling; at 10 ms that ceiling is ~370 concurrent committers away, i.e. on slow storage group commit scales essentially linearly with concurrency. Equally important for interactive workloads: group commit caps commit latency at ~one sync (~12 ms here) instead of N × sync (~196 ms at 16 threads).

### Untouched macOS baseline (plain `fsync()` per commit)

~16k TPS at 1 thread, plateauing at ~24k. Fast only because macOS plain `fsync()` doesn't flush the drive cache — see finding below.

## Conclusions

1. **Group commit directly attacks the dominant cost**: expected ~N× 214 TPS for N concurrent committers (≈15× at 16 threads), with headroom up to the ~30k ceiling. Latency under concurrency improves even more dramatically (one shared fsync instead of N queued ones).
2. **Moving the fsync out of `transaction_lock` alone** (without batching) already stops fsyncs from blocking `StartTransaction`/rollbacks; the full win needs batching.
3. **A secondary optimization target** is the ~33 µs serialized commit path (`transaction_lock` critical section) — only relevant once fsyncs are amortized.
4. **Upstream-worthy side-finding:** `HAVE_FULLFSYNC` is referenced in `src/common/local_file_system.cpp:833` but never defined by any build configuration → macOS DuckDB commits do not survive power loss. Either define it on macOS (huge perf hit per the table above — which strengthens the case for group commit) or document the behavior. On Linux, `fdatasync` behaves like the durable table on consumer disks (~0.5–5 ms) and closer to the ceiling on enterprise disks with power-loss-protected caches.

## Group commit prototype implementation (2026-06-11)

Implemented in the working tree (on top of the benchmark hacks). Design choice: keep the WAL append **and** the in-memory commit under the existing locks exactly as today — preserving all failure/truncation semantics — and only move the fsync out, where it is batched across concurrent committers. The client is acked only after the fsync covers its commit, so durability semantics are unchanged. (This is "visibility before durability, ack after durability": a concurrent reader can observe a commit before its fsync completes; see Open questions.)

### Changes

- **`BufferedFileWriter::SyncData()`** (new, `src/common/serializer/buffered_file_writer.{hpp,cpp}`): fsyncs the file handle without touching the in-memory buffer, safe to run concurrently with `WriteData`/`Flush` from an appender thread.
- **`WriteAheadLog`** (`src/storage/write_ahead_log.{hpp,cpp}`): `Flush()` split into:
  - `WriteFlushMarker()` — writes the `WAL_FLUSH` marker and pushes buffered data to the OS (no fsync); returns the target offset; caller holds the WAL lock. Updates `written_offset` (a monotonic logical byte counter, `BufferedFileWriter::GetTotalWritten`).
  - `SyncUpTo(target)` — leader/follower batched fsync: tracks `synced_offset` under `sync_mutex` + condvar; one leader fsyncs (covering every marker pushed to the OS so far), followers wait. Callable without the WAL lock.
  - `Flush()` = `WriteFlushMarker()` + `SyncUpTo()` (used by checkpoint/WAL-swap paths, full durability as before).
- **`StorageManager`**: the `wal` member became `shared_ptr`; new `GetWALShared()` lets a committer keep the WAL alive across a concurrent checkpoint WAL-swap while it finishes its sync. The swap path (`WALStartCheckpoint`) calls `wal->Flush()` before replacing the WAL, so any pending sync target is already durable at swap time.
- **`SingleFileStorageCommitState::FlushCommit`**: calls `WriteFlushMarker()` instead of `Flush()` — the commit's fsync no longer happens inside `DuckTransaction::Commit` (i.e. no longer under `transaction_lock` + WAL lock).
- **`DuckTransactionManager::CommitTransaction`**: after the in-memory commit succeeds, captures `GetWALShared()` + `GetWrittenOffset()` while still holding the WAL lock; after releasing both `transaction_lock` and the WAL lock, calls `SyncUpTo(target)`. An fsync failure at that point is escalated to `FatalException` (the commit is already visible and can no longer be rolled back).

### Adaptive micro-batching (the two iterations that mattered)

1. **Naive leader/follower** scales linearly but every commit pays ~2× fsync latency: lockstep arrivals mean a commit's marker always lands just after the in-flight fsync started. (Measured: 24.4 ms flat at 10 ms sync latency.)
2. **Leader micro-wait** (PostgreSQL `commit_delay`-style): before fsync-ing, the leader polls `written_offset` in 100 µs sleeps (max 10 rounds) until it stops advancing, so the in-flight burst is covered by *this* fsync. Latency dropped to ~1.1× fsync. But macOS `sleep_for(100µs)` overshoots ~1.5 ms, regressing single-connection commits by ~25%.
3. **Adaptive trigger**: micro-wait only when concurrent commit activity is detected. Detection = `sync_waiters > 0` at fsync completion (threads blocked on the condvar) OR appends arrived during the fsync. A first attempt that only used "appends during fsync" flapped: when batching works perfectly, all appends land *before* the fsync and the signal reads zero — the waiter count is the stable signal.

### Results (same benchmark, group commit build)

Durable, forced `F_FULLFSYNC` (~4.7 ms device sync) — baseline vs group commit:

| threads | baseline TPS | group commit TPS | baseline latency | group commit latency |
|---|---|---|---|---|
| 1 | 223 | 215 | 4.5 ms | 4.7 ms |
| 2 | 225 | 361 | 8.9 ms | 5.5 ms |
| 4 | 232 | 734 | 17.2 ms | 5.5 ms |
| 8 | 217 | 1357 | 36.8 ms | 5.9 ms |
| 16 | 217 | **2537** | 73.6 ms | **6.3 ms** |

Simulated 10 ms sync latency:

| threads | baseline TPS | group commit TPS | baseline latency | group commit latency |
|---|---|---|---|---|
| 1 | 81 | 81 | 12.3 ms | 12.4 ms |
| 16 | 82 | **1066** | 195.8 ms | **15.0 ms** |

**11.7×–13× throughput at 16 threads, no single-thread regression, and per-commit latency stays ~1.1–1.3× the device sync latency regardless of concurrency** (vs N× before). Batching efficiency at 16 threads: ~12–13 commits per fsync.

### Validation so far

- `test/sql/transactions/*` (73 tests), `test/sql/storage/wal/*`, `test/sql/storage/checkpoint/*`, full `test/sql/storage/*` suite: all pass.
- Crash test: SIGKILL mid-benchmark (8 writers), reopen → WAL replays cleanly, consistent prefix of committed transactions recovered, no corruption.
- Full fast unittest suite: run pending/in progress at time of writing.

### EFS-like fsync latency (2026-06-11)

AWS quotes best-case EFS write latency of ~2.7 ms (Standard, us-east-1; 1.6 ms One Zone). Each fsync on EFS is an NFS COMMIT round-trip to replicated multi-AZ storage, so fsync-heavy workloads in practice see several to tens of ms. Bracketed with `DUCKDB_BENCH_SYNC_DELAY_MS` = 3 (best case) and 30 (degraded/conservative); the 10 ms tables above are the middle point. Baseline numbers are from the (twice-validated) serialized model: TPS = 1/effective-sync-latency flat, latency = N × sync; effective latency taken from the measured 1-thread run (includes ~1 ms sleep overshoot).

**EFS best case (3 ms requested, ~4.0 ms effective):**

| threads | baseline TPS (model) | group commit TPS | baseline latency | group commit latency |
|---|---|---|---|---|
| 1 | 248 | 248 | 4.0 ms | 4.0 ms |
| 8 | ~248 | 1475 | 32 ms | 5.4 ms |
| 16 | ~248 | 2760 | 64 ms | 5.8 ms |
| 32 | ~248 | 4265 | 129 ms | 7.5 ms |
| 64 | ~248 | **7642** | 258 ms | **8.4 ms** |

**EFS degraded (30 ms requested, ~34 ms effective):**

| threads | baseline TPS (model) | group commit TPS | baseline latency | group commit latency |
|---|---|---|---|---|
| 1 | 29 | 29 | 34 ms | 34 ms |
| 8 | ~29 | 219 | 273 ms | 37 ms |
| 16 | ~29 | 442 | 546 ms | 36 ms |
| 32 | ~29 | 613 | 1.09 s | 52 ms |
| 64 | ~29 | **1041** | 2.18 s | **61 ms** |

Takeaways:

1. **EFS is the environment where group commit matters most.** The baseline hard-caps at 1/sync-latency total writes per second for the entire database — ~250 TPS on healthy EFS, ~30 TPS degraded — no matter how many clients. Group commit lifts that to 7.6k / 1.0k TPS at 64 connections (**31× / 36×**), and the multiplier keeps growing with concurrency.
2. **Latency under load transforms completely**: at 64 connections on degraded EFS, a single-row commit takes 2.2 *seconds* in the baseline (queueing behind 63 other fsyncs) vs 61 ms with group commit (~2 fsync periods).
3. Batching efficiency at 64 threads is ~31–35 commits per fsync (~50%) — the remaining gap is the fixed micro-wait window (10 × 100 µs requests) versus staggered arrivals; a latency-proportional wait window would close most of it. At ≤16 threads efficiency is 70–85%.

## Visibility-before-durability fix (2026-06-11, second commit)

The prototype initially published the in-memory commit before the group fsync, so a transaction starting during the sync window could observe a commit that wasn't durable yet. Fixed with Postgres-style strict ordering: **a commit only becomes visible after its WAL entries are fsynced.**

### Design

Data-only commits that write to the WAL now run as **deferred (group) commits**:

1. *WAL phase* (under `transaction_lock` + WAL lock): `DuckTransaction::CommitToWAL` — first `UndoBuffer::ValidateCommitConflicts()` (the table-was-altered checks that `CommitState::CommitEntry` would otherwise raise during publish — they must fire **before** the flush marker, because after it the commit can no longer abort), then write the `WAL_FLUSH` marker. Failure here truncates the WAL and rolls back exactly as before.
2. Register in `pending_commit_publishes` (ordered set in `DuckTransactionManager`), release both locks, `SyncUpTo` (batched fsync).
3. `WaitForPublishTurn` — publishes happen in commit-id order, which keeps `recently_committed_transactions` ordered (a documented invariant of its GC scan) and ensures the visible state is always a *prefix of the WAL*, matching what replay would reconstruct after a crash.
4. Re-acquire `transaction_lock`, `DuckTransaction::PublishCommit` (the `undo_buffer.Commit` that sets commit ids on versions/catalog = visibility), bookkeeping, remove from active transactions, `FinishPendingCommit`.

A publish or sync failure after the marker is durable is a `FatalException` — the WAL says committed, so crash-restart replays the transaction, which is the correct committed state (same rationale as Postgres PANIC after the commit record is flushed). The conflict validation in step 1 makes this path unreachable in practice.

Two interleaving hazards required explicit guards:

- **Catalog commits**: the alter-conflict check reads catalog state, so no catalog change may interleave between a data commit's marker and its publish. Catalog-changing commits therefore stay on the legacy synchronous path (publish + *inline durable fsync* via `FlushCommit(durable=true)` under `transaction_lock` — exactly the pre-prototype behavior, anomaly-free since no transaction can start meanwhile) **and** they first `WaitForPendingCommits()` while holding the WAL lock (which blocks new registrations). This freezes the catalog between any data commit's validation and its publish.
- **Checkpoints**: a checkpoint snapshot would not include a durable-but-unpublished commit while still deleting its WAL — losing it on restart. `WALStartCheckpoint` now calls `WaitForPendingCommits()` after acquiring the WAL lock, before taking the snapshot.

Deadlock safety: lock order is WAL lock → `transaction_lock` → `publish_lock`; waiters on `publish_cv` hold neither the transaction lock nor the WAL lock, and publishers need only `transaction_lock`, which no waiter holds.

### Validation & performance after the fix

- `test/sql/transactions/*`, `test/sql/storage/wal/*`, `test/sql/storage/checkpoint/*`, `test/sql/catalog/*` (123 tests), `test/sql/alter/*` (107 tests, incl. `test_drop_col_concurrent_dml_conflict.test` which exercises the moved conflict check), `test/sql/storage/*`: all pass.
- SIGKILL crash test: clean recovery, consistent prefix.
- Performance unchanged: 209/2770 TPS at 1/16 threads (F_FULLFSYNC), 82/1110 TPS at 1/16 threads (10 ms); 64 threads at 10 ms reaches 2834 TPS.

## Experimental setting, DDL gate, and stress verification (2026-06-11, third commit)

- **`SET experimental_group_commit = true`** (global, default `false`) now gates the deferred-commit path. With it off, every commit takes the synchronous path, which is behaviorally identical to the pre-group-commit code — verified: 236 TPS flat at 16 threads (off) vs 3020 TPS (on, F_FULLFSYNC). The pending-commit drains and the DDL gate remain unconditional (no-ops when no deferred commits exist), making runtime toggling safe.
- **DDL publish gate**: `CatalogSet::AlterEntry`/`DropEntry` call `DuckTransactionManager::BlockPendingCommits()` before attaching a new catalog version — closing the race where an ALTER *executing* (not committing) during a deferred commit's sync window invalidated the marker-time conflict validation (clean abort would have become Fatal). While a DDL waits, new commits fall back to the synchronous path (no starvation, no blocking while holding the transaction lock). Registration now precedes validation, linearizing both through `publish_lock`.
- **Self-deadlock found by stress test + stack trace**: `AlterEntry` held the gate through `DependencyManager::AlterObject`, which re-enters `CatalogSet::DropEntry` → `BlockPendingCommits()` on the same thread. Fixed by releasing the gate after the version attachment, before the dependency-manager call. A good reminder that the lock reasoning needs adversarial testing.

### Stress verification (all with group commit on)

| test | result |
|---|---|
| DDL race: 8 writers + ALTER ADD/DROP COLUMN loop, 5 ms sync, 15 s | OK — 6046 commits, 42k clean conflict aborts, 761 ALTERs, 0 fatals, count exact |
| Checkpoint race: 8 writers + manual CHECKPOINT loop + 128KB auto-checkpoint threshold, 20 s | OK — 25,150 commits, **520 WAL-swap cycles**, 0 failures, count exact after close+reopen |
| SIGKILL during checkpoint+commit activity | OK — 11,793 rows replayed cleanly, database writable |
| `alter` (107), `catalog` (123), `transactions` (73), `storage/wal`, `storage/checkpoint` suites | all pass |

The WAL-swap safety argument (drain in `WALStartCheckpoint` + WAL-lock exclusion of registrations + shared_ptr lifetime + move-not-delete in `WALFinishCheckpoint`) is documented in the session notes; the checkpoint stress above exercises both swap directions under load.

## ART index interactions (2026-06-12)

Analysis of index state vs the deferred window, plus a new stress test (`commit-bench/index_race_stress.cpp`): concurrent unique inserts + occasional deletes racing a CREATE UNIQUE INDEX / probe / DROP INDEX loop. The probe inserts a duplicate of the newest visible row right after each successful index build — if it succeeds, the "unique" index silently misses a committed row.

**Covered by the existing design:** physical ART insertions happen at WAL-flush time (pre-marker, irrevocable together with the marker); readers MVCC-filter early entries; publish-time delete removals recompute their `ActiveTransactionState` under the re-acquired lock; DROP INDEX / ADD PRIMARY KEY are catalog commits (synchronous + drained; ADD PK versions the table entry through the gated `AlterEntry`); index vacuum is excluded by the transaction-lifetime vacuum lock.

**New gate:** `PhysicalCreateIndex::Finalize` now takes the publish gate (`BlockPendingCommits`) before `storage.AddIndex` — attaching a new index to the live table is the index analog of a catalog version attachment. Acquired after the catalog operations in `Finalize`, which re-acquire the gate internally (it is not re-entrant). Index (142 tests) and alter suites pass.

**UPSTREAM BUG FOUND (reproduces on pristine `main`, no group commit involved):** a `CREATE UNIQUE INDEX` built while other connections commit inserts silently misses rows that commit during the build — such rows are invisible to the build scan AND their flush does not insert into the not-yet-attached index. Result: a unique index that accepts duplicates of committed rows and misses index-scan results. The stress test triggers it within seconds on `main` (4 writers, 12 s, exit code 2), and equally on the branch with group commit off/on, gate or no gate — the Finalize gate cannot fix it because the root cause is the index build protocol (build-scan visibility vs index-list attachment timing). Candidate upstream fixes: include flushed-but-uncommitted physical rows in the build scan (their revert already removes them from all listed indexes), or detect table modifications overlapping the build and abort the CREATE INDEX with a transaction conflict. Reported: https://github.com/duckdblabs/motherduck/issues/559 (minimal reproducer: `commit-bench/create_index_repro.cpp`).

### Remaining open questions for productionizing

1. **fsync/publish failure after the marker is durable** is `FatalException` — principled (matches Postgres PANIC semantics), but should be reviewed; the validation pass is the safeguard that keeps it unreachable.
2. ~~Micro-wait tuning~~ — resolved (2026-06-12): new `experimental_group_commit_delay` setting (µs; `0` = no wait, `-1` = automatic, the default). Automatic scales the leader's accumulation window to 1/4 of an EWMA of the observed fsync duration, with a deadline + early-exit when appends stop; the `sync_waiters` trigger is the `commit_siblings` analog and stays. Unlike PostgreSQL's `commit_delay`, the wait happens while appenders can still stream in (only sync leadership is held), so the delay is productive. Measured: no change at 1 thread (221 TPS) or ms-class latency (2619 TPS @ 16 threads F_FULLFSYNC, 1097 @ 16 threads/10 ms), and on slow storage the latency-proportional window pays off: 64 threads @ 30 ms went from 1041 TPS / 61.5 ms to **1467 TPS / 43.6 ms** (batching efficiency ~50% → ~78%). Tuning note: 1/8 of the fsync was measurably too short on macOS (timer overshoot eats the first poll; 1467 TPS regressed at 16 threads F_FULLFSYNC) — 1/4 matches the previous fixed window on fast syncs while scaling up on slow ones. `0` reverts to plain leader/follower (~2× fsync latency under lockstep, 766 TPS @ 16 threads).
3. The optimistic-row-group `FileSync` on the DB file (`duck_transaction.cpp` in `WriteToWAL`) is still per-transaction, under the WAL lock.
4. ~~`ValidateCommitConflicts` duplicates the conflict conditions in `CommitState::CommitEntry`~~ — resolved: shared via `CommitState::VerifyTableModification`.
5. ~~Out-of-tree storage extensions implementing `StorageCommitState` need a signature change~~ — resolved: `FlushCommit()` kept its original signature; `FlushCommitMarker()` is a new virtual with a durable default.
6. `make test_configs` / full extensive CI sweep not yet run; thread-sanitizer run advisable for the condvar protocols.
7. The upstream CREATE INDEX race above needs an upstream fix (tracked in https://github.com/duckdblabs/motherduck/issues/559); until then, concurrent CREATE INDEX + writes can corrupt unique indexes regardless of group commit.
