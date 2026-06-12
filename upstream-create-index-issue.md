# DRAFT upstream issue (github.com/duckdb/duckdb)

## Title

Rows committed during `CREATE INDEX` are missing from the new index — a concurrent `CREATE UNIQUE INDEX` silently accepts duplicates afterwards

## What happens

When a `CREATE INDEX` (including `CREATE UNIQUE INDEX`) runs while other connections are concurrently committing inserts into the same table, rows whose commits overlap the index build can end up **missing from the newly created index**:

- they are not visible to the index build's table scan (they commit after the scan passes / after the build transaction's snapshot), and
- their commit does not insert them into the new index either, because the index is only attached to the table (`storage.AddIndex`) at the very end of `PhysicalCreateIndex::Finalize` — after those commits have already added their rows to the indexes that were attached at commit time.

The result is a structurally valid but **incomplete** index:

- a `UNIQUE` index accepts duplicates of committed rows (silent constraint violation), and
- index scans miss committed rows (wrong query results).

There is no error or conflict on either side — the `CREATE INDEX` succeeds, the concurrent inserts succeed, and the corruption is silent. The only concurrency guard in `Finalize` is the `storage.IsMainTable()` check, which catches the table being altered/dropped but not concurrent DML commits.

## To reproduce

Reproduces reliably within seconds (typically < 5 `CREATE INDEX` attempts) on `main` @ `73a804ef72` (`v1.6.0-dev8496`), macOS 15.7.7 arm64, release build from source. Four writer threads insert strictly unique values via autocommit while the main thread repeatedly creates a `UNIQUE` index and probes it: since all values in the table are unique and the unique index was just created successfully, inserting a duplicate of the newest visible row must fail — if it succeeds, the index is missing a committed row.

```cpp
// clang++ -O2 -std=c++17 -Isrc/include -Ithird_party/fmt/include create_index_repro.cpp \
//         -Lbuild/release/src -lduckdb -Wl,-rpath,build/release/src -o create_index_repro
#include "duckdb.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;

int main() {
	DuckDB db("create_index_repro.db");
	{
		Connection con(db);
		con.Query("SET checkpoint_threshold='100GB'");
		con.Query("CREATE TABLE t (i BIGINT)");
	}

	std::atomic<bool> stop {false};
	std::vector<std::thread> writers;
	for (idx_t w = 0; w < 4; w++) {
		writers.emplace_back([&, w]() {
			Connection con(db);
			// every writer inserts strictly unique values: writer w inserts w*10^9 + 0, 1, 2, ...
			int64_t base = static_cast<int64_t>(w) * 1000000000LL;
			int64_t next = 0;
			while (!stop.load()) {
				auto res = con.Query("INSERT INTO t (i) VALUES (" + std::to_string(base + next) + ")");
				if (res->HasError()) {
					fprintf(stderr, "unexpected insert error: %s\n", res->GetError().c_str());
					stop = true;
					return;
				}
				next++;
			}
		});
	}

	int result = 0;
	Connection con(db);
	for (int attempt = 1; attempt <= 200; attempt++) {
		auto res = con.Query("CREATE UNIQUE INDEX idx ON t (i)");
		if (res->HasError()) {
			con.Query("DROP INDEX IF EXISTS idx");
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			continue;
		}
		// PROBE: all values in t are unique and the unique index was just created successfully.
		// Inserting a duplicate of any visible row must therefore fail with a constraint violation.
		auto probe = con.Query("INSERT INTO t SELECT MAX(i) FROM t");
		if (!probe->HasError()) {
			printf("CORRUPTION after %d attempts: inserted a duplicate of the newest visible row - the "
			       "freshly built UNIQUE index is missing a committed row\n",
			       attempt);
			result = 2;
			break;
		}
		con.Query("DROP INDEX idx");
	}
	if (result == 0) {
		printf("no corruption detected in 200 attempts\n");
	}

	stop = true;
	for (auto &t : writers) {
		t.join();
	}
	return result;
}
```

Observed output:

```
CORRUPTION after 3 attempts: inserted a duplicate of the newest visible row - the freshly built UNIQUE index is missing a committed row
```

## Expected behavior

Either the new index contains every row that is committed by the time `CREATE INDEX` returns, or one of the two sides fails with a transaction conflict (the established behavior for e.g. `ALTER TABLE` vs concurrent DML: "another transaction has altered this table").

## Analysis

The window is between the index build's table scan and the index attachment:

1. The build pipeline scans the table with the build transaction's snapshot visibility (`PhysicalCreateIndex::Sink`). Rows committed after the scan/snapshot are not included.
2. A concurrent commit flushes its rows into the table and into the indexes listed in the table's `TableIndexList` *at that moment* — the new index is not attached yet, so it receives nothing.
3. `PhysicalCreateIndex::Finalize` then attaches the (already incomplete) index via `storage.AddIndex(...)` (`src/execution/operator/schema/physical_create_index.cpp:174`).

Rows that commit between (1) and (3) are therefore in neither the build result nor the post-attachment maintenance path. `ADD PRIMARY KEY` takes a different path (it versions the table catalog entry via `catalog.Alter`, so concurrent DML commits abort with a transaction conflict), but plain `CREATE INDEX` does not version the table entry, so neither side detects the overlap.

## Possible directions

- Detect table data modifications overlapping the build (e.g. a per-table append/commit counter captured at build start and re-checked at attach) and abort the `CREATE INDEX` with a retryable `TransactionException` — consistent with the existing `IsMainTable()` conflict semantics.
- Or include flushed-but-uncommitted physical rows in the build scan, so that any row that can still commit is in the index (transaction rollback already removes reverted rows from all attached indexes).
- Or attach the index (empty or partially built) before the scan so commit-time maintenance covers it during the build, with the build filling in the snapshot prefix.

## Environment

- DuckDB `v1.6.0-dev8496`, `main` @ `73a804ef72`, built from source (release)
- macOS 15.7.7, arm64

Found while stress-testing unrelated WAL changes; the reproducer above runs against an unmodified checkout.
