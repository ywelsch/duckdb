// Reproducer: rows committed during CREATE INDEX are silently missing from the new index.
//
// Writer threads insert strictly unique values (autocommit) while the main thread repeatedly creates a
// UNIQUE index on the same column and probes it: inserting a duplicate of the newest visible row must
// fail with a constraint violation. If the insert succeeds, the freshly built unique index is missing a
// committed row.
//
// Build (from a DuckDB checkout with a release build):
//   clang++ -O2 -std=c++17 -Isrc/include -Ithird_party/fmt/include create_index_repro.cpp \
//           -Lbuild/release/src -lduckdb -Wl,-rpath,build/release/src -o create_index_repro
// Run:
//   ./create_index_repro
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
		auto res = con.Query("CREATE TABLE t (i BIGINT)");
		if (res->HasError()) {
			fprintf(stderr, "setup failed: %s\n", res->GetError().c_str());
			return 1;
		}
	}

	std::atomic<bool> stop {false};
	const idx_t num_writers = 4;
	std::vector<std::thread> writers;
	for (idx_t w = 0; w < num_writers; w++) {
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
			// e.g. transient conflicts - retry
			con.Query("DROP INDEX IF EXISTS idx");
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			continue;
		}
		// PROBE: all values in t are unique, and the unique index was just created successfully.
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
