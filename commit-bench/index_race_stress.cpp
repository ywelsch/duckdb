// Stress test for CREATE/DROP INDEX racing concurrent (group) commits: writer threads insert unique
// values (and occasionally delete old ones) while another connection repeatedly creates and drops a
// UNIQUE index on the same column.
//
// Corruption probe: right after a successful CREATE UNIQUE INDEX, inserting a duplicate of the newest
// visible row MUST fail with a constraint violation. If it succeeds, the index is silently missing a
// committed row (e.g. a row whose commit published during the index build) - silent constraint
// violations and missed index-scan results.
//
// Usage: index_race_stress <db_path> <writer_threads> <seconds>
#include "duckdb.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;

int main(int argc, char **argv) {
	if (argc != 4) {
		fprintf(stderr, "Usage: %s <db_path> <writer_threads> <seconds>\n", argv[0]);
		return 1;
	}
	std::string db_path = argv[1];
	idx_t num_writers = std::stoull(argv[2]);
	int seconds = std::stoi(argv[3]);

	DuckDB db(db_path);
	{
		Connection con(db);
		con.Query("SET checkpoint_threshold='100GB'");
		if (getenv("DUCKDB_BENCH_GROUP_COMMIT")) {
			auto set_res = con.Query("SET experimental_group_commit=true");
			if (set_res->HasError()) {
				fprintf(stderr, "enabling group commit failed: %s\n", set_res->GetError().c_str());
				return 1;
			}
		}
		auto res = con.Query("CREATE TABLE t (i BIGINT)");
		if (res->HasError()) {
			fprintf(stderr, "setup failed: %s\n", res->GetError().c_str());
			return 1;
		}
	}

	std::atomic<bool> stop {false};
	std::atomic<bool> failed {false};
	std::atomic<bool> corruption {false};
	std::atomic<idx_t> inserts {0};
	std::atomic<idx_t> deletes {0};
	std::atomic<idx_t> idx_created {0};
	std::atomic<idx_t> idx_failed {0};
	std::atomic<idx_t> probes {0};

	std::vector<std::thread> threads;
	for (idx_t w = 0; w < num_writers; w++) {
		threads.emplace_back([&, w]() {
			Connection con(db);
			// each writer inserts strictly unique, monotonically increasing values
			int64_t base = static_cast<int64_t>(w) * 1000000000LL;
			int64_t next = 0;
			int64_t oldest = 0;
			while (!stop.load()) {
				auto res = con.Query("INSERT INTO t (i) VALUES (" + std::to_string(base + next) + ")");
				if (res->HasError()) {
					fprintf(stderr, "insert failed: %s\n", res->GetError().c_str());
					failed = true;
					stop = true;
					return;
				}
				next++;
				inserts++;
				if (next % 50 == 0) {
					// occasionally delete our oldest row (exercises publish-time index removals)
					res = con.Query("DELETE FROM t WHERE i = " + std::to_string(base + oldest));
					if (res->HasError()) {
						fprintf(stderr, "delete failed: %s\n", res->GetError().c_str());
						failed = true;
						stop = true;
						return;
					}
					oldest++;
					deletes++;
				}
			}
		});
	}

	// index thread: repeatedly create + probe + drop a unique index on the column being written
	threads.emplace_back([&]() {
		Connection con(db);
		auto is_transient = [](const std::string &msg) {
			return msg.find("Transaction conflict") != std::string::npos ||
			       msg.find("Catalog write-write conflict") != std::string::npos ||
			       msg.find("another transaction has") != std::string::npos;
		};
		while (!stop.load()) {
			auto res = con.Query("CREATE UNIQUE INDEX idx ON t (i)");
			if (res->HasError()) {
				auto msg = res->GetError();
				if (msg.find("Duplicate key") != std::string::npos ||
				    msg.find("duplicate key") != std::string::npos) {
					// writers only produce unique values: a duplicate during the build means the build scan saw
					// a row twice - also corruption
					fprintf(stderr, "CORRUPTION (duplicate during build): %s\n", msg.c_str());
					corruption = true;
					stop = true;
					return;
				}
				if (!is_transient(msg)) {
					fprintf(stderr, "unexpected create index error: %s\n", msg.c_str());
					failed = true;
					stop = true;
					return;
				}
				idx_failed++;
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
				continue;
			}
			idx_created++;

			// corruption probe: a duplicate of the newest visible row must violate the unique index
			res = con.Query("INSERT INTO t SELECT MAX(i) FROM t");
			probes++;
			if (!res->HasError()) {
				fprintf(stderr, "CORRUPTION: duplicate insert of newest visible row succeeded - the unique index "
				                "is missing a committed row\n");
				corruption = true;
				stop = true;
				return;
			}
			auto msg = res->GetError();
			if (msg.find("violates unique constraint") == std::string::npos &&
			    msg.find("Duplicate key") == std::string::npos && msg.find("duplicate key") == std::string::npos &&
			    msg.find("Constraint Error") == std::string::npos) {
				fprintf(stderr, "unexpected probe error (expected constraint violation): %s\n", msg.c_str());
				failed = true;
				stop = true;
				return;
			}

			res = con.Query("DROP INDEX idx");
			if (res->HasError() && !is_transient(res->GetError())) {
				fprintf(stderr, "unexpected drop index error: %s\n", res->GetError().c_str());
				failed = true;
				stop = true;
				return;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	});

	std::this_thread::sleep_for(std::chrono::seconds(seconds));
	stop = true;
	for (auto &t : threads) {
		t.join();
	}
	if (corruption.load()) {
		fprintf(stderr, "RESULT: CORRUPTION DETECTED\n");
		return 2;
	}
	if (failed.load()) {
		fprintf(stderr, "RESULT: FAILED\n");
		return 1;
	}

	// quiesced verification: counts must match, and a final unique index must build cleanly
	Connection con(db);
	con.Query("DROP INDEX IF EXISTS idx");
	auto res = con.Query("SELECT COUNT(*) FROM t");
	auto count = res->GetValue(0, 0).GetValue<int64_t>();
	auto expected = static_cast<int64_t>(inserts.load()) - static_cast<int64_t>(deletes.load());
	if (count != expected) {
		fprintf(stderr, "row count mismatch: expected %lld got %lld\n", (long long)expected, (long long)count);
		return 1;
	}
	res = con.Query("CREATE UNIQUE INDEX final_idx ON t (i)");
	if (res->HasError()) {
		fprintf(stderr, "final unique index build failed (physical duplicates?): %s\n", res->GetError().c_str());
		return 1;
	}

	printf("OK: inserts=%llu deletes=%llu idx_created=%llu idx_failed=%llu probes=%llu final_count=%lld\n",
	       (unsigned long long)inserts.load(), (unsigned long long)deletes.load(),
	       (unsigned long long)idx_created.load(), (unsigned long long)idx_failed.load(),
	       (unsigned long long)probes.load(), (long long)count);
	return 0;
}
