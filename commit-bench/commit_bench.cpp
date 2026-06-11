// Prototype benchmark: N threads doing single-row autocommit INSERTs against a
// persistent DuckDB database. Measures commit throughput (TPS).
//
// Usage: commit_bench <db_path> <num_threads> <inserts_per_thread>
#include "duckdb.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;

int main(int argc, char **argv) {
	if (argc != 4) {
		fprintf(stderr, "Usage: %s <db_path> <num_threads> <inserts_per_thread>\n", argv[0]);
		return 1;
	}
	std::string db_path = argv[1];
	idx_t num_threads = std::stoull(argv[2]);
	idx_t inserts_per_thread = std::stoull(argv[3]);

	DuckDB db(db_path);
	{
		Connection con(db);
		// avoid automatic checkpoints interfering with the measurement
		auto res = con.Query("SET checkpoint_threshold='100GB'");
		if (res->HasError()) {
			fprintf(stderr, "setup failed: %s\n", res->GetError().c_str());
			return 1;
		}
		if (getenv("DUCKDB_BENCH_GROUP_COMMIT")) {
			res = con.Query("SET experimental_group_commit=true");
			if (res->HasError()) {
				fprintf(stderr, "enabling group commit failed: %s\n", res->GetError().c_str());
				return 1;
			}
		}
		con.Query("CREATE TABLE t (i BIGINT)");
	}

	std::atomic<bool> start_flag {false};
	std::atomic<idx_t> ready_count {0};
	std::atomic<bool> failed {false};

	std::vector<std::thread> threads;
	for (idx_t t = 0; t < num_threads; t++) {
		threads.emplace_back([&, t]() {
			Connection con(db);
			auto prepared = con.Prepare("INSERT INTO t VALUES ($1)");
			if (prepared->HasError()) {
				fprintf(stderr, "prepare failed: %s\n", prepared->GetError().c_str());
				failed = true;
				return;
			}
			ready_count++;
			while (!start_flag.load()) {
				std::this_thread::yield();
			}
			for (idx_t i = 0; i < inserts_per_thread; i++) {
				auto res = prepared->Execute(static_cast<int64_t>(t * inserts_per_thread + i));
				if (res->HasError()) {
					fprintf(stderr, "insert failed: %s\n", res->GetError().c_str());
					failed = true;
					return;
				}
			}
		});
	}

	while (ready_count.load() < num_threads) {
		std::this_thread::yield();
	}
	auto begin = std::chrono::steady_clock::now();
	start_flag = true;
	for (auto &thread : threads) {
		thread.join();
	}
	auto end = std::chrono::steady_clock::now();
	if (failed) {
		return 1;
	}

	double seconds = std::chrono::duration<double>(end - begin).count();
	idx_t total = num_threads * inserts_per_thread;

	// sanity check the row count
	Connection con(db);
	auto res = con.Query("SELECT COUNT(*) FROM t");
	auto count = res->GetValue(0, 0).GetValue<int64_t>();
	if (static_cast<idx_t>(count) != total) {
		fprintf(stderr, "row count mismatch: expected %llu got %lld\n", (unsigned long long)total, (long long)count);
		return 1;
	}

	printf("threads=%llu inserts_per_thread=%llu total=%llu time=%.3fs tps=%.1f avg_commit_latency_ms=%.3f\n",
	       (unsigned long long)num_threads, (unsigned long long)inserts_per_thread, (unsigned long long)total, seconds,
	       total / seconds, seconds * 1000.0 * num_threads / total);
	return 0;
}
