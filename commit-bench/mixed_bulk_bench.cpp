// Measures the impact of the per-transaction DB-file FileSync for optimistically written row groups:
// N small committers (single-row inserts, latency tracked per commit) run concurrently with an optional
// bulk writer whose transactions are large enough to trigger optimistic row group writes.
//
// Usage: mixed_bulk_bench <db_path> <small_writers> <bulk_rows_per_txn> <seconds>
//        (bulk_rows_per_txn = 0 disables the bulk writer)
#include "duckdb.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;

int main(int argc, char **argv) {
	if (argc != 5) {
		fprintf(stderr, "Usage: %s <db_path> <small_writers> <bulk_rows_per_txn> <seconds>\n", argv[0]);
		return 1;
	}
	std::string db_path = argv[1];
	idx_t num_small = std::stoull(argv[2]);
	idx_t bulk_rows = std::stoull(argv[3]);
	int seconds = std::stoi(argv[4]);

	DuckDB db(db_path);
	{
		Connection con(db);
		con.Query("SET checkpoint_threshold='100GB'");
		if (getenv("DUCKDB_BENCH_GROUP_COMMIT")) {
			auto res = con.Query("SET experimental_group_commit=true");
			if (res->HasError()) {
				fprintf(stderr, "enabling group commit failed: %s\n", res->GetError().c_str());
				return 1;
			}
		}
		con.Query("CREATE TABLE small (i BIGINT)");
		con.Query("CREATE TABLE bulk (i BIGINT)");
	}

	std::atomic<bool> stop {false};
	std::atomic<bool> failed {false};
	std::atomic<idx_t> bulk_commits {0};
	std::mutex latency_lock;
	std::vector<double> latencies_ms; // all small-commit latencies

	std::vector<std::thread> threads;
	for (idx_t w = 0; w < num_small; w++) {
		threads.emplace_back([&]() {
			Connection con(db);
			std::vector<double> local;
			while (!stop.load()) {
				auto start = std::chrono::steady_clock::now();
				auto res = con.Query("INSERT INTO small (i) VALUES (1)");
				auto ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
				if (res->HasError()) {
					fprintf(stderr, "small insert failed: %s\n", res->GetError().c_str());
					failed = true;
					stop = true;
					return;
				}
				local.push_back(ms);
			}
			std::lock_guard<std::mutex> guard(latency_lock);
			latencies_ms.insert(latencies_ms.end(), local.begin(), local.end());
		});
	}

	if (bulk_rows > 0) {
		threads.emplace_back([&]() {
			Connection con(db);
			while (!stop.load()) {
				auto res = con.Query("INSERT INTO bulk SELECT range FROM range(" + std::to_string(bulk_rows) + ")");
				if (res->HasError()) {
					fprintf(stderr, "bulk insert failed: %s\n", res->GetError().c_str());
					failed = true;
					stop = true;
					return;
				}
				bulk_commits++;
			}
		});
	}

	std::this_thread::sleep_for(std::chrono::seconds(seconds));
	stop = true;
	for (auto &t : threads) {
		t.join();
	}
	if (failed.load()) {
		return 1;
	}

	std::sort(latencies_ms.begin(), latencies_ms.end());
	auto n = latencies_ms.size();
	double sum = 0;
	for (auto v : latencies_ms) {
		sum += v;
	}
	printf("small_commits=%zu tps=%.1f avg=%.2fms p50=%.2fms p99=%.2fms max=%.2fms bulk_commits=%llu\n", n,
	       n / (double)seconds, sum / static_cast<double>(n), latencies_ms[n / 2], latencies_ms[(n * 99) / 100],
	       latencies_ms[n - 1], (unsigned long long)bulk_commits.load());
	return 0;
}
