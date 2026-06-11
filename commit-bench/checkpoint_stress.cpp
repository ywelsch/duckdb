// Stress test for WAL swaps (checkpoints) concurrent with group commit: writer threads doing
// single-row autocommit inserts while (1) a low checkpoint_threshold triggers frequent automatic
// checkpoints and (2) a dedicated thread issues manual CHECKPOINTs. Inserts must never fail, the
// process must not hang, and after a clean reopen the row count must match the acknowledged commits.
//
// Usage: checkpoint_stress <db_path> <writer_threads> <seconds>
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

	std::atomic<idx_t> committed {0};
	std::atomic<idx_t> checkpoints_ok {0};
	std::atomic<idx_t> checkpoints_rejected {0};

	{
		DuckDB db(db_path);
		{
			Connection con(db);
			// low threshold so that automatic checkpoints fire frequently during the run
			auto res = con.Query("SET checkpoint_threshold='128KB'");
			if (res->HasError()) {
				fprintf(stderr, "setup failed: %s\n", res->GetError().c_str());
				return 1;
			}
			res = con.Query("SET experimental_group_commit=true");
			if (res->HasError()) {
				fprintf(stderr, "enabling group commit failed: %s\n", res->GetError().c_str());
				return 1;
			}
			res = con.Query("CREATE TABLE t (i BIGINT)");
			if (res->HasError()) {
				fprintf(stderr, "setup failed: %s\n", res->GetError().c_str());
				return 1;
			}
		}

		std::atomic<bool> stop {false};
		std::atomic<bool> failed {false};

		std::vector<std::thread> threads;
		for (idx_t w = 0; w < num_writers; w++) {
			threads.emplace_back([&]() {
				Connection con(db);
				while (!stop.load()) {
					auto res = con.Query("INSERT INTO t (i) VALUES (1)");
					if (res->HasError()) {
						// no DDL in this test: inserts must never fail
						fprintf(stderr, "insert failed: %s\n", res->GetError().c_str());
						failed = true;
						stop = true;
						return;
					}
					committed++;
				}
			});
		}

		// checkpoint thread: manual CHECKPOINTs racing the automatic ones
		threads.emplace_back([&]() {
			Connection con(db);
			while (!stop.load()) {
				auto res = con.Query("CHECKPOINT");
				if (!res->HasError()) {
					checkpoints_ok++;
				} else {
					auto msg = res->GetError();
					// a manual checkpoint may be cleanly rejected while write transactions are active
					if (msg.find("other write transactions") == std::string::npos &&
					    msg.find("FATAL") == std::string::npos) {
						fprintf(stderr, "unexpected checkpoint error: %s\n", msg.c_str());
					}
					if (msg.find("FATAL") != std::string::npos || msg.find("invalidated") != std::string::npos) {
						failed = true;
						stop = true;
						return;
					}
					checkpoints_rejected++;
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(20));
			}
		});

		std::this_thread::sleep_for(std::chrono::seconds(seconds));
		stop = true;
		for (auto &t : threads) {
			t.join();
		}
		if (failed.load()) {
			fprintf(stderr, "FAILED during stress phase\n");
			return 1;
		}

		// in-process verification
		Connection con(db);
		auto res = con.Query("SELECT COUNT(*) FROM t");
		if (res->HasError()) {
			fprintf(stderr, "post-check failed: %s\n", res->GetError().c_str());
			return 1;
		}
		auto count = res->GetValue(0, 0).GetValue<int64_t>();
		if (static_cast<idx_t>(count) != committed.load()) {
			fprintf(stderr, "in-process row count mismatch: committed=%llu count=%lld\n",
			        (unsigned long long)committed.load(), (long long)count);
			return 1;
		}
		// database closes here (shutdown checkpoint)
	}

	// reopen: exercises checkpoint correctness + WAL replay
	DuckDB db2(db_path);
	Connection con(db2);
	auto res = con.Query("SELECT COUNT(*) FROM t");
	if (res->HasError()) {
		fprintf(stderr, "reopen check failed: %s\n", res->GetError().c_str());
		return 1;
	}
	auto count = res->GetValue(0, 0).GetValue<int64_t>();
	if (static_cast<idx_t>(count) != committed.load()) {
		fprintf(stderr, "post-reopen row count mismatch: committed=%llu count=%lld\n",
		        (unsigned long long)committed.load(), (long long)count);
		return 1;
	}
	// database must still be fully usable
	res = con.Query("INSERT INTO t (i) VALUES (2)");
	if (res->HasError()) {
		fprintf(stderr, "post-reopen insert failed: %s\n", res->GetError().c_str());
		return 1;
	}

	printf("OK: committed=%llu checkpoints_ok=%llu checkpoints_rejected=%llu reopened_count=%lld\n",
	       (unsigned long long)committed.load(), (unsigned long long)checkpoints_ok.load(),
	       (unsigned long long)checkpoints_rejected.load(), (long long)count);
	return 0;
}
