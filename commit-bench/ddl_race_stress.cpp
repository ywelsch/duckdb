// Stress test for the DDL-vs-deferred-commit race: concurrent single-row inserts while another
// connection repeatedly ALTERs the same table. Every insert commit must either succeed or fail with
// the clean "another transaction has altered this table" TransactionException - a FatalException or
// "database has been invalidated" error means the publish gate failed.
//
// Usage: ddl_race_stress <db_path> <writer_threads> <seconds>
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
	std::atomic<bool> fatal {false};
	std::atomic<idx_t> committed {0};
	std::atomic<idx_t> clean_aborts {0};
	std::atomic<idx_t> ddl_ok {0};
	std::atomic<idx_t> ddl_conflicts {0};

	auto is_acceptable_dml_error = [](const std::string &msg) {
		return msg.find("another transaction has") != std::string::npos ||
		       msg.find("altered by a different transaction") != std::string::npos ||
		       msg.find("Transaction conflict") != std::string::npos ||
		       msg.find("Catalog write-write conflict") != std::string::npos ||
		       msg.find("does not exist") != std::string::npos;
	};

	std::vector<std::thread> threads;
	for (idx_t w = 0; w < num_writers; w++) {
		threads.emplace_back([&]() {
			Connection con(db);
			while (!stop.load()) {
				auto res = con.Query("INSERT INTO t (i) VALUES (1)");
				if (!res->HasError()) {
					committed++;
					continue;
				}
				auto msg = res->GetError();
				if (msg.find("FATAL") != std::string::npos || msg.find("invalidated") != std::string::npos) {
					fprintf(stderr, "FATAL ERROR on insert: %s\n", msg.c_str());
					fatal = true;
					stop = true;
					return;
				}
				if (!is_acceptable_dml_error(msg)) {
					fprintf(stderr, "unexpected insert error: %s\n", msg.c_str());
					fatal = true;
					stop = true;
					return;
				}
				clean_aborts++;
			}
		});
	}

	// DDL thread: alternately add and drop a column on the same table
	threads.emplace_back([&]() {
		Connection con(db);
		bool add = true;
		while (!stop.load()) {
			auto res = con.Query(add ? "ALTER TABLE t ADD COLUMN extra INTEGER" : "ALTER TABLE t DROP COLUMN extra");
			if (!res->HasError()) {
				ddl_ok++;
				add = !add;
			} else {
				auto msg = res->GetError();
				if (msg.find("FATAL") != std::string::npos || msg.find("invalidated") != std::string::npos) {
					fprintf(stderr, "FATAL ERROR on DDL: %s\n", msg.c_str());
					fatal = true;
					stop = true;
					return;
				}
				ddl_conflicts++;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}
	});

	std::this_thread::sleep_for(std::chrono::seconds(seconds));
	stop = true;
	for (auto &t : threads) {
		t.join();
	}

	if (fatal.load()) {
		fprintf(stderr, "RACE DETECTED: database hit a fatal error\n");
		return 1;
	}

	// verify the database is still fully functional
	Connection con(db);
	auto res = con.Query("SELECT COUNT(*) FROM t");
	if (res->HasError()) {
		fprintf(stderr, "post-check failed: %s\n", res->GetError().c_str());
		return 1;
	}
	auto count = res->GetValue(0, 0).GetValue<int64_t>();
	printf("OK: committed=%llu clean_aborts=%llu ddl_ok=%llu ddl_conflicts=%llu final_count=%lld\n",
	       (unsigned long long)committed.load(), (unsigned long long)clean_aborts.load(),
	       (unsigned long long)ddl_ok.load(), (unsigned long long)ddl_conflicts.load(), (long long)count);
	if (static_cast<idx_t>(count) != committed.load()) {
		fprintf(stderr, "row count mismatch: committed=%llu but count=%lld\n",
		        (unsigned long long)committed.load(), (long long)count);
		return 1;
	}
	return 0;
}
