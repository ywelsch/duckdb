#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <condition_variable>
#include <thread>

using namespace duckdb;
using namespace std;

namespace {

//! A file system that blocks (once) on the first WAL write after being armed, so a test can deterministically hold a
//! transaction inside its commit: its rows are already physically flushed into the table, but its commit outcome is
//! not yet decided.
class WALBlockingFileSystem : public LocalFileSystem {
public:
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		MaybeBlock(handle);
		LocalFileSystem::Write(handle, buffer, nr_bytes, location);
	}
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		MaybeBlock(handle);
		return LocalFileSystem::Write(handle, buffer, nr_bytes);
	}

	void Arm() {
		armed = true;
	}
	void WaitUntilBlocked() {
		unique_lock<mutex> guard(lock);
		cv.wait(guard, [&]() { return blocked; });
	}
	void Resume() {
		{
			lock_guard<mutex> guard(lock);
			resume = true;
		}
		cv.notify_all();
	}

private:
	void MaybeBlock(FileHandle &handle) {
		if (!armed || !StringUtil::EndsWith(handle.GetPath(), ".wal")) {
			return;
		}
		armed = false;
		unique_lock<mutex> guard(lock);
		blocked = true;
		cv.notify_all();
		cv.wait(guard, [&]() { return resume; });
	}

	atomic<bool> armed {false};
	mutex lock;
	condition_variable cv;
	bool blocked = false;
	bool resume = false;
};

} // namespace

// An ALTER TABLE must not base the new table state on rows of an in-flight commit. If it does, that commit is doomed
// (the table version changed) and its revert truncates the column data shared with the altered table. Any rows the
// altering transaction then flushes land in the truncated shared trees below where its row groups account for them,
// and its own rollback does not remove them - resurrecting rolled-back rows into the table.
TEST_CASE("ALTER TABLE concurrent with an in-flight commit must not corrupt the table", "[api]") {
	auto db_path = TestCreatePath("alter_inflight_commit_race.db");
	DeleteDatabase(db_path);

	DBConfig config;
	auto fs = make_uniq<WALBlockingFileSystem>();
	auto &wal_fs = *fs;
	config.file_system = make_uniq<VirtualFileSystem>(std::move(fs));

	DuckDB db(db_path, &config);
	Connection con_alter(db), con_insert(db), con_check(db);

	REQUIRE_NO_FAIL(con_check.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(con_check.Query(
	    "CREATE TABLE t AS SELECT range::INTEGER AS i, md5(range::VARCHAR)::VARCHAR AS v FROM range(200)"));

	// the inserting connection commits 500 rows: the commit physically appends them into the table, then blocks on
	// the WAL write, before the commit's outcome is decided
	wal_fs.Arm();
	thread insert_thread([&]() { con_insert.Query("INSERT INTO t SELECT 9999, repeat('y', 32) FROM range(500)"); });
	wal_fs.WaitUntilBlocked();

	// the altering transaction runs while that commit is in flight, and appends rows of its own
	atomic<bool> alter_txn_ready {false};
	bool commit_succeeded = false;
	thread alter_thread([&]() {
		con_alter.Query("BEGIN");
		con_alter.Query("ALTER TABLE t ADD COLUMN extra INTEGER");
		con_alter.Query("INSERT INTO t (i, v) SELECT 7777, 'ROLLED-BACK' FROM range(10)");
		alter_txn_ready = true;
		auto result = con_alter.Query("COMMIT");
		commit_succeeded = !result->HasError();
	});

	// under a correct implementation the ALTER may block until the in-flight commit resolves (alter_txn_ready then
	// never becomes true before the resume); under a broken one it proceeds against the in-flight state
	for (idx_t i = 0; i < 200 && !alter_txn_ready; i++) {
		this_thread::sleep_for(chrono::milliseconds(10));
	}
	wal_fs.Resume();
	insert_thread.join();
	alter_thread.join();

	// a fresh insert makes any divergence between the table's row accounting and the shared column data visible
	auto ins_result = con_check.Query("INSERT INTO t (i, v) SELECT 1, 'x' FROM range(10)");
	if (ins_result->HasError()) {
		fprintf(stderr, "FOLLOW-UP INSERT ERROR: %s\n", ins_result->GetError().c_str());
	}
	REQUIRE_NO_FAIL(*ins_result);

	// if the altering transaction failed, none of its rows may be visible
	auto result = con_check.Query("SELECT count(*) FROM t WHERE v = 'ROLLED-BACK'");
	REQUIRE_NO_FAIL(*result);
	auto resurrected = result->GetValue(0, 0).GetValue<int64_t>();
	if (commit_succeeded) {
		REQUIRE(resurrected == 10);
	} else {
		REQUIRE(resurrected == 0);
	}

	// every row must be intact: 200 seed rows (md5, length 32), plus batches of 'y'/'x'/marker rows (lengths 32/1/11)
	result = con_check.Query("SELECT count(*) FROM t WHERE v IS NULL OR length(v) NOT IN (1, 11, 32)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 0);
}
