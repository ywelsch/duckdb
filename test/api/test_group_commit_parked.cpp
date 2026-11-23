#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"

#include <chrono>
#include <condition_variable>
#include <thread>

using namespace duckdb;

// An explicit COMMIT that would wait for another commit's WAL sync parks its pipeline task instead of its thread.
// These tests check that it resumes, and that it completes when nobody resumes it
namespace {

//! Holds WAL fsyncs while armed, and fails them on request
class GatedFsyncFileSystem : public LocalFileSystem {
public:
	void FileSync(FileHandle &handle) override {
		if (StringUtil::Contains(handle.GetPath(), ".wal")) {
			unique_lock<mutex> guard(lock);
			if (armed) {
				parked = true;
				cv.notify_all();
				cv.wait(guard, [&]() { return released; });
				if (fail) {
					throw IOException("injected WAL fsync failure");
				}
			}
		}
		LocalFileSystem::FileSync(handle);
	}
	void Arm() {
		lock_guard<mutex> guard(lock);
		armed = true;
		parked = false;
		released = false;
		fail = false;
	}
	bool WaitUntilParked() {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(60), [&]() { return parked; });
	}
	void Release(bool fail_sync = false) {
		{
			lock_guard<mutex> guard(lock);
			released = true;
			armed = false;
			fail = fail_sync;
		}
		cv.notify_all();
	}

private:
	mutex lock;
	std::condition_variable cv;
	bool armed = false;
	bool parked = false;
	bool released = false;
	bool fail = false;
};

struct GatedDatabase {
	explicit GatedDatabase(const string &name) {
		auto path = TestCreatePath(name);
		DeleteDatabase(path);
		auto fs = make_uniq<GatedFsyncFileSystem>();
		gate = fs.get();
		DBConfig config;
		config.file_system = make_uniq<VirtualFileSystem>(std::move(fs));
		db = make_uniq<DuckDB>(path, &config);
	}
	~GatedDatabase() {
		FinishWriter();
	}

	//! Runs an autocommit query on another thread whose WAL sync holds in the file system
	void StartWriter(const string &query) {
		// keep another transaction open, or the writer syncs under the transaction lock
		holder = make_uniq<Connection>(*db);
		REQUIRE_NO_FAIL(holder->Query("BEGIN"));
		REQUIRE_NO_FAIL(holder->Query("SELECT count(*) FROM t"));
		gate->Arm();
		writer = std::thread([this, query]() {
			Connection con(*db);
			writer_failed = con.Query(query)->HasError();
		});
		REQUIRE(gate->WaitUntilParked());
	}
	bool FinishWriter(bool fail_sync = false) {
		gate->Release(fail_sync);
		if (writer.joinable()) {
			writer.join();
		}
		if (holder) {
			holder->Query("ROLLBACK");
			holder.reset();
		}
		return !writer_failed;
	}

	unique_ptr<DuckDB> db;
	GatedFsyncFileSystem *gate;
	unique_ptr<Connection> holder;
	std::thread writer;
	bool writer_failed = false;
};

idx_t ScalarValue(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	return result->Collection().GetValue(0, 0).GetValue<idx_t>();
}

void Setup(Connection &con) {
	// only the calling thread runs a submitted query's tasks
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));
}

//! Submits COMMIT and runs it until its task parks
unique_ptr<QueryResult> SubmitParkedCommit(Connection &con) {
	auto result = con.Submit("COMMIT");
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
	while (std::chrono::steady_clock::now() < deadline) {
		auto state = result->ExecuteTask();
		if (state == QueryResultState::BLOCKED) {
			return result;
		}
		REQUIRE(state != QueryResultState::FINISHED);
		REQUIRE(state != QueryResultState::EXECUTION_ERROR);
		REQUIRE(state != QueryResultState::READY);
	}
	FAIL("the COMMIT did not park");
	return result;
}

} // namespace

TEST_CASE("A checkpoint completes a parked COMMIT that nobody resumes", "[api][group_commit]") {
	GatedDatabase gated("group_commit_parked_checkpoint.db");
	Connection setup(*gated.db);
	Setup(setup);

	Connection committer(*gated.db);
	REQUIRE_NO_FAIL(committer.Query("BEGIN"));
	REQUIRE_NO_FAIL(committer.Query("INSERT INTO t VALUES (1000)"));

	gated.StartWriter("INSERT INTO t SELECT * FROM range(100)");
	auto commit = SubmitParkedCommit(committer);

	// the writer's sync does not cover the parked commit; with no worker threads its task is not run again
	REQUIRE(gated.FinishWriter());
	REQUIRE(ScalarValue(setup, "SELECT count(*) FROM t") == 100);
	// the checkpoint drains the WAL before replacing it: it must sync for the parked commit, not wait for it
	REQUIRE_NO_FAIL(setup.Query("CHECKPOINT"));
	REQUIRE(ScalarValue(setup, "SELECT count(*) FROM t") == 101);

	commit->Complete();
	REQUIRE_NO_FAIL(*commit);
}

TEST_CASE("A failed WAL sync fails the COMMIT parked behind it", "[api][group_commit]") {
	GatedDatabase gated("group_commit_parked_failure.db");
	Connection setup(*gated.db);
	Setup(setup);

	Connection committer(*gated.db);
	REQUIRE_NO_FAIL(committer.Query("BEGIN"));
	REQUIRE_NO_FAIL(committer.Query("INSERT INTO t VALUES (1000)"));

	gated.StartWriter("INSERT INTO t SELECT * FROM range(100)");
	auto commit = SubmitParkedCommit(committer);

	REQUIRE(!gated.FinishWriter(true));
	commit->Complete();
	REQUIRE(commit->HasError());
	// the database is invalidated
	REQUIRE_FAIL(setup.Query("SELECT count(*) FROM t"));
}

TEST_CASE("A parked COMMIT blocks new transactions on its connection and completes when abandoned",
          "[api][group_commit]") {
	GatedDatabase gated("group_commit_parked_abandoned.db");
	Connection setup(*gated.db);
	Setup(setup);

	Connection committer(*gated.db);
	REQUIRE_NO_FAIL(committer.Query("BEGIN"));
	REQUIRE_NO_FAIL(committer.Query("INSERT INTO t VALUES (1000)"));

	gated.StartWriter("INSERT INTO t SELECT * FROM range(100)");
	auto commit = SubmitParkedCommit(committer);

	// runs in a transaction without ending the open query: refused, and the COMMIT is unaffected
	REQUIRE_THROWS(committer.TableInfo("t"));

	REQUIRE(gated.FinishWriter());
	// still parked: snapshots stop below the commit until it is durable
	REQUIRE(ScalarValue(setup, "SELECT count(*) FROM t") == 100);
	// the client drops the result: the parked COMMIT is abandoned, but it is irreversible
	commit.reset();
	REQUIRE(ScalarValue(committer, "SELECT count(*) FROM t") == 101);
	REQUIRE(committer.TableInfo("t"));
}

TEST_CASE("A parked COMMIT resumes the commit of the remaining databases", "[api][group_commit]") {
	GatedDatabase gated("group_commit_parked_attached.db");
	Connection setup(*gated.db);
	Setup(setup);
	auto other_path = TestCreatePath("group_commit_parked_attached_other.db");
	DeleteDatabase(other_path);
	REQUIRE_NO_FAIL(setup.Query("ATTACH '" + other_path + "' AS other"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE other.o AS SELECT 1 AS i"));

	Connection committer(*gated.db);
	REQUIRE_NO_FAIL(committer.Query("BEGIN"));
	// databases commit in reverse order of use: the write to t commits and parks before temp and other commit
	REQUIRE_NO_FAIL(committer.Query("CREATE TEMP TABLE tt AS SELECT 7 AS i"));
	REQUIRE(ScalarValue(committer, "SELECT count(*) FROM other.o") == 1);
	REQUIRE_NO_FAIL(committer.Query("INSERT INTO t VALUES (1000)"));

	gated.StartWriter("INSERT INTO t SELECT * FROM range(100)");
	auto commit = SubmitParkedCommit(committer);

	REQUIRE(gated.FinishWriter());
	commit->Complete();
	REQUIRE_NO_FAIL(*commit);
	REQUIRE(ScalarValue(setup, "SELECT count(*) FROM t") == 101);
	// the temp database committed after the resume
	REQUIRE(ScalarValue(committer, "SELECT i FROM tt") == 7);
	REQUIRE(ScalarValue(committer, "SELECT count(*) FROM other.o") == 1);
}
