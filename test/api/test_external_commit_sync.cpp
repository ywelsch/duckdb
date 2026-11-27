#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

#include <atomic>
#include <chrono>
#include <thread>

using namespace duckdb;

// With external_commit_sync, a COMMIT that needs a WAL sync publishes, parks its task, and finishes once a sync
// that it does not run itself covers it. Until then no new snapshot sees it
namespace {

idx_t ScalarValue(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	return result->Collection().GetValue(0, 0).GetValue<idx_t>();
}

struct ExternalSyncDatabase {
	explicit ExternalSyncDatabase(const string &name) : path(TestCreatePath(name)) {
		DeleteDatabase(path);
		db = make_uniq<DuckDB>(path);
		setup = make_uniq<Connection>(*db);
		// only the calling thread runs a submitted query's tasks
		REQUIRE_NO_FAIL(setup->Query("SET threads=1"));
		REQUIRE_NO_FAIL(setup->Query("SET checkpoint_threshold='1TB'"));
		REQUIRE_NO_FAIL(setup->Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(setup->Query("CREATE TABLE t(i INTEGER)"));
	}

	//! A connection whose COMMIT awaits an external sync, in a transaction that inserted rows
	unique_ptr<Connection> Committer(const string &insert = "INSERT INTO t VALUES (1000)") {
		auto con = make_uniq<Connection>(*db);
		REQUIRE_NO_FAIL(con->Query("SET external_commit_sync=true"));
		REQUIRE_NO_FAIL(con->Query("BEGIN"));
		REQUIRE_NO_FAIL(con->Query(insert));
		return con;
	}

	DuckTransactionManager &TransactionManager() {
		auto &db_manager = DatabaseManager::Get(*setup->context);
		auto attached = db_manager.GetDatabase(db_manager.GetDefaultDatabase());
		REQUIRE(attached);
		return DuckTransactionManager::Get(*attached);
	}

	string path;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> setup;
};

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

TEST_CASE("An external-sync COMMIT parks until an external sync covers it", "[api][group_commit]") {
	ExternalSyncDatabase db("external_commit_sync_park.db");
	auto committer = db.Committer();
	auto commit = SubmitParkedCommit(*committer);
	REQUIRE(commit->ExecuteTask() == QueryResultState::BLOCKED);
	// published, but not durable: new snapshots stop below it
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == 0);

	REQUIRE(db.TransactionManager().SyncCommits());
	REQUIRE(!db.TransactionManager().SyncCommits());
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == 1);
	commit->Complete();
	REQUIRE_NO_FAIL(*commit);
	REQUIRE(ScalarValue(*committer, "SELECT count(*) FROM t") == 1);
}

TEST_CASE("A synchronous external-sync COMMIT waits for an external sync", "[api][group_commit]") {
	ExternalSyncDatabase db("external_commit_sync_blocking.db");
	auto committer = db.Committer();
	std::atomic<bool> committed {false};
	bool commit_failed = false;
	std::thread commit_thread([&]() {
		commit_failed = committer->Query("COMMIT")->HasError();
		committed = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	REQUIRE(!committed);
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
	while (!committed && std::chrono::steady_clock::now() < deadline) {
		db.TransactionManager().SyncCommits();
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	commit_thread.join();
	REQUIRE(!commit_failed);
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == 1);
}

TEST_CASE("A checkpoint syncs for a parked COMMIT that nobody resumes", "[api][group_commit]") {
	ExternalSyncDatabase db("external_commit_sync_checkpoint.db");
	auto committer = db.Committer();
	auto commit = SubmitParkedCommit(*committer);
	// the checkpoint drains the WAL before replacing it: it syncs for the parked commit instead of waiting for it
	REQUIRE_NO_FAIL(db.setup->Query("CHECKPOINT"));
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == 1);
	commit->Complete();
	REQUIRE_NO_FAIL(*commit);
}

TEST_CASE("A failed external sync fails the parked COMMIT", "[api][group_commit]") {
	ExternalSyncDatabase db("external_commit_sync_failure.db");
	auto committer = db.Committer();
	auto commit = SubmitParkedCommit(*committer);
	REQUIRE_NO_FAIL(db.setup->Query("SET debug_force_wal_fsync_failure=true"));
	REQUIRE(db.TransactionManager().SyncCommits());
	commit->Complete();
	REQUIRE(commit->HasError());
	// the database is invalidated
	REQUIRE_FAIL(db.setup->Query("SELECT count(*) FROM t"));
}

TEST_CASE("A parked COMMIT blocks new transactions on its connection and completes when abandoned",
          "[api][group_commit]") {
	ExternalSyncDatabase db("external_commit_sync_abandoned.db");
	auto committer = db.Committer();
	auto commit = SubmitParkedCommit(*committer);
	// runs in a transaction without ending the open query: refused, and the COMMIT is unaffected
	REQUIRE_THROWS(committer->TableInfo("t"));
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == 0);
	// the client drops the result: the COMMIT is irreversible, so ending the query syncs for it
	commit.reset();
	REQUIRE(ScalarValue(*committer, "SELECT count(*) FROM t") == 1);
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == 1);
	REQUIRE(committer->TableInfo("t"));
}

TEST_CASE("A parked COMMIT resumes the commit of the remaining databases", "[api][group_commit]") {
	ExternalSyncDatabase db("external_commit_sync_attached.db");
	auto other_path = TestCreatePath("external_commit_sync_attached_other.db");
	DeleteDatabase(other_path);
	REQUIRE_NO_FAIL(db.setup->Query("ATTACH '" + other_path + "' AS other"));
	REQUIRE_NO_FAIL(db.setup->Query("CREATE TABLE other.o AS SELECT 1 AS i"));

	Connection committer(*db.db);
	REQUIRE_NO_FAIL(committer.Query("SET external_commit_sync=true"));
	REQUIRE_NO_FAIL(committer.Query("BEGIN"));
	// databases commit in reverse order of use: the write to t commits and parks before temp and other commit
	REQUIRE_NO_FAIL(committer.Query("CREATE TEMP TABLE tt AS SELECT 7 AS i"));
	REQUIRE(ScalarValue(committer, "SELECT count(*) FROM other.o") == 1);
	REQUIRE_NO_FAIL(committer.Query("INSERT INTO t VALUES (1000)"));
	auto commit = SubmitParkedCommit(committer);

	REQUIRE(db.TransactionManager().SyncCommits());
	commit->Complete();
	REQUIRE_NO_FAIL(*commit);
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == 1);
	// the temp database committed after the resume
	REQUIRE(ScalarValue(committer, "SELECT i FROM tt") == 7);
	REQUIRE(ScalarValue(committer, "SELECT count(*) FROM other.o") == 1);
}

TEST_CASE("External-sync and self-sync commits alongside an external syncer and checkpoints", "[api][group_commit]") {
	constexpr idx_t COMMITTERS = 4;
	constexpr idx_t COMMITS = 100;
	ExternalSyncDatabase db("external_commit_sync_concurrent.db");
	REQUIRE_NO_FAIL(db.setup->Query("SET threads=4"));
	std::atomic<bool> stop {false};
	std::atomic<idx_t> failures {0};
	std::thread syncer([&]() {
		while (!stop) {
			db.TransactionManager().SyncCommits();
			std::this_thread::sleep_for(std::chrono::microseconds(200));
		}
	});
	std::thread checkpointer([&]() {
		Connection con(*db.db);
		while (!stop) {
			if (con.Query("CHECKPOINT")->HasError()) {
				failures++;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	});
	vector<std::thread> committers;
	for (idx_t c = 0; c < COMMITTERS; c++) {
		committers.emplace_back([&, c]() {
			Connection con(*db.db);
			// every other committer syncs its own commits
			if (con.Query("SET external_commit_sync=" + string(c % 2 == 0 ? "true" : "false"))->HasError()) {
				failures++;
			}
			for (idx_t i = 0; i < COMMITS; i++) {
				auto value = to_string(c * COMMITS + i);
				bool failed = con.Query("BEGIN")->HasError();
				failed = failed || con.Query("INSERT INTO t VALUES (" + value + ")")->HasError();
				failed = failed || con.Query("COMMIT")->HasError();
				// read-your-writes: the next transaction sees the commit
				auto count = con.Query("SELECT count(*) FROM t WHERE i = " + value);
				if (failed || count->HasError() || count->Collection().GetValue(0, 0).GetValue<idx_t>() != 1) {
					failures++;
				}
			}
		});
	}
	for (auto &committer : committers) {
		committer.join();
	}
	stop = true;
	syncer.join();
	checkpointer.join();
	REQUIRE(failures == 0);
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == COMMITTERS * COMMITS);
}

TEST_CASE("COMMITs abandoned while worker threads run them and an external syncer wakes them", "[api][group_commit]") {
	constexpr idx_t COMMITTERS = 4;
	constexpr idx_t COMMITS = 50;
	ExternalSyncDatabase db("external_commit_sync_abandon_workers.db");
	REQUIRE_NO_FAIL(db.setup->Query("SET threads=4"));
	std::atomic<bool> stop {false};
	mutex error_lock;
	vector<string> errors;
	std::thread syncer([&]() {
		while (!stop) {
			db.TransactionManager().SyncCommits();
		}
	});
	vector<std::thread> committers;
	for (idx_t c = 0; c < COMMITTERS; c++) {
		committers.emplace_back([&, c]() {
			Connection con(*db.db);
			auto run = [&](const string &query) {
				auto result = con.Query(query);
				if (result->HasError()) {
					lock_guard<mutex> guard(error_lock);
					errors.push_back(query + ": " + result->GetError());
					return false;
				}
				return true;
			};
			run("SET external_commit_sync=true");
			for (idx_t i = 0; i < COMMITS; i++) {
				auto value = to_string(c * COMMITS + i);
				if (!run("BEGIN") || !run("INSERT INTO t VALUES (" + value + ")")) {
					return;
				}
				// dropped at once or after a few tasks, while workers may run it and the syncer may wake it
				auto commit = con.Submit("COMMIT");
				for (idx_t task = 0; task < i % 3; task++) {
					auto state = commit->ExecuteTask();
					if (state == QueryResultState::FINISHED || state == QueryResultState::EXECUTION_ERROR) {
						break;
					}
				}
				commit.reset();
				if (!con.context->transaction.IsAutoCommit() && !run("COMMIT")) {
					// dropped before any task ran it: aborted, so the transaction is still open
					return;
				}
				auto count = con.Query("SELECT count(*) FROM t WHERE i = " + value);
				if (count->HasError() || count->Collection().GetValue(0, 0).GetValue<idx_t>() != 1) {
					lock_guard<mutex> guard(error_lock);
					errors.push_back("commit " + value + " not visible to its connection");
				}
			}
		});
	}
	for (auto &committer : committers) {
		committer.join();
	}
	stop = true;
	syncer.join();
	INFO(StringUtil::Join(errors, "\n"));
	REQUIRE(errors.empty());
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == COMMITTERS * COMMITS);
}

TEST_CASE("A parked UPDATE commit holds off CHECKPOINT until it resumes", "[api][group_commit]") {
	ExternalSyncDatabase db("external_commit_sync_update.db");
	REQUIRE_NO_FAIL(db.setup->Query("INSERT INTO t SELECT * FROM range(1000)"));
	auto committer = db.Committer("UPDATE t SET i = i + 1");
	auto commit = SubmitParkedCommit(*committer);
	// the parked transaction keeps the shared checkpoint lock it took for the update
	REQUIRE_FAIL(db.setup->Query("CHECKPOINT"));
	REQUIRE(ScalarValue(*db.setup, "SELECT sum(i) FROM t") == 499500);

	// once synced, the resumed commit upgrades its lock for the automatic checkpoint it gave up
	REQUIRE_NO_FAIL(db.setup->Query("SET checkpoint_threshold='1KB'"));
	REQUIRE(db.TransactionManager().SyncCommits());
	commit->Complete();
	REQUIRE_NO_FAIL(*commit);
	auto &fs = FileSystem::GetFileSystem(*db.db->instance);
	REQUIRE(!fs.FileExists(db.path + ".wal"));
	REQUIRE(ScalarValue(*db.setup, "SELECT sum(i) FROM t") == 500500);
	REQUIRE_NO_FAIL(db.setup->Query("CHECKPOINT"));
}

TEST_CASE("A parked COMMIT runs its automatic checkpoint once synced", "[api][group_commit]") {
	ExternalSyncDatabase db("external_commit_sync_autocheckpoint.db");
	auto committer = db.Committer("INSERT INTO t SELECT * FROM range(10000)");
	REQUIRE_NO_FAIL(db.setup->Query("SET checkpoint_threshold='1KB'"));
	auto commit = SubmitParkedCommit(*committer);

	auto &fs = FileSystem::GetFileSystem(*db.db->instance);
	REQUIRE(fs.FileExists(db.path + ".wal"));
	REQUIRE(db.TransactionManager().SyncCommits());
	commit->Complete();
	REQUIRE_NO_FAIL(*commit);
	// the resumed commit checkpointed, which removes the WAL
	REQUIRE(!fs.FileExists(db.path + ".wal"));
	REQUIRE(ScalarValue(*db.setup, "SELECT count(*) FROM t") == 10000);
}
