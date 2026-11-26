#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

#include <chrono>
#include <condition_variable>
#include <future>
#include <thread>

using namespace duckdb;

// Small data payloads fit in one partial block, whose first write occurs after preparation but before publication.
// A filesystem gate pauses that write to test concurrent inserts/deletes, rollback, and old-reader visibility.
// The full-checkpoint case checks that vacuum exclusion protects remapped indexes until table publication.
// Checkpoint failure cases inject write errors; maintenance cases also gate reads to test error/cancellation cleanup.
// The gate controls the publication window without relying on sleep-based scheduling.
namespace {

class GatedCheckpointFileSystem : public LocalFileSystem {
public:
	explicit GatedCheckpointFileSystem(string database_name_p) : database_name(std::move(database_name_p)) {
	}

	using LocalFileSystem::Read;
	using LocalFileSystem::Write;

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		if (WaitForRelease(handle, false)) {
			throw IOException("Injected deferred checkpoint write failure");
		}
		LocalFileSystem::Write(handle, buffer, nr_bytes, location);
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		if (WaitForRelease(handle, true)) {
			throw IOException("Injected SQL maintenance read failure");
		}
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	void Arm(bool fail, bool read = false) {
		lock_guard<mutex> guard(lock);
		armed = true;
		parked = false;
		released = false;
		fail_checkpoint = fail;
		gate_reads = read;
	}

	bool WaitUntilParked() {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(60), [&]() { return parked; });
	}

	void Release() {
		{
			lock_guard<mutex> guard(lock);
			released = true;
			armed = false;
		}
		cv.notify_all();
	}

private:
	bool WaitForRelease(FileHandle &handle, bool reading) {
		if (!StringUtil::EndsWith(handle.GetPath(), database_name)) {
			return false;
		}
		unique_lock<mutex> guard(lock);
		if (!armed || gate_reads != reading) {
			return false;
		}
		armed = false;
		parked = true;
		cv.notify_all();
		if (!cv.wait_for(guard, std::chrono::seconds(60), [&]() { return released; })) {
			throw IOException("Timed out waiting to release gated file I/O");
		}
		return fail_checkpoint;
	}

private:
	string database_name;
	mutex lock;
	std::condition_variable cv;
	bool armed = false;
	bool parked = false;
	bool released = false;
	bool fail_checkpoint = false;
	bool gate_reads = false;
};

struct GatedCheckpointDatabase {
	explicit GatedCheckpointDatabase(const string &name, bool small_row_groups_p = false,
	                                 const string &storage_version_p = "latest")
	    : database_name(name), path(TestCreatePath(name)), small_row_groups(small_row_groups_p),
	      storage_version(storage_version_p) {
		DeleteDatabase(path);
		auto fs = make_uniq<GatedCheckpointFileSystem>(name);
		gate = *fs;
		DBConfig config;
		config.file_system = make_uniq<VirtualFileSystem>(std::move(fs));
		if (small_row_groups) {
			db = make_uniq<DuckDB>(nullptr, &config);
			Connection con(*db);
			REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS data (ROW_GROUP_SIZE 2048, STORAGE_VERSION '" +
			                          storage_version + "')"));
		} else {
			db = make_uniq<DuckDB>(path, &config);
		}
	}

	~GatedCheckpointDatabase() {
		FinishCheckpoint();
	}

	void StartCheckpoint(bool fail, const string &query = "CHECKPOINT") {
		gate->Arm(fail);
		checkpoint_thread = std::thread([this, query]() {
			Connection con(*db);
			checkpoint_result = con.Query(query);
		});
		REQUIRE(gate->WaitUntilParked());
	}

	void FinishCheckpoint() {
		if (gate) {
			gate->Release();
		}
		if (checkpoint_thread.joinable()) {
			checkpoint_thread.join();
		}
	}

	void Reopen(bool gate_io = false) {
		FinishCheckpoint();
		checkpoint_result.reset();
		gate = nullptr;
		db.reset();
		DBConfig config;
		if (gate_io) {
			auto fs = make_uniq<GatedCheckpointFileSystem>(database_name);
			gate = *fs;
			config.file_system = make_uniq<VirtualFileSystem>(std::move(fs));
		}
		if (small_row_groups) {
			db = make_uniq<DuckDB>(nullptr, &config);
			Connection con(*db);
			REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS data (ROW_GROUP_SIZE 2048, STORAGE_VERSION '" +
			                          storage_version + "')"));
		} else {
			db = make_uniq<DuckDB>(path, &config);
		}
	}

	string database_name;
	string path;
	bool small_row_groups;
	string storage_version;
	unique_ptr<DuckDB> db;
	optional_ptr<GatedCheckpointFileSystem> gate;
	std::thread checkpoint_thread;
	unique_ptr<QueryResult> checkpoint_result;
};

struct CheckpointReleaseGuard {
	GatedCheckpointDatabase &database;

	~CheckpointReleaseGuard() {
		database.FinishCheckpoint();
	}
};

void RequireScalar(Connection &con, const string &sql, int64_t expected) {
	INFO(sql);
	auto result = con.Query(sql);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == expected);
}

} // namespace

TEST_CASE("Merges are visible once sealed and a failed checkpoint takes them back", "[api][storage][checkpoint]") {
	const bool fail_checkpoint = GENERATE(false, true);
	CAPTURE(fail_checkpoint);
	GatedCheckpointDatabase database("vacuum_seal_publication.db", true);
	{
		Connection con(*database.db);
		Connection deleter(*database.db);
		REQUIRE_NO_FAIL(con.Query("USE data"));
		REQUIRE_NO_FAIL(deleter.Query("USE data"));
		REQUIRE_NO_FAIL(con.Query("SET threads=1"));
		REQUIRE_NO_FAIL(con.Query("SET checkpoint_threshold='1TB'"));
		REQUIRE_NO_FAIL(con.Query("SET force_compression='uncompressed'"));
		REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(k INTEGER PRIMARY KEY, v INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO a SELECT range, range*10 FROM range(2048)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT data"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO a SELECT range, range*10 FROM range(2048,3048)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT data"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM a WHERE k < 1000"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE b(k INTEGER REFERENCES a(k), v INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO b SELECT range, range*10 FROM range(1000,1032)"));

		CheckpointReleaseGuard release_guard {database};
		// parked at the first write to the database file: the row groups are merged, nothing is flushed or published
		database.StartCheckpoint(fail_checkpoint, "CHECKPOINT data");

		// table scans and index scans both see the merged row group
		RequireScalar(con, "SELECT sum(rowid) FILTER (WHERE k=2048) FROM a", 1048);
		RequireScalar(con, "SELECT rowid FROM a WHERE k=2048", 1048);
		RequireScalar(con, "SELECT count(*) FROM a", 2048);
		RequireScalar(con, "SELECT sum(k) FROM a", 4144128);
		RequireScalar(con, "SELECT sum(v) FROM a", 41441280);
		RequireScalar(con, "SELECT count(DISTINCT rowid) FROM a", 2048);
		RequireScalar(con, "SELECT max(rowid) FROM a", 2047);
		RequireScalar(con, "SELECT v FROM a WHERE k=3047", 30470);
		RequireScalar(con, "SELECT sum(v) FROM b", 324960);
		REQUIRE_NO_FAIL(con.Query("BEGIN"));
		{
			// the checkpoint does not hold the vacuum lock between merges
			auto &attached = Catalog::GetCatalog(*con.context, "data").GetAttached();
			auto &transaction_manager = DuckTransactionManager::Get(attached);
			auto vacuum_lock = transaction_manager.TryGetVacuumLock();
			REQUIRE(vacuum_lock.get() != nullptr);
		}
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

		// commits that do not write to the merged row group are acknowledged while the checkpoint is parked
		REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (5000, 50000)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO b VALUES (2048, 1)"));
		REQUIRE_FAIL(con.Query("INSERT INTO a VALUES (2048, 0)"));
		REQUIRE_FAIL(con.Query("INSERT INTO b VALUES (999, 1)"));
		RequireScalar(con, "SELECT rowid FROM a WHERE k=5000", 3048);

		// a rolled back delete in the merged row group does not wait
		REQUIRE_NO_FAIL(con.Query("BEGIN"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM a WHERE k=3001"));
		RequireScalar(con, "SELECT count(*) FROM a WHERE k=3001", 0);
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
		RequireScalar(con, "SELECT rowid FROM a WHERE k=3001", 2001);

		// a delete in the merged row group commits when the checkpoint is durable, not before
		auto pending_delete =
		    std::async(std::launch::async, [&]() { return deleter.Query("DELETE FROM a WHERE k=3000"); });
		REQUIRE(pending_delete.wait_for(std::chrono::milliseconds(1000)) == std::future_status::timeout);
		// nobody else sees it yet
		RequireScalar(con, "SELECT count(*) FROM a WHERE k=3000", 1);

		database.FinishCheckpoint();
		auto delete_result = pending_delete.get();
		REQUIRE(database.checkpoint_result);
		if (fail_checkpoint) {
			REQUIRE(database.checkpoint_result->HasError());
			REQUIRE(StringUtil::Contains(database.checkpoint_result->GetError(),
			                             "Injected deferred checkpoint write failure"));
			REQUIRE(delete_result->HasError());
			INFO(delete_result->GetError());
			REQUIRE(StringUtil::Contains(delete_result->GetError(), "Cannot commit"));
		} else {
			REQUIRE_NO_FAIL(*database.checkpoint_result);
			REQUIRE_NO_FAIL(*delete_result);
			RequireScalar(con, "SELECT rowid FROM a WHERE k=2048", 1048);
			RequireScalar(con, "SELECT count(*) FROM a WHERE k=3000", 0);
			RequireScalar(con, "SELECT count(*) FROM a", 2048);
			RequireScalar(con, "SELECT count(DISTINCT row_group_id) FROM pragma_storage_info('a')", 2);
			RequireScalar(con, "SELECT sum(v) FROM b", 324961);
		}
	}

	// without a shutdown checkpoint: the commits made while the checkpoint was parked are replayed from the WAL
	database.Reopen();
	{
		Connection con(*database.db);
		REQUIRE_NO_FAIL(con.Query("USE data"));
		// a failed checkpoint leaves the row groups as they were, and the delete that waited for it did not commit
		RequireScalar(con, "SELECT count(*) FROM a", fail_checkpoint ? 2049 : 2048);
		RequireScalar(con, "SELECT sum(k) FROM a", fail_checkpoint ? 4149128 : 4146128);
		RequireScalar(con, "SELECT count(DISTINCT rowid) FROM a", fail_checkpoint ? 2049 : 2048);
		RequireScalar(con, "SELECT rowid FROM a WHERE k=2048", fail_checkpoint ? 2048 : 1048);
		RequireScalar(con, "SELECT sum(rowid) FILTER (WHERE k=2048) FROM a", fail_checkpoint ? 2048 : 1048);
		RequireScalar(con, "SELECT count(*) FROM a WHERE k=3000", fail_checkpoint ? 1 : 0);
		RequireScalar(con, "SELECT rowid FROM a WHERE k=3001", fail_checkpoint ? 3001 : 2001);
		RequireScalar(con, "SELECT rowid FROM a WHERE k=5000", 3048);
		RequireScalar(con, "SELECT v FROM a WHERE k=5000", 50000);
		RequireScalar(con, "SELECT sum(v) FROM b", 324961);
		REQUIRE_FAIL(con.Query("INSERT INTO a VALUES (2048, 0)"));
		REQUIRE_FAIL(con.Query("INSERT INTO a VALUES (5000, 0)"));
		REQUIRE_FAIL(con.Query("DELETE FROM a WHERE k=2048"));
		REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	database.Reopen();
	Connection con(*database.db);
	REQUIRE_NO_FAIL(con.Query("USE data"));
	RequireScalar(con, "SELECT rowid FROM a WHERE k=2048", 1048);
	RequireScalar(con, "SELECT count(*) FROM a", fail_checkpoint ? 2049 : 2048);
	RequireScalar(con, "SELECT count(DISTINCT rowid) FROM a", fail_checkpoint ? 2049 : 2048);
	RequireScalar(con, "SELECT sum(k) FROM a", fail_checkpoint ? 4149128 : 4146128);
	RequireScalar(con, "SELECT v FROM a WHERE k=5000", 50000);
	RequireScalar(con, "SELECT sum(v) FROM b", 324961);
}

TEST_CASE("Old storage: full checkpoint defers remapped collections under vacuum exclusion",
          "[api][storage][checkpoint]") {
	const bool fail_checkpoint = GENERATE(false, true);
	CAPTURE(fail_checkpoint);
	GatedCheckpointDatabase database("vacuum_seal_publication_old.db", true, "v2.0.0");
	{
		Connection con(*database.db);
		REQUIRE_NO_FAIL(con.Query("USE data"));
		REQUIRE_NO_FAIL(con.Query("SET threads=1"));
		REQUIRE_NO_FAIL(con.Query("SET checkpoint_threshold='1TB'"));
		REQUIRE_NO_FAIL(con.Query("SET force_compression='uncompressed'"));
		REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(k INTEGER PRIMARY KEY, v INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO a SELECT range, range*10 FROM range(2048)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT data"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO a SELECT range, range*10 FROM range(2048,3048)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT data"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM a WHERE k < 1000"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE b(k INTEGER REFERENCES a(k), v INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO b SELECT range, range*10 FROM range(1000,1032)"));

		CheckpointReleaseGuard release_guard {database};
		database.StartCheckpoint(fail_checkpoint, "CHECKPOINT data");

		// A table scan still sees the old layout. The filter inside SUM does not request an index probe.
		REQUIRE_NO_FAIL(con.Query("BEGIN"));
		RequireScalar(con, "SELECT sum(rowid) FILTER (WHERE k=2048) FROM a", 2048);
		auto &attached = Catalog::GetCatalog(*con.context, "data").GetAttached();
		auto &transaction_manager = DuckTransactionManager::Get(attached);
		REQUIRE(!transaction_manager.TryGetVacuumLock());
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

		database.FinishCheckpoint();
		REQUIRE(database.checkpoint_result);
		if (fail_checkpoint) {
			REQUIRE(database.checkpoint_result->HasError());
			REQUIRE(StringUtil::Contains(database.checkpoint_result->GetError(),
			                             "Injected deferred checkpoint write failure"));
		} else {
			REQUIRE_NO_FAIL(*database.checkpoint_result);
			RequireScalar(con, "SELECT rowid FROM a WHERE k=2048", 1048);
			RequireScalar(con, "SELECT count(DISTINCT row_group_id) FROM pragma_storage_info('a')", 1);
			RequireScalar(con, "SELECT sum(v) FROM b", 324960);
		}
	}

	database.Reopen();
	{
		Connection con(*database.db);
		REQUIRE_NO_FAIL(con.Query("USE data"));
		RequireScalar(con, "SELECT count(*) FROM a", 2048);
		RequireScalar(con, "SELECT sum(k) FROM a", 4144128);
		RequireScalar(con, "SELECT rowid FROM a WHERE k=2048", fail_checkpoint ? 2048 : 1048);
		RequireScalar(con, "SELECT sum(v) FROM b", 324960);
		REQUIRE_FAIL(con.Query("INSERT INTO a VALUES (2048, 0)"));
		REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	database.Reopen();
	Connection con(*database.db);
	REQUIRE_NO_FAIL(con.Query("USE data"));
	RequireScalar(con, "SELECT rowid FROM a WHERE k=2048", 1048);
	RequireScalar(con, "SELECT count(*) FROM a", 2048);
	RequireScalar(con, "SELECT sum(v) FROM b", 324960);
}
