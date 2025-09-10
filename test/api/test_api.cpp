#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test comment in CPP API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.SendQuery("--ups");
	//! Should not crash
	REQUIRE(1);
}

TEST_CASE("Test StarExpression replace_list parameter", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto sql = "select * replace(i * $n as i) from range(1, 10) t(i)";
	auto stmts = con.ExtractStatements(sql);

	auto &select_stmt = stmts[0]->Cast<SelectStatement>();
	auto &select_node = select_stmt.node->Cast<SelectNode>();

	REQUIRE(select_node.select_list[0]->HasParameter());
}

TEST_CASE("Test using connection after database is gone", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);
	// check that the connection works
	auto result = conn->Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	// destroy the database
	db.reset();
	// try to use the connection
	// it still works: the database remains until all connections are destroyed
	REQUIRE_NO_FAIL(conn->Query("SELECT 42"));

	// now try it with an open transaction
	db = make_uniq<DuckDB>(nullptr);
	conn = make_uniq<Connection>(*db);

	REQUIRE_NO_FAIL(conn->Query("BEGIN TRANSACTION"));
	result = conn->Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	db.reset();

	REQUIRE_NO_FAIL(conn->Query("SELECT 42"));
}

TEST_CASE("Test destroying connections with open transactions", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	{
		Connection con(*db);
		con.Query("BEGIN TRANSACTION");
		con.Query("CREATE TABLE test(i INTEGER);");
	}

	auto conn = make_uniq<Connection>(*db);
	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE test(i INTEGER)"));
}

static void long_running_query(Connection *conn, bool *correct) {
	*correct = true;
	auto result = conn->Query("SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, "
	                          "integers i6, integers i7, integers i8, integers i9, integers i10,"
	                          "integers i11, integers i12, integers i13");
	// the query should fail
	*correct = result->HasError();
}

TEST_CASE("Test closing database during long running query", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);
	// create the database
	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers FROM range(10000)"));
	conn->DisableProfiling();
	// perform a long running query in the background (many cross products)
	bool correct = true;
	auto background_thread = thread(long_running_query, conn.get(), &correct);
	// wait a little bit
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	// destroy the database
	conn->Interrupt();
	db.reset();
	// wait for the thread
	background_thread.join();
	REQUIRE(correct);
	// try to use the connection
	REQUIRE_NO_FAIL(conn->Query("SELECT 42"));
}

TEST_CASE("Test closing result after database is gone", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);
	// check that the connection works
	auto result = conn->Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	// destroy the database
	db.reset();
	conn.reset();
	result.reset();

	// now the streaming result
	db = make_uniq<DuckDB>(nullptr);
	conn = make_uniq<Connection>(*db);
	// check that the connection works
	auto streaming_result = conn->SendQuery("SELECT 42");
	// destroy the database
	db.reset();
	conn.reset();
	REQUIRE(CHECK_COLUMN(streaming_result, 0, {42}));
	streaming_result.reset();
}

TEST_CASE("Test closing database with open prepared statements", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);

	auto p1 = conn->Prepare("CREATE TABLE a (i INTEGER)");
	REQUIRE_NO_FAIL(p1->Execute());
	auto p2 = conn->Prepare("INSERT INTO a VALUES (42)");
	REQUIRE_NO_FAIL(p2->Execute());

	db.reset();
	conn.reset();

	// the prepared statements are still valid
	// the database is only destroyed when the prepared statements are destroyed
	REQUIRE_NO_FAIL(p2->Execute());
	p1.reset();
	p2.reset();
}

static void parallel_query(Connection *conn, bool *correct, size_t threadnr) {
	correct[threadnr] = true;
	for (size_t i = 0; i < 100; i++) {
		auto result = conn->Query("SELECT * FROM integers ORDER BY i");
		if (!CHECK_COLUMN(result, 0, {1, 2, 3, Value()})) {
			correct[threadnr] = false;
		}
	}
}

TEST_CASE("Test temp_directory defaults", "[api][.]") {
	const char *db_paths[] = {nullptr, "", ":memory:"};
	for (auto &path : db_paths) {
		auto db = make_uniq<DuckDB>(path);
		auto conn = make_uniq<Connection>(*db);

		REQUIRE(db->instance->config.options.temporary_directory == ".tmp");
	}
}

TEST_CASE("Test parallel usage of single client", "[api][.]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);

	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	bool correct[20];
	thread threads[20];
	for (size_t i = 0; i < 20; i++) {
		threads[i] = thread(parallel_query, conn.get(), correct, i);
	}
	for (size_t i = 0; i < 20; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}
}

static void parallel_query_with_new_connection(DuckDB *db, bool *correct, size_t threadnr) {
	correct[threadnr] = true;
	for (size_t i = 0; i < 100; i++) {
		auto conn = make_uniq<Connection>(*db);
		auto result = conn->Query("SELECT * FROM integers ORDER BY i");
		if (!CHECK_COLUMN(result, 0, {1, 2, 3, Value()})) {
			correct[threadnr] = false;
		}
	}
}

TEST_CASE("Test making and dropping connections in parallel to a single database", "[api][.]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);

	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	bool correct[20];
	thread threads[20];
	for (size_t i = 0; i < 20; i++) {
		threads[i] = thread(parallel_query_with_new_connection, db.get(), correct, i);
	}
	for (size_t i = 0; i < 100; i++) {
		auto result = conn->Query("SELECT * FROM integers ORDER BY i");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, Value()}));
	}
	for (size_t i = 0; i < 20; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}
	auto result = conn->Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, Value()}));
}

TEST_CASE("Test multiple result sets", "[api]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.DisableQueryVerification();
	con.EnableQueryVerification();

	con.ForceParallelism();

	result = con.Query("SELECT 42; SELECT 84");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	result = std::move(result->next);
	REQUIRE(CHECK_COLUMN(result, 0, {84}));
	REQUIRE(!result->next);

	// also with stream api
	result = con.SendQuery("SELECT 42; SELECT 84");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	result = std::move(result->next);
	REQUIRE(CHECK_COLUMN(result, 0, {84}));
	REQUIRE(!result->next);
}

TEST_CASE("Test streaming API errors", "[api]") {
	duckdb::unique_ptr<QueryResult> result, result2;
	DuckDB db(nullptr);
	Connection con(db);

	// multiple streaming result
	result = con.SendQuery("SELECT 42;");
	result2 = con.SendQuery("SELECT 42;");
	// "result" is invalidated
	REQUIRE_THROWS(CHECK_COLUMN(result, 0, {42}));
	// "result2" we can read
	REQUIRE(CHECK_COLUMN(result2, 0, {42}));

	// streaming result followed by non-streaming result
	result = con.SendQuery("SELECT 42;");
	result2 = con.Query("SELECT 42;");
	// "result" is invalidated
	REQUIRE_THROWS(CHECK_COLUMN(result, 0, {42}));
	// "result2" we can read
	REQUIRE(CHECK_COLUMN(result2, 0, {42}));

	// error in binding
	result = con.SendQuery("SELECT * FROM nonexistanttable");
	REQUIRE(!result->ToString().empty());
	REQUIRE(result->type == QueryResultType::MATERIALIZED_RESULT);
	REQUIRE_FAIL(result);

	// error in stream that only happens after fetching
	result = con.SendQuery(
	    "SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);");
	while (!result->HasError()) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
	}
	REQUIRE(!result->ToString().empty());
	REQUIRE_FAIL(result);

	// same query but call Materialize
	result = con.SendQuery(
	    "SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);");
	REQUIRE(!result->ToString().empty());
	REQUIRE(result->type == QueryResultType::STREAM_RESULT);
	result = ((StreamQueryResult &)*result).Materialize();
	REQUIRE_FAIL(result);

	// same query but call materialize after fetching
	result = con.SendQuery(
	    "SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);");
	while (!result->HasError()) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
	}
	REQUIRE(!result->ToString().empty());
	REQUIRE(result->type == QueryResultType::STREAM_RESULT);
	result = ((StreamQueryResult &)*result).Materialize();
	REQUIRE_FAIL(result);
}

TEST_CASE("Test fetch API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	duckdb::unique_ptr<QueryResult> result;

	// fetch from an error
	result = con.Query("SELECT 'hello'::INT");
	REQUIRE_THROWS(result->Fetch());

	result = con.SendQuery("CREATE TABLE test (a INTEGER);");

	result = con.Query("select a from test where 1 <> 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	result = con.SendQuery("INSERT INTO test VALUES (42)");
	result = con.SendQuery("SELECT a from test");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	auto materialized_result = con.Query("select a from test");
	REQUIRE(CHECK_COLUMN(materialized_result, 0, {42}));

	// override fetch result
	result = con.SendQuery("SELECT a from test");
	result = con.SendQuery("SELECT a from test");
	result = con.SendQuery("SELECT a from test");
	result = con.SendQuery("SELECT a from test");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Test fetch API not to completion", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);
	// remove connection with active stream result
	auto result = conn->SendQuery("SELECT 42");
	// close the connection
	conn.reset();
	// now try to fetch a chunk, this should not return a nullptr
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	// Only if we would call Fetch again would we Close the QueryResult
	// this is testing that it can get cleaned up without this.

	db.reset();
}

TEST_CASE("Test fetch API robustness", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);

	// remove connection with active stream result
	auto result = conn->SendQuery("SELECT 42");
	// close the connection
	conn.reset();
	// now try to fetch a chunk, this should not return a nullptr
	auto chunk = result->Fetch();
	REQUIRE(chunk);

	// now close the entire database
	conn = make_uniq<Connection>(*db);
	result = conn->SendQuery("SELECT 42");

	db.reset();
	// fetch should not fail
	chunk = result->Fetch();
	REQUIRE(chunk);
	// new queries on the connection should not fail either
	REQUIRE_NO_FAIL(conn->SendQuery("SELECT 42"));

	// override fetch result
	db = make_uniq<DuckDB>(nullptr);
	conn = make_uniq<Connection>(*db);
	auto result1 = conn->SendQuery("SELECT 42");
	auto result2 = conn->SendQuery("SELECT 84");
	REQUIRE_NO_FAIL(*result1);
	REQUIRE_NO_FAIL(*result2);

	// result1 should be closed now
	REQUIRE_THROWS(result1->Fetch());
	// result2 should work
	REQUIRE(result2->Fetch());

	// test materialize
	result1 = conn->SendQuery("SELECT 42");
	REQUIRE(result1->type == QueryResultType::STREAM_RESULT);
	auto materialized = ((StreamQueryResult &)*result1).Materialize();
	result2 = conn->SendQuery("SELECT 84");

	// we can read materialized still, even after opening a new result
	REQUIRE(CHECK_COLUMN(materialized, 0, {42}));
	REQUIRE(CHECK_COLUMN(result2, 0, {84}));
}

static void VerifyStreamResult(duckdb::unique_ptr<QueryResult> result) {
	REQUIRE(result->types[0] == LogicalType::INTEGER);
	size_t current_row = 0;
	int current_expected_value = 0;
	size_t expected_rows = 500 * 5;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		auto col1_data = FlatVector::GetData<int>(chunk->data[0]);
		for (size_t k = 0; k < chunk->size(); k++) {
			if (current_row % 500 == 0) {
				current_expected_value++;
			}
			REQUIRE(col1_data[k] == current_expected_value);
			current_row++;
		}
	}
	REQUIRE(current_row == expected_rows);
}

TEST_CASE("Test fetch API with big results", "[api][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create table that consists of multiple chunks
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER)"));
	for (size_t i = 0; i < 500; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1); INSERT INTO test VALUES (2); INSERT INTO test VALUES "
		                          "(3); INSERT INTO test VALUES (4); INSERT INTO test VALUES (5);"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// stream the results using the Fetch() API
	auto result = con.SendQuery("SELECT CAST(a AS INTEGER) FROM test ORDER BY a");
	VerifyStreamResult(std::move(result));
	// we can also stream a materialized result
	auto materialized = con.Query("SELECT CAST(a AS INTEGER) FROM test ORDER BY a");
	VerifyStreamResult(std::move(materialized));
	// return multiple results using the stream API
	result = con.SendQuery("SELECT CAST(a AS INTEGER) FROM test ORDER BY a; SELECT CAST(a AS INTEGER) FROM test ORDER "
	                       "BY a; SELECT CAST(a AS INTEGER) FROM test ORDER BY a;");
	auto next = std::move(result->next);
	while (next) {
		auto nextnext = std::move(next->next);
		VerifyStreamResult(std::move(nextnext));
		next = std::move(nextnext);
	}
	VerifyStreamResult(std::move(result));
}

TEST_CASE("Test TryFlushCachingOperators interrupted ExecutePushInternal", "[api][.]") {
	DuckDB db;
	Connection con(db);

	con.Query("create table tbl as select 100000 a from range(2) t(a);");
	con.Query("pragma threads=1");

	// Use PhysicalCrossProduct with a very low amount of produced tuples, this caches the result in the
	// CachingOperatorState This gets flushed with FinalExecute in PipelineExecutor::TryFlushCachingOperator
	auto pending_query = con.PendingQuery("select unnest(range(a.a)) from tbl a, tbl b;");

	// Through `unnest(range(a.a.))` this FinalExecute multiple chunks, more than the ExecutionBudget can handle with
	// PROCESS_PARTIAL
	pending_query->ExecuteTask();

	// query the connection as normal after
	auto res = pending_query->Execute();
	REQUIRE(!res->HasError());
	auto &materialized_res = res->Cast<MaterializedQueryResult>();
	idx_t initial_tuples = 2 * 2;
	REQUIRE(materialized_res.RowCount() == initial_tuples * 100000);
	for (idx_t i = 0; i < initial_tuples; i++) {
		for (idx_t j = 0; j < 100000; j++) {
			auto value = static_cast<idx_t>(materialized_res.GetValue<int64_t>(0, (i * 100000) + j));
			REQUIRE(value == j);
		}
	}
}

TEST_CASE("Test streaming query during stack unwinding", "[api]") {
	DuckDB db;
	Connection con(db);

	try {
		auto result = con.SendQuery("SELECT * FROM range(1000000)");

		throw std::runtime_error("hello");
	} catch (...) {
	}
}

TEST_CASE("Test prepare dependencies with multiple connections", "[catalog]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	auto con = make_uniq<Connection>(db);
	auto con2 = make_uniq<Connection>(db);
	auto con3 = make_uniq<Connection>(db);

	// simple prepare: begin transaction before the second client calls PREPARE
	REQUIRE_NO_FAIL(con->Query("CREATE TABLE integers(i INTEGER)"));
	// open a transaction in con2, this forces the prepared statement to be kept around until this transaction is closed
	REQUIRE_NO_FAIL(con2->Query("BEGIN TRANSACTION"));
	// we prepare a statement in con
	REQUIRE_NO_FAIL(con->Query("PREPARE s1 AS SELECT * FROM integers"));
	// now we drop con while the second client still has an active transaction
	con.reset();
	// now commit the transaction in the second client
	REQUIRE_NO_FAIL(con2->Query("COMMIT"));

	con = make_uniq<Connection>(db);
	// three transactions
	// open a transaction in con2, this forces the prepared statement to be kept around until this transaction is closed
	REQUIRE_NO_FAIL(con2->Query("BEGIN TRANSACTION"));
	// create a prepare, this creates a dependency from s1 -> integers
	REQUIRE_NO_FAIL(con->Query("PREPARE s1 AS SELECT * FROM integers"));
	// drop the client
	con.reset();
	// now begin a transaction in con3
	REQUIRE_NO_FAIL(con3->Query("BEGIN TRANSACTION"));
	// drop the table integers with cascade, this should drop s1 as well
	REQUIRE_NO_FAIL(con3->Query("DROP TABLE integers CASCADE"));
	REQUIRE_NO_FAIL(con2->Query("COMMIT"));
	REQUIRE_NO_FAIL(con3->Query("COMMIT"));
}

TEST_CASE("Test connection API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// extract a plan node
	REQUIRE_NOTHROW(con.ExtractPlan("SELECT 42"));
	// can only extract one statement at a time
	REQUIRE_THROWS(con.ExtractPlan("SELECT 42; SELECT 84"));

	// append to a table
	con.Query("CREATE TABLE integers(i integer);");
	auto table_info = con.TableInfo("integers");

	DataChunk chunk;
	REQUIRE_NOTHROW(con.Append(*table_info, chunk));

	// no transaction active
	REQUIRE_THROWS(con.Commit());
	REQUIRE_THROWS(con.Rollback());

	// cannot start a transaction within a transaction
	REQUIRE_NOTHROW(con.BeginTransaction());
	REQUIRE_THROWS(con.BeginTransaction());

	con.SetAutoCommit(false);
	REQUIRE(!con.IsAutoCommit());

	con.SetAutoCommit(true);
	REQUIRE(con.IsAutoCommit());
}

TEST_CASE("Test parser tokenize", "[api]") {
	Parser parser;
	REQUIRE_NOTHROW(parser.Tokenize("SELECT * FROM table WHERE i+1=3 AND j='hello'; --tokenize example query"));
}

TEST_CASE("Test opening an invalid database file", "[api]") {
	duckdb::unique_ptr<DuckDB> db;
	bool success = false;
	try {
		db = make_uniq<DuckDB>("duckdb:data/parquet-testing/blob.parquet");
		success = true;
	} catch (std::exception &ex) {
		REQUIRE(StringUtil::Contains(ex.what(), "DuckDB"));
	}
	REQUIRE(!success);
	try {
		db = make_uniq<DuckDB>("duckdb:data/parquet-testing/h2oai/h2oai_group_small.parquet");
		success = true;
	} catch (std::exception &ex) {
		REQUIRE(StringUtil::Contains(ex.what(), "DuckDB"));
	}
	REQUIRE(!success);
}

TEST_CASE("Test large number of connections to a single database", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto context = make_uniq<ClientContext>((*db).instance);
	auto &connection_manager = ConnectionManager::Get(*context);

	duckdb::vector<duckdb::unique_ptr<Connection>> connections;
	size_t createdConnections = 5000;
	size_t remainingConnections = 500;
	size_t toRemove = createdConnections - remainingConnections;

	for (size_t i = 0; i < createdConnections; i++) {
		auto conn = make_uniq<Connection>(*db);
		connections.push_back(std::move(conn));
	}

	REQUIRE(connection_manager.GetConnectionCount() == createdConnections);

	for (size_t i = 0; i < toRemove; i++) {
		connections.erase(connections.begin());
	}

	REQUIRE(connection_manager.GetConnectionCount() == remainingConnections);
}

TEST_CASE("Issue #4583: Catch Insert/Update/Delete errors", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	duckdb::unique_ptr<QueryResult> result;

	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0 (c0 int);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (1);"));

	result = con.SendQuery(
	    "INSERT INTO t0(VALUES('\\x15\\x00\\x00\\x00\\x00@\\x01\\x0A\\x27:!\\x0A\\x00\\x00x12e\"\\x00'::BLOB));");
	//! Should not terminate the process
	REQUIRE_FAIL(result);

	result = con.SendQuery("SELECT MIN(c0) FROM t0;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Issue #14130: InsertStatement::ToString causes InternalException later on", "[api][.]") {
	auto db = DuckDB(nullptr);
	auto conn = Connection(db);

	conn.Query("CREATE TABLE foo(a int, b varchar, c int)");

	auto query = "INSERT INTO Foo values (1, 'qwerty', 42)";

	auto stmts = conn.ExtractStatements(query);
	auto &stmt = stmts[0];

	// Issue was here: calling ToString destroyed the 'alias' of the ValuesList
	stmt->ToString();
	// Which caused an 'InternalException: expected non-empty binding_name' here
	auto prepared_stmt = conn.Prepare(std::move(stmt));
	REQUIRE(!prepared_stmt->HasError());
	REQUIRE_NO_FAIL(prepared_stmt->Execute());
}

TEST_CASE("Issue #6284: CachingPhysicalOperator in pull causes issues", "[api][.]") {

	DBConfig config;
	config.options.maximum_threads = 8;
	DuckDB db(nullptr, &config);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("select setseed(0.1); CREATE TABLE T0 AS SELECT DISTINCT (RANDOM()*9999999)::BIGINT "
	                          "record_nb, 0.0 x_0, 1.0 y_0 FROM range(1000000) tbl"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE T1 AS SELECT record_nb, 0.0 x_1, 1.0 y_1 FROM T0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE T2 AS SELECT record_nb, 0.0 x_2, 1.0 y_2 FROM T0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE T3 AS SELECT record_nb, 0.0 x_3, 1.0 y_3 FROM T0"));
	auto result = con.SendQuery(R"(
        SELECT T0.record_nb,
            T1.x_1 x_1,
            T1.y_1 y_1,
            T2.x_2 x_2,
            T2.y_2 y_2,
            T3.x_3 x_3,
            T3.y_3 y_3
         FROM T0
           INNER JOIN T1 on T0.record_nb = T1.record_nb
           INNER JOIN T2 on T0.record_nb = T2.record_nb
           INNER JOIN T3 on T0.record_nb = T3.record_nb
    )");

	idx_t count = 0;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk) {
			break;
		}
		if (chunk->size() == 0) {
			break;
		}
		count += chunk->size();
	}

	REQUIRE(951382 == count);
}

TEST_CASE("Fuzzer 50 - Alter table heap-use-after-free", "[api]") {
	// FIXME: not fixed yet
	return;
	DuckDB db(nullptr);
	Connection con(db);

	con.SendQuery("CREATE TABLE t0(c0 INT);");
	con.SendQuery("ALTER TABLE t0 ADD c1 TIMESTAMP_SEC;");
}

TEST_CASE("Test loading database with enable_external_access set to false", "[api]") {
	DBConfig config;
	config.options.enable_external_access = false;
	auto path = TestCreatePath("external_access_test");
	DuckDB db(path, &config);
	Connection con(db);

	REQUIRE_FAIL(con.Query("ATTACH 'mydb.db' AS external_access_test"));
}

TEST_CASE("Test insert returning in CPP API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE test(val VARCHAR);");

	con.Query("INSERT INTO test(val) VALUES ('query_1')");
	auto res = con.Query("INSERT INTO test(val) VALUES ('query_2') returning *");
	REQUIRE(CHECK_COLUMN(res, 0, {"query_2"}));

	con.Query("INSERT INTO test(val) VALUES (?);", "query_arg_1");
	auto returning_args = con.Query("INSERT INTO test(val) VALUES (?) RETURNING *;", "query_arg_2");
	REQUIRE(CHECK_COLUMN(returning_args, 0, {"query_arg_2"}));

	con.Prepare("INSERT INTO test(val) VALUES (?);")->Execute("prepared_arg_1");
	auto prepared_returning_args =
	    con.Prepare("INSERT INTO test(val) VALUES (?) returning *;")->Execute("prepared_arg_2");
	REQUIRE(CHECK_COLUMN(prepared_returning_args, 0, {"prepared_arg_2"}));

	// make sure all inserts actually inserted
	auto result = con.Query("SELECT * from test;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"query_1", "query_2", "query_arg_1", "query_arg_2", "prepared_arg_1", "prepared_arg_2"}));
}

TEST_CASE("Test a logical execute still has types after an optimization pass", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("PREPARE test AS SELECT 42::INTEGER;");
	const auto query_plan = con.ExtractPlan("EXECUTE test");
	REQUIRE((query_plan->type == LogicalOperatorType::LOGICAL_EXECUTE));
	REQUIRE((query_plan->types.size() == 1));
	REQUIRE((query_plan->types[0].id() == LogicalTypeId::INTEGER));
}

TEST_CASE("Test SqlStatement::ToString for UPDATE, INSERT, DELETE statements with alias of RETURNING clause", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	std::string sql;
	con.Query("CREATE TABLE test(id INT);");

	sql = "INSERT INTO test (id) VALUES (1) RETURNING id AS inserted";
	auto stmts = con.ExtractStatements(sql);
	REQUIRE(stmts[0]->ToString() == "INSERT INTO test (id ) (VALUES (1)) RETURNING id AS inserted");

	sql = "UPDATE test SET id = 1 RETURNING id AS updated";
	stmts = con.ExtractStatements(sql);
	REQUIRE(stmts[0]->ToString() == sql);

	sql = "DELETE FROM test WHERE (id = 1) RETURNING id AS deleted";
	stmts = con.ExtractStatements(sql);
	REQUIRE(stmts[0]->ToString() == sql);
}

class DuckDBAppCpp {
private:
    std::unique_ptr<duckdb::DuckDB> database;

    // Thread configuration flags
    bool enable_loading_thread{true};
    bool enable_mutating_thread{true};
    bool enable_mutating2_thread{false};
    bool enable_checkpointing_thread{true};

    std::atomic<bool> loading_exit{false};
    std::atomic<bool> mutating_exit{false};
    std::atomic<bool> mutating2_exit{false};
    std::atomic<bool> checkpointing_exit{false};
    std::atomic<bool> fatal_error{false};

    std::mt19937 rng;

public:
    DuckDBAppCpp() : rng(std::random_device{}()) {
        // Open the database
        database = make_uniq<duckdb::DuckDB>("test.db");

        // Output DuckDB version
        duckdb::Connection temp_conn(*database);
        auto version_result = temp_conn.Query("SELECT version()");
        if (version_result->HasError()) {
            std::cerr << "Failed to get DuckDB version: " << version_result->GetError() << std::endl;
        } else if (version_result->RowCount() > 0) {
            std::string version = version_result->GetValue(0, 0).GetValue<std::string>();
            std::cout << "DuckDB Version: " << version << std::endl;
        } else {
            std::cout << "DuckDB Version: No rows returned" << std::endl;
        }

        createTable();
    }

    // Constructor with thread configuration
    DuckDBAppCpp(bool enable_loading, bool enable_mutating, bool enable_mutating2, bool enable_checkpointing)
        : rng(std::random_device{}()),
          enable_loading_thread(enable_loading),
          enable_mutating_thread(enable_mutating),
          enable_mutating2_thread(enable_mutating2),
          enable_checkpointing_thread(enable_checkpointing) {
        // Open the database
        database = make_uniq<duckdb::DuckDB>("test.db");

        // Output DuckDB version
        duckdb::Connection temp_conn(*database);
    	//temp_conn.Query("SET experimental_metadata_reuse=true");
        auto version_result = temp_conn.Query("SELECT version()");
        if (version_result->HasError()) {
            std::cerr << "Failed to get DuckDB version: " << version_result->GetError() << std::endl;
        } else if (version_result->RowCount() > 0) {
            std::string version = version_result->GetValue(0, 0).GetValue<std::string>();
            std::cout << "DuckDB Version: " << version << std::endl;
        } else {
            std::cout << "DuckDB Version: No rows returned" << std::endl;
        }

        createTable();
    }

    ~DuckDBAppCpp() = default; // RAII handles cleanup automatically

    // Helper function to check for fatal errors
    bool checkFatalError(const std::string& error_msg) {
        if (error_msg.find("FATAL Error") != std::string::npos ||
            error_msg.find("database has been invalidated") != std::string::npos) {
            std::cerr << "FATAL ERROR DETECTED: " << error_msg << std::endl;
            std::cerr << "Setting fatal_error flag and exiting threads..." << std::endl;
            fatal_error = true;
            return true;
        }
        return false;
    }

    void createTable() {
        const char* create_sql = R"(
            CREATE or REPLACE TABLE t1 (
                md_session_id VARCHAR,
                user_email VARCHAR,
                event_name VARCHAR,
                event_ts TIMESTAMP,
                event_version INTEGER,
                extra VARCHAR,
                file_name VARCHAR,
                line_number INTEGER,
                insert_ts TIMESTAMP,
                batch_file_id UUID,
                duckling_version VARCHAR,
                pod_name VARCHAR,
                connection_id UUID,
                query_id UBIGINT,
                duckdb_id UUID,
                user_id UUID,
                query_type VARCHAR,
                query_properties STRUCT(
                    tools STRUCT(
                        is_dbt BOOLEAN,
                        is_airbyte BOOLEAN
                    ),
                    external_storage STRUCT(
                        s3 BOOLEAN,
                        gcp BOOLEAN,
                        azure BOOLEAN
                    )
                ),
                md_sql_metadata JSON,
                is_hatchling BOOLEAN,
                is_hatchling_automated_query BOOLEAN,
                transaction_id UBIGINT,
                organization_id UUID,
                host VARCHAR,
                trace_id VARCHAR,
                message_id VARCHAR
            );
        )";

        const char* create_t2_sql = R"(
            CREATE or REPLACE TABLE t2 (
                md_session_id VARCHAR,
                user_email VARCHAR,
                event_name VARCHAR,
                event_ts TIMESTAMP,
                event_version INTEGER,
                extra VARCHAR,
                file_name VARCHAR,
                line_number INTEGER,
                insert_ts TIMESTAMP,
                batch_file_id UUID,
                duckling_version VARCHAR,
                pod_name VARCHAR,
                connection_id UUID,
                query_id UBIGINT,
                duckdb_id UUID,
                user_id UUID,
                query_type VARCHAR,
                query_properties STRUCT(
                    tools STRUCT(
                        is_dbt BOOLEAN,
                        is_airbyte BOOLEAN
                    ),
                    external_storage STRUCT(
                        s3 BOOLEAN,
                        gcp BOOLEAN,
                        azure BOOLEAN
                    )
                ),
                md_sql_metadata JSON,
                is_hatchling BOOLEAN,
                is_hatchling_automated_query BOOLEAN,
                transaction_id UBIGINT,
                organization_id UUID,
                host VARCHAR,
                trace_id VARCHAR,
                message_id VARCHAR
            );
        )";

        // Create a temporary connection for table creation
        duckdb::Connection temp_conn(*database);

        try {
            auto result1 = temp_conn.Query(create_sql);
            if (result1->HasError()) {
                throw std::runtime_error("Failed to create table t1: " + result1->GetError());
            }

            auto result2 = temp_conn.Query(create_t2_sql);
            if (result2->HasError()) {
                throw std::runtime_error("Failed to create table t2: " + result2->GetError());
            }

            std::cout << "Tables t1 and t2 created successfully" << std::endl;
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to create tables: " + std::string(e.what()));
        }
    }

    void loadingThread() {
        std::string sql = R"""(
			INSERT INTO t1 BY NAME
			-- INSERT INTO mdw_stg_v10.raw.datadog_combined_logs_landing BY NAME
            SELECT md_session_id::varchar as md_session_id, event_name::varchar as event_name, make_timestamp(event_ts) as event_ts, extra::varchar as extra, query_id::UBIGINT as query_id, duckling_version::varchar as duckling_version, pod_name::varchar as pod_name, connection_id::UUID as connection_id, duckdb_id::UUID as duckdb_id, line_number::int as line_number, file_name::varchar as file_name, batch_file_id::uuid as batch_file_id, transaction_id::ubigint as transaction_id, host::varchar as host, trace_id::varchar as trace_id, message_id::varchar as message_id, user_id::uuid as user_id,
            now() as insert_ts
            FROM read_parquet('/Users/ywelsch/Downloads/flat_vec_repro/parquet/*')
		)""";

        while (!loading_exit.load() && !fatal_error.load()) {
            // Create a fresh connection for each iteration
            duckdb::Connection conn(*database);
            std::cout << "Loading data" << std::endl;

            std::uniform_real_distribution<double> dist(0.0, 1.0);
            int limit = static_cast<int>(200000 * dist(rng));

            std::string query = sql + " limit " + std::to_string(limit);

            try {
                auto result = conn.Query(query);
                if (result->HasError()) {
                    std::string error_msg = result->GetError();
                    std::cerr << "Loading query failed: " << error_msg << std::endl;
                    if (checkFatalError(error_msg)) {
                        std::cerr << "Loading thread exiting due to fatal error" << std::endl;
                        break;
                    }
                    continue;
                }
            } catch (const std::exception& e) {
                std::string error_msg = e.what();
                std::cerr << "Loading query exception: " << error_msg << std::endl;
                if (checkFatalError(error_msg)) {
                    std::cerr << "Loading thread exiting due to fatal error" << std::endl;
                    break;
                }
                continue;
            }
        }

        std::cout << "Loading thread finished" << std::endl;
    }

    void mutatingThread() {
        while (!mutating_exit.load() && !fatal_error.load()) {
            // Create a fresh connection for each iteration
            duckdb::Connection conn(*database);
            try {

                std::this_thread::sleep_for(std::chrono::duration<float>(0.1f));

                // Get max insert_ts and count
                std::unique_ptr<duckdb::MaterializedQueryResult> result;
                result = conn.Query("select max(insert_ts), count(1) c from t1");
                if (result->HasError()) {
                    std::string error_msg = result->GetError();
                    std::cerr << "Mutating query failed: " << error_msg << std::endl;
                    if (checkFatalError(error_msg)) {
                        std::cerr << "Mutating thread exiting due to fatal error" << std::endl;
                        break;
                    }
                    continue;
                }

                if (result->RowCount() == 0) {
                    std::cout << "no rows" << std::endl;
                    continue;
                }

                // Get the max insert_ts value
                auto max_insert_ts_value = result->GetValue(0, 0);
                if (max_insert_ts_value.IsNull()) {
                    std::cout << "Failed to get max insert_ts" << std::endl;
                    continue;
                }

                std::string max_insert_ts = max_insert_ts_value.GetValue<std::string>();

                std::this_thread::sleep_for(std::chrono::duration<float>(1.23f));
                // Query with filter
                std::string filter_query = "select insert_ts, count(1) c, count(1) filter (where insert_ts <= '" + max_insert_ts + "') c2 from t1 group by insert_ts order by insert_ts";

                std::unique_ptr<duckdb::MaterializedQueryResult> filter_result;
                filter_result = conn.Query(filter_query);
                if (filter_result->HasError()) {
                    std::cerr << "Filter query failed: " << filter_result->GetError() << std::endl;
                    continue;
                }

                // Print results
                for (size_t row = 0; row < filter_result->RowCount(); row++) {
                    auto insert_ts_value = filter_result->GetValue(0, row);
                    auto count1_value = filter_result->GetValue(1, row);
                    auto count2_value = filter_result->GetValue(2, row);

                    std::cout << insert_ts_value.GetValue<std::string>() << " "
                              << count1_value.GetValue<int64_t>() << " "
                              << count2_value.GetValue<int64_t>() << std::endl;
                }

                // Update and copy operations
                std::string update_query = "update t1 set user_email = md5(random()::varchar) where insert_ts <= '" + max_insert_ts + "'";
                std::string copy_query = "insert into t2 select * from t1 where insert_ts <= '" + max_insert_ts + "'";
                std::string delete_query = "delete from t1 where insert_ts <= '" + max_insert_ts + "'";

                auto update_result = conn.Query(update_query);
                if (update_result->HasError()) {
                    std::cerr << "Update query failed: " << update_result->GetError() << std::endl;
                }

                // Copy selected rows from t1 to t2
                auto copy_result = conn.Query(copy_query);
                if (copy_result->HasError()) {
                    std::cerr << "Copy query failed: " << copy_result->GetError() << std::endl;
                } else {
                    std::cout << "Copied rows from t1 to t2" << std::endl;
                }

                auto delete_result = conn.Query(delete_query);
                if (delete_result->HasError()) {
                    std::cerr << "Delete query failed: " << delete_result->GetError() << std::endl;
                }

            } catch (const std::exception& e) {
                std::string error_msg = e.what();
                std::cerr << "Mutating thread exception: " << error_msg << std::endl;
                if (checkFatalError(error_msg)) {
                    std::cerr << "Mutating thread exiting due to fatal error" << std::endl;
                    break;
                }
                continue;
            }
        }

        std::cout << "Mutating thread finished" << std::endl;
    }

    void mutating2Thread() {
        while (!mutating2_exit.load() && !fatal_error.load()) {
            // Create a fresh connection for each iteration
            duckdb::Connection conn(*database);
            try {
                // Randomly alter email column for the whole table
                std::uniform_real_distribution<double> dist(0.0, 1.0);

                std::string update_query = "update t1 set user_email = md5(random()::varchar) where random() < 0.1";

                auto update_result = conn.Query(update_query);
                if (update_result->HasError()) {
                    std::cerr << "Random update query failed: " << update_result->GetError() << std::endl;
                } else {
                    std::cout << "Randomly updated emails " << std::endl;
                }

            } catch (const std::exception& e) {
                std::string error_msg = e.what();
                std::cerr << "Mutating2 thread exception: " << error_msg << std::endl;
                if (checkFatalError(error_msg)) {
                    std::cerr << "Mutating2 thread exiting due to fatal error" << std::endl;
                    break;
                }
                continue;
            }

            // Random sleep between 1-5 seconds
            std::uniform_real_distribution<double> sleep_dist(1.0, 5.0);
            std::this_thread::sleep_for(std::chrono::duration<double>(sleep_dist(rng)));
        }

        std::cout << "Mutating2 thread finished" << std::endl;
    }

    void checkpointingThread() {
        while (!checkpointing_exit.load() && !fatal_error.load()) {
            // Create a fresh connection for each iteration
            duckdb::Connection conn(*database);
            std::cout << "Checkpointing data" << std::endl;

            try {
                // Randomly use FORCE CHECKPOINT 1/3 of the time
                std::uniform_real_distribution<double> checkpoint_dist(0.0, 1.0);
                std::string checkpoint_query = (checkpoint_dist(rng) < 0.33) ? "FORCE CHECKPOINT" : "checkpoint";

                auto result = conn.Query(checkpoint_query);
                if (result->HasError()) {
                    std::string error_msg = result->GetError();
                    std::cerr << "TransactionException: " << error_msg << std::endl;
                    if (checkFatalError(error_msg)) {
                        std::cerr << "Checkpointing thread exiting due to fatal error" << std::endl;
                        break;
                    }
                } else {
                    std::cout << "Checkpoint successful: " << checkpoint_query << std::endl;
                }
            } catch (const std::exception& e) {
                std::string error_msg = e.what();
                std::cerr << "Checkpoint exception: " << error_msg << std::endl;
                if (checkFatalError(error_msg)) {
                    std::cerr << "Checkpointing thread exiting due to fatal error" << std::endl;
                    break;
                }
            }

            std::uniform_real_distribution<double> dist(0.0, 1.0);
            std::this_thread::sleep_for(std::chrono::duration<double>(dist(rng)));
        }

        std::cout << "Checkpointing thread finished" << std::endl;
    }

    void run() {
        std::cout << "Starting threads..." << std::endl;

        // Conditionally start threads based on configuration flags
        std::unique_ptr<std::thread> t1, t2, t3, t4;

        if (enable_loading_thread) {
            t1 = make_uniq<std::thread>(&DuckDBAppCpp::loadingThread, this);
            std::cout << "Loading thread started" << std::endl;
        }

        if (enable_mutating_thread) {
            t2 = make_uniq<std::thread>(&DuckDBAppCpp::mutatingThread, this);
            std::cout << "Mutating thread started" << std::endl;
        }

        if (enable_mutating2_thread) {
            t3 = make_uniq<std::thread>(&DuckDBAppCpp::mutating2Thread, this);
            std::cout << "Mutating2 thread started" << std::endl;
        }

        if (enable_checkpointing_thread) {
            t4 = make_uniq<std::thread>(&DuckDBAppCpp::checkpointingThread, this);
            std::cout << "Checkpointing thread started" << std::endl;
        }

        // Run for 500 seconds (like the Python version)
        std::this_thread::sleep_for(std::chrono::seconds(1800));

        // Signal threads to exit
        if (enable_loading_thread) loading_exit = true;
        if (enable_mutating_thread) mutating_exit = true;
        if (enable_mutating2_thread) mutating2_exit = true;
        if (enable_checkpointing_thread) checkpointing_exit = true;

        // Wait for threads to finish
        if (t1 && t1->joinable()) {
            t1->join();
        }

        if (t2 && t2->joinable()) {
            t2->join();
        }

        if (t3 && t3->joinable()) {
            t3->join();
        }

        if (t4 && t4->joinable()) {
            t4->join();
        }

        std::cout << "Main thread finished" << std::endl;
    }
};

TEST_CASE("Test concurrent loads", "[api]") {
	DuckDBAppCpp app;

	// Or configure specific threads:
	// DuckDBAppCpp app(true, true, false, true);  // Disable mutating2 thread
	// DuckDBAppCpp app(false, true, true, false); // Only mutation threads
	// DuckDBAppCpp app(true, false, false, false); // Only loading thread

	app.run();
}
