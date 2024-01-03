#include "catch.hpp"
#include "test_helpers.hpp"
#include <thread>
#include <chrono>
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/valid_checker.hpp"

#include "duckdb.hpp"

TEST_CASE("Test checkpoint", "[checkpoint]") {
	duckdb::DuckDB db1;

	// setup
	{
		duckdb::Connection con(db1);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
		REQUIRE_NO_FAIL(con.Query("SET threads to 1"));
	}

	{
		duckdb::Connection con(db1);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1)"));

		REQUIRE(con.context->transaction.HasActiveTransaction());
		REQUIRE_FALSE(duckdb::ValidChecker::Get(con.context->transaction.ActiveTransaction()).IsInvalidated());

		// uniqueness constraint violated on second insert
		REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (1)"));

		REQUIRE(con.context->transaction.HasActiveTransaction());
		REQUIRE(duckdb::ValidChecker::Get(con.context->transaction.ActiveTransaction()).IsInvalidated());
	}

	{
		duckdb::Connection con(db1);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1)"));

		REQUIRE(con.context->transaction.HasActiveTransaction());
		REQUIRE_FALSE(duckdb::ValidChecker::Get(con.context->transaction.ActiveTransaction()).IsInvalidated());

		auto pending_query = con.PendingQuery("INSERT INTO integers VALUES (1)");
		// never executes this (or only partially execute it in case there are background threads), but submit new query

		REQUIRE(con.context->transaction.HasActiveTransaction());
		// No invalidation has happened yet
		REQUIRE_FALSE(duckdb::ValidChecker::Get(con.context->transaction.ActiveTransaction()).IsInvalidated());

		// Now just send another query, the TX is not rolled back even though the previous query was interrupted / aborted
		REQUIRE_NO_FAIL(con.Query("FROM INTEGERS"));

		REQUIRE_THROWS(pending_query->Execute()); // the previous one throws (as it was interrupted by newer query)

		REQUIRE(con.context->transaction.HasActiveTransaction());
		// No invalidation has happened yet
		REQUIRE_FALSE(duckdb::ValidChecker::Get(con.context->transaction.ActiveTransaction()).IsInvalidated());

		// let's commit
		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		// check that the previous state was actually committed
		REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (1)")); // previous value got inserted
	}


}
