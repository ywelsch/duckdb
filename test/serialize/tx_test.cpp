#include "catch.hpp"
#include "test_helpers.hpp"
#include <thread>
#include <chrono>

#include "duckdb.hpp"

TEST_CASE("Test checkpoint", "[checkpoint]") {
	duckdb::DuckDB db1;
	duckdb::Connection con(db1);
	duckdb::Connection con2(db1);

	REQUIRE_NO_FAIL(con.Query("create table test as select 1 as name from range(1000)"));

	REQUIRE_NO_FAIL(con.Query("PREPARE s1 AS INSERT INTO test VALUES (?::INTEGER)"));

	auto thread = std::thread([&] {
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		printf("Running query 2\n");
		REQUIRE_NO_FAIL(con2.Query("CREATE OR REPLACE TABLE test AS select 'foo' as bla"));
		printf("Query 2 completed\n");
	});

	printf("Running query 1\n");
	REQUIRE_NO_FAIL(con.Query("EXECUTE s1(1)"));
	printf("Query 1 completed\n");

	thread.join();
}
