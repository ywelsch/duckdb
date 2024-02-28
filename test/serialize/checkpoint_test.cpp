#include "catch.hpp"

#include "test_helpers.hpp"

#define RELOAD_DB() { \
REQUIRE_NO_FAIL(con.Query("USE my_db;")); \
REQUIRE_NO_FAIL(con.Query("DETACH test_storage_fuzzed;")); \
REQUIRE_NO_FAIL(con.Query("ATTACH 'test_storage_fuzzed.db';")); \
REQUIRE_NO_FAIL(con.Query("USE test_storage_fuzzed;")); \
} \

TEST_CASE("Fatal exception during checkpointing", "[bug]") {
	duckdb::DuckDB db(nullptr);

	duckdb::Connection con(db);
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS my_db;"));
	REQUIRE_NO_FAIL(con.Query("ATTACH 'test_storage_fuzzed.db';"));
	REQUIRE_NO_FAIL(con.Query("use test_storage_fuzzed;"));
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE test (pk UINT64, int1 UINT64, string1 VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT range, hash(range), md5(range::VARCHAR) || md5(range::VARCHAR) FROM range(0, 1000000);"));

	REQUIRE_NO_FAIL(con.Query("CHECKPOINT test_storage_fuzzed;"));
	RELOAD_DB()
	REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE pk > 738645 AND pk < 978908;"));
	RELOAD_DB()
	REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE pk > 282475 AND pk < 522738;"));
	RELOAD_DB()
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT range, hash(range), md5(range::VARCHAR) || md5(range::VARCHAR) FROM range(1201414, 1201514);"));
	RELOAD_DB()
	REQUIRE_NO_FAIL(con.Query("SELECT bit_xor(pk), bit_xor(int1), bit_xor(hash(string1)) FROM test;"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT test_storage_fuzzed;"));
}