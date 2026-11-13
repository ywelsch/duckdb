#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("Test that releasing an invalid block id does not poison the free list", "[storage]") {
	auto path = TestCreatePath("free_invalid_block_ids.db");
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET trim_free_blocks = true"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range AS i FROM range(1000000)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));

		con.BeginTransaction();
		auto &block_manager = StorageManager::Get(Catalog::GetCatalog(*con.context, "")).GetBlockManager();
		con.Commit();
		// a corrupt block pointer can hold any 64-bit value
		block_manager.MarkBlockAsModified(block_id_t(-9207709771714237280LL));
		block_manager.MarkBlockAsModified(NumericLimits<block_id_t>::Maximum());

		// the checkpoint trims the freed blocks, and the inserts after it reuse free blocks
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT range FROM range(1000000)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2 AS SELECT range AS i FROM range(1000000)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	DuckDB db(path);
	Connection con(db);
	auto result = con.Query("SELECT (SELECT count(*) FROM t), (SELECT count(*) FROM t2)");
	REQUIRE(CHECK_COLUMN(result, 0, {2000000}));
	REQUIRE(CHECK_COLUMN(result, 1, {1000000}));
}
