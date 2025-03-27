#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb.hpp"

TEST_CASE("Test write amplification", "[writes]") {
    duckdb::DuckDB db {"local" + duckdb::UUID::ToString(duckdb::UUID::GenerateRandomUUID()) + ".db"};
    duckdb::Connection con {db};
    printf("CREATING TABLE\n");
    REQUIRE_NO_FAIL(con.Query("create table z as select sha256(i::varchar) as a, sha256((i**2)::varchar) as b, sha256((i**3)::varchar) as c from range(100000) r(i);"));
    printf("CHECKPOINTING TABLE A FIRST TIME\n");
    REQUIRE_NO_FAIL(con.Query("CHECKPOINT;"));
	printf("DONE CHECKPOINTING TABLE A FIRST TIME\n");
    printf("CHECKPOINTING TABLE A SECOND TIME\n");
    REQUIRE_NO_FAIL(con.Query("CHECKPOINT;"));
	printf("DONE CHECKPOINTING TABLE A SECOND TIME\n");
    printf("INSERTING EXTRA DATA 1\n");
    REQUIRE_NO_FAIL(con.Query("insert into z values('a','b','c');"));
    printf("DONE INSERTING EXTRA DATA 1\n");
	printf("CHECKPOINTING AFTER INSERTING EXTRA DATA\n");
    REQUIRE_NO_FAIL(con.Query("CHECKPOINT;"));
    printf("DONE CHECKPOINTING AFTER INSERTING EXTRA DATA\n");
    printf("INSERTING EXTRA DATA 2\n");
    REQUIRE_NO_FAIL(con.Query("insert into z values('a2','b2','c2');"));
    printf("DONE INSERTING EXTRA DATA 2\n");
	printf("CHECKPOINTING AFTER INSERTING EXTRA DATA 2\n");
    REQUIRE_NO_FAIL(con.Query("CHECKPOINT;"));
    printf("DONE CHECKPOINTING AFTER INSERTING EXTRA DATA 2\n");
}
