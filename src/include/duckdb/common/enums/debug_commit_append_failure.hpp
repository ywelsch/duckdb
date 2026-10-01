//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/debug_commit_append_failure.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Where to force the commit-time append of a transaction's rows to a table to fail (debug_force_commit_append_failure)
enum class DebugCommitAppendFailure : uint8_t {
	//! Do not force a failure
	NONE = 0,
	//! Fail while appending the rows to the indexes, after the first chunk was appended
	INDEX_APPEND,
	//! Fail while appending the rows to the table, after the first chunk was appended (after the index append)
	TABLE_APPEND,
	//! Fail while merging the row groups into the table in the bulk-append path (after the index append)
	MERGE_STORAGE,
	//! Fail while preparing that merge (before the index append)
	PREPARE_MERGE_STORAGE
};

} // namespace duckdb
