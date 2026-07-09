//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/append_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class DataTable;

struct AppendInfo {
	DataTable *table;
	idx_t start_row;
	idx_t count;
	//! Whether the pending commit append bookkeeping for this append has been resolved (committed or reverted).
	//! Guards against double resolution: a failed commit can revert the same append twice (once through
	//! RevertCommit and once more through the transaction rollback).
	bool pending_commit_resolved;
};

} // namespace duckdb
