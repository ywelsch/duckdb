//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/transaction_row_groups.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class RowGroupSegmentTree;

//! The row groups a transaction reads a table from
struct TransactionRowGroups {
	shared_ptr<RowGroupSegmentTree> row_groups;
	//! Counts the checkpoints that gave rows of the table new row ids
	idx_t row_id_version = 0;
	//! Whether such a checkpoint ran since: these are the row groups from before it, not the live ones
	bool stale = false;
};

} // namespace duckdb
