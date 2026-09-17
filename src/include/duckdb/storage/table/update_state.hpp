//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/update_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {
class TableCatalogEntry;

struct TableUpdateState {
	unique_ptr<ConstraintState> constraint_state;
	//! Excludes a checkpoint of the table for the duration of the update
	shared_ptr<CheckpointLock> checkpoint_lock;
};

} // namespace duckdb
