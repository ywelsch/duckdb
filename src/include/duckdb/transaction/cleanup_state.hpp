//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/cleanup_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/transaction/commit_state.hpp"

namespace duckdb {

class DataTable;

struct DeleteInfo;
struct UpdateInfo;

class CleanupState {
public:
	explicit CleanupState(DuckTransaction &transaction, VisibilityBound lowest_visibility_bound,
	                      ActiveTransactionState transaction_state, bool dropped_storage_freed);

public:
	void CleanupEntry(UndoFlags type, data_ptr_t data);

private:
	//! Lowest active transaction
	VisibilityBound lowest_visibility_bound;
	ActiveTransactionState transaction_state;
	//! While cleaning up, we remove data from any delta indexes we added data to during the commit
	IndexDataRemover index_data_remover;
	//! Whether a checkpoint already freed the storage that the transaction dropped
	bool dropped_storage_freed;

private:
	//! Frees the storage of a dropped table or index, which no snapshot reads any more
	void CleanupEntryDrop(CatalogEntry &entry);
	void CleanupDelete(DeleteInfo &info);
	void CleanupUpdate(UpdateInfo &info);
};

} // namespace duckdb
