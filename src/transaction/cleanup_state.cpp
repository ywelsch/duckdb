#include "duckdb/transaction/cleanup_state.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/transaction/append_info.hpp"

#include "duckdb/storage/data_table.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/transaction/commit_state.hpp"

namespace duckdb {

CleanupState::CleanupState(DuckTransaction &transaction, VisibilityBound lowest_visibility_bound,
                           ActiveTransactionState transaction_state, bool dropped_storage_freed)
    : lowest_visibility_bound(lowest_visibility_bound), transaction_state(transaction_state),
      index_data_remover(transaction, QueryContext(), IndexRemovalType::DELETED_ROWS_IN_USE),
      dropped_storage_freed(dropped_storage_freed) {
}

void CleanupState::CleanupEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry);
		auto &entry = *catalog_entry;
		D_ASSERT(entry.set);
		CleanupEntryDrop(entry);
		entry.set->CleanupEntry(entry);
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		auto info = reinterpret_cast<AppendInfo *>(data);
		// mark the tuples as committed
		info->table->GetStorage().CleanupAppend(lowest_visibility_bound, info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		auto info = reinterpret_cast<DeleteInfo *>(data);
		CleanupDelete(*info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		auto info = reinterpret_cast<UpdateInfo *>(data);
		CleanupUpdate(*info);
		break;
	}
	default:
		break;
	}
}

void CleanupState::CleanupEntryDrop(CatalogEntry &entry) {
	if (entry.temporary || entry.Parent().temporary || entry.Parent().type != CatalogType::DELETED_ENTRY) {
		return;
	}
	switch (entry.type) {
	case CatalogType::TABLE_ENTRY: {
		auto &table_entry = entry.Cast<DuckTableEntry>();
		D_ASSERT(table_entry.IsDuckTable());
		auto &table = table_entry.GetStorage();
		if (dropped_storage_freed) {
			// the checkpoint freed the blocks of the indexes with those of the table
			table.GetDataTableInfo()->GetIndexes().DisownStorage();
			break;
		}
		CommitDropState drop_state(table.GetAttached().GetStorageManager().GetBlockManager());
		table_entry.CleanupDrop(drop_state);
		drop_state.FinalizeCommit();
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		auto &index_entry = entry.Cast<DuckIndexEntry>();
		index_entry.GetDataTableInfo().GetIndexes().RemoveDetachedIndex(index_entry.oid);
		break;
	}
	default:
		break;
	}
}

void CleanupState::CleanupUpdate(UpdateInfo &info) {
	// remove the update info from the update chain
	info.segment->CleanupUpdate(info);
}

void CleanupState::CleanupDelete(DeleteInfo &info) {
	if (transaction_state == ActiveTransactionState::NO_OTHER_TRANSACTIONS) {
		// if there are no active transactions we don't need to do any clean-up, as we haven't written to
		// deleted_rows_in_use
		return;
	}
	index_data_remover.PushDelete(info);
}

} // namespace duckdb
