#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/alter_database_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAlter::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	if (info->type == AlterType::ALTER_DATABASE) {
		auto &db_info = info->Cast<AlterDatabaseInfo>();
		auto &db_manager = DatabaseManager::Get(context.client);
		db_manager.Alter(context.client, db_info);
	} else {
		auto &catalog = Catalog::GetCatalog(context.client, info->GetQualifiedName().Catalog());
		unique_ptr<StorageLockKey> checkpoint_lock;
		if (catalog.IsDuckCatalog() && info->GetModifications().AlterTable()) {
			// the ALTER copies the row groups of the table: no checkpoint may replace them while it does
			checkpoint_lock = DuckTransaction::Get(context.client, catalog).GetCheckpointLockForStatement();
		}
		catalog.Alter(context.client, *info);
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
