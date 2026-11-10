#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/alter_database_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/operator/schema/physical_alter_column.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AlterSourceState : public GlobalSourceState {
public:
	AlterSourceState(ClientContext &context, AlterInfo &info) : context(context), info(info) {
	}
	~AlterSourceState() override {
		// the executor destroys this state after its tasks are done and before the query ends the transaction
		if (!info.altered_entry) {
			return;
		}
		try {
			PhysicalAlterColumn::DiscardAlteredEntry(context, info);
		} catch (...) { // NOLINT: at worst the blocks of the computed column are not given back
		}
	}

	ClientContext &context;
	//! The ALTER, whose altered entry the PhysicalAlterColumn above installs once it computed the column
	AlterInfo &info;
};

unique_ptr<GlobalSourceState> PhysicalAlter::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<AlterSourceState>(context, *info);
}

SourceResultType PhysicalAlter::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	if (info->type == AlterType::ALTER_DATABASE) {
		auto &db_info = info->Cast<AlterDatabaseInfo>();
		auto &db_manager = DatabaseManager::Get(context.client);
		db_manager.Alter(context.client, db_info);
	} else {
		auto &catalog = Catalog::GetCatalog(context.client, info->GetQualifiedName().Catalog());
		info->defer_column_rewrite = defer_column_rewrite;
		catalog.Alter(context.client, *info);
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
