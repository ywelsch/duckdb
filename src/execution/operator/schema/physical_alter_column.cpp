#include "duckdb/execution/operator/schema/physical_alter_column.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_rewrite.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

//! Installs the computed column in the new table version
static void InstallColumn(ClientContext &context, DuckTableEntry &table_entry) {
	auto &storage = table_entry.GetStorage();
	storage.FinishColumnRewrite(context);
	// the catalog entry was created before the transaction-local rows moved over to its storage
	LocalStorage::Get(context, storage.db).SetTableEntry(table_entry);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class AlterColumnSinkState : public GlobalSinkState {
public:
	//! The new table version, if the ALTER left a column to compute
	optional_ptr<DuckTableEntry> table_entry;
};

unique_ptr<GlobalSinkState> PhysicalAlterColumn::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<AlterColumnSinkState>();
}

SinkResultType PhysicalAlterColumn::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalAlterColumn::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<AlterColumnSinkState>();
	// the new table version holds the column the ALTER left to us, unless it had nothing to do
	EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, table);
	auto entry = Catalog::GetEntry(context, lookup_info, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		return SinkFinalizeType::READY;
	}
	auto table_entry = entry->Cast<TableCatalogEntry>().TryGetDuckTableEntry();
	if (!table_entry) {
		return SinkFinalizeType::READY;
	}
	auto rewrite = table_entry->GetStorage().GetColumnRewrite();
	if (!rewrite) {
		return SinkFinalizeType::READY;
	}
	if (rewrite->RowGroupCount() == 0) {
		InstallColumn(context, *table_entry);
		return SinkFinalizeType::READY;
	}
	gstate.table_entry = table_entry;
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AlterColumnSourceState : public GlobalSourceState {
public:
	explicit AlterColumnSourceState(optional_ptr<DuckTableEntry> table_entry_p)
	    : table_entry(table_entry_p), next_row_group(0), rewritten_row_groups(0) {
		if (table_entry) {
			rewrite = table_entry->GetStorage().GetColumnRewrite();
			row_group_count = rewrite->RowGroupCount();
		}
	}

	idx_t MaxThreads() override {
		// a volatile expression is computed in row order, by a single thread
		return rewrite && !rewrite->IsVolatile() ? row_group_count : 1;
	}

	optional_ptr<DuckTableEntry> table_entry;
	optional_ptr<ColumnRewrite> rewrite;
	idx_t row_group_count = 0;
	atomic<idx_t> next_row_group;
	atomic<idx_t> rewritten_row_groups;
};

unique_ptr<GlobalSourceState> PhysicalAlterColumn::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<AlterColumnSourceState>(sink_state->Cast<AlterColumnSinkState>().table_entry);
}

SourceResultType PhysicalAlterColumn::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<AlterColumnSourceState>();
	auto index = state.next_row_group++;
	if (index >= state.row_group_count) {
		return SourceResultType::FINISHED;
	}
	state.rewrite->RewriteRowGroup(index);
	if (++state.rewritten_row_groups < state.row_group_count) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	// the thread that rewrote the last row group installs the column
	InstallColumn(context.client, *state.table_entry);
	return SourceResultType::FINISHED;
}

ProgressData PhysicalAlterColumn::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	auto &state = gstate.Cast<AlterColumnSourceState>();
	ProgressData progress;
	if (state.row_group_count == 0) {
		progress.SetInvalid();
		return progress;
	}
	progress.done = static_cast<double>(state.rewritten_row_groups.load());
	progress.total = static_cast<double>(state.row_group_count);
	return progress;
}

} // namespace duckdb
