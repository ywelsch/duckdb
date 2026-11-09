#include "duckdb/execution/operator/schema/physical_alter_column.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_rewrite.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

//! Installs the computed column in the new table version
static void InstallColumn(ClientContext &context, DuckTableEntry &table_entry) {
	auto &storage = table_entry.GetStorage();
	storage.FinishColumnRewrite(context);
	// the catalog entry was created before the transaction-local rows moved over to its storage
	LocalStorage::Get(context, storage.db).SetTableEntry(table_entry);
}

//! Gives up on a column that could not be computed, so the transaction cannot commit the table version without it
static void AbortColumn(ClientContext &context, DuckTableEntry &table_entry) {
	ValidChecker::Invalidate(MetaTransaction::Get(context), "an ALTER TABLE failed to compute its column");
	table_entry.GetStorage().AbortColumnRewrite();
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
	// look the table up as Catalog::Alter did: its new version holds the column the ALTER left to us, if any
	auto &catalog = Catalog::GetCatalog(context, table.Catalog());
	CatalogEntryRetriever retriever(context);
	auto lookup =
	    catalog.LookupEntry(retriever, EntryLookupInfo(CatalogType::TABLE_ENTRY, table), OnEntryNotFound::RETURN_NULL);
	if (!lookup.Found() || lookup.entry->type != CatalogType::TABLE_ENTRY) {
		return SinkFinalizeType::READY;
	}
	auto table_entry = lookup.entry->Cast<TableCatalogEntry>().TryGetDuckTableEntry();
	if (!table_entry) {
		return SinkFinalizeType::READY;
	}
	auto rewrite = table_entry->GetStorage().GetColumnRewrite();
	if (!rewrite) {
		return SinkFinalizeType::READY;
	}
	if (rewrite->RowGroupCount() == 0) {
		try {
			InstallColumn(context, *table_entry);
		} catch (...) {
			AbortColumn(context, *table_entry);
			throw;
		}
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
	AlterColumnSourceState(ClientContext &context, optional_ptr<DuckTableEntry> table_entry_p)
	    : context(context), table_entry(table_entry_p), next_row_group(0), rewritten_row_groups(0) {
		if (table_entry) {
			rewrite = table_entry->GetStorage().GetColumnRewrite();
			row_group_count = rewrite->RowGroupCount();
		}
	}

	~AlterColumnSourceState() override {
		// the tasks are done when the executor destroys this state, before the query ends the transaction
		if (!table_entry || !table_entry->GetStorage().GetColumnRewrite()) {
			return;
		}
		try {
			AbortColumn(context, *table_entry);
		} catch (...) { // NOLINT: the transaction rollback still gives back the blocks
		}
	}

	idx_t MaxThreads() override {
		// a volatile expression is computed in row order, by a single thread
		return rewrite && !rewrite->IsVolatile() ? row_group_count : 1;
	}

	ClientContext &context;
	optional_ptr<DuckTableEntry> table_entry;
	optional_ptr<ColumnRewrite> rewrite;
	idx_t row_group_count = 0;
	atomic<idx_t> next_row_group;
	atomic<idx_t> rewritten_row_groups;
};

unique_ptr<GlobalSourceState> PhysicalAlterColumn::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<AlterColumnSourceState>(context, sink_state->Cast<AlterColumnSinkState>().table_entry);
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
