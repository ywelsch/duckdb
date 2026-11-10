#include "duckdb/execution/operator/schema/physical_alter_column.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_rewrite.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

PhysicalAlterColumn::PhysicalAlterColumn(PhysicalPlan &physical_plan, PhysicalAlter &alter, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::ALTER, {LogicalType::BOOLEAN}, estimated_cardinality),
      info(*alter.info) {
	children.push_back(alter);
}

//! The storage of the altered entry, whose column this operator computes
static DataTable &GetAlteredStorage(AlterInfo &info) {
	return info.altered_entry->Cast<DuckTableEntry>().GetStorage();
}

//! Installs the altered entry once its column is computed
static void InstallColumn(ClientContext &context, AlterInfo &info) {
	GetAlteredStorage(info).FinishColumnRewrite(context);
	// altering again installs the altered entry in the catalog
	Catalog::GetCatalog(context, info.GetQualifiedName().Catalog()).Alter(context, info);
}

void PhysicalAlterColumn::DiscardAlteredEntry(ClientContext &context, AlterInfo &info) {
	auto altered_entry = std::move(info.altered_entry);
	auto &storage = altered_entry->Cast<DuckTableEntry>().GetStorage();
	if (LocalStorage::Get(context, storage.db).GetStorage(storage)) {
		// the rows the transaction appended already moved over to the discarded table version
		ValidChecker::Invalidate(MetaTransaction::Get(context), "an ALTER TABLE failed to install the altered table");
	}
	storage.AbortColumnRewrite();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalAlterColumn::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalAlterColumn::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	if (!info.altered_entry) {
		// the ALTER had nothing to alter, e.g. ADD COLUMN IF NOT EXISTS of a column that exists
		return SinkFinalizeType::READY;
	}
	auto rewrite = GetAlteredStorage(info).GetColumnRewrite();
	// every table version that an ALTER with defer_column_rewrite builds has a column to compute
	D_ASSERT(rewrite);
	if (rewrite->RowGroupCount() == 0) {
		InstallColumn(context, info);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AlterColumnSourceState : public GlobalSourceState {
public:
	explicit AlterColumnSourceState(AlterInfo &info) : next_row_group(0), rewritten_row_groups(0) {
		if (info.altered_entry) {
			rewrite = GetAlteredStorage(info).GetColumnRewrite();
			row_group_count = rewrite->RowGroupCount();
		}
	}

	idx_t MaxThreads() override {
		// a volatile expression is computed in row order, by a single thread
		return rewrite && !rewrite->IsVolatile() ? row_group_count : 1;
	}

	optional_ptr<ColumnRewrite> rewrite;
	idx_t row_group_count = 0;
	atomic<idx_t> next_row_group;
	atomic<idx_t> rewritten_row_groups;
};

unique_ptr<GlobalSourceState> PhysicalAlterColumn::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<AlterColumnSourceState>(info);
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
	// the thread that rewrote the last row group installs the altered entry
	InstallColumn(context.client, info);
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
