#include "duckdb/execution/operator/schema/physical_alter_add_column.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/column_statistics.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_rewrite.hpp"

namespace duckdb {

PhysicalAlterAddColumn::PhysicalAlterAddColumn(PhysicalPlan &physical_plan, unique_ptr<AlterInfo> info_p,
                                               bool is_volatile, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::ALTER, {LogicalType::BOOLEAN}, estimated_cardinality),
      info(std::move(info_p)), is_volatile(is_volatile) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
//! The new column of one row group, filled from the chunks of the scan in row order
struct RowGroupColumnState {
	mutex lock;
	shared_ptr<ColumnData> column;
	ColumnAppendState append_state;
	shared_ptr<ColumnStatistics> stats;
	//! The offset in the row group of the next row to append
	idx_t next_row = 0;
	//! Chunks that arrived ahead of the rows before them, by their offset
	map<idx_t, unique_ptr<DataChunk>> early_chunks;
	bool finished = false;
};

class AlterAddColumnSinkState : public GlobalSinkState {
public:
	AlterAddColumnSinkState(ClientContext &context, DataTable &table, const ColumnDefinition &new_column)
	    : computed(make_shared_ptr<ComputedColumn>(context, table, new_column)), type(new_column.GetType()) {
		for (idx_t index = 0; index < computed->RowGroupCount(); index++) {
			row_groups.push_back(make_uniq<RowGroupColumnState>());
		}
	}

	shared_ptr<ComputedColumn> computed;
	LogicalType type;
	vector<unique_ptr<RowGroupColumnState>> row_groups;
};

unique_ptr<GlobalSinkState> PhysicalAlterAddColumn::GetGlobalSinkState(ClientContext &context) const {
	auto &table_name = info->GetQualifiedName();
	auto &catalog = Catalog::GetCatalog(context, table_name.Catalog());
	CatalogEntryRetriever retriever(context);
	auto lookup = catalog.LookupEntry(retriever, EntryLookupInfo(CatalogType::TABLE_ENTRY, table_name),
	                                  OnEntryNotFound::THROW_EXCEPTION);
	auto &table = lookup.entry->Cast<DuckTableEntry>();
	return make_uniq<AlterAddColumnSinkState>(context, table.GetStorage(), info->Cast<AddColumnInfo>().new_column);
}

static void AppendRows(RowGroupColumnState &state, Vector &values, idx_t count) {
	Vector hashes(LogicalType::HASH);
	state.stats->UpdateDistinctStatistics(values, count, hashes);
	state.column->Append(state.append_state, values, count);
	state.next_row += count;
}

static TransactionException TableChanged(const AlterInfo &info) {
	return TransactionException("Catalog write-write conflict on alter with \"%s\": the table changed while the new "
	                            "column was computed",
	                            info.GetQualifiedName().Name());
}

SinkResultType PhysicalAlterAddColumn::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<AlterAddColumnSinkState>();
	auto &computed = *gstate.computed;
	auto count = chunk.size();
	if (count == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	// a chunk of the scan holds consecutive rows of one row group
	UnifiedVectorFormat row_ids;
	chunk.data[1].ToUnifiedFormat(count, row_ids);
	auto ids = UnifiedVectorFormat::GetData<row_t>(row_ids);
	auto first_row = NumericCast<idx_t>(ids[row_ids.sel->get_index(0)]);
	auto last_row = NumericCast<idx_t>(ids[row_ids.sel->get_index(count - 1)]);
	auto index = computed.FindRowGroup(first_row);
	if (!index.IsValid() || last_row != first_row + count - 1 ||
	    last_row >= computed.RowGroupStart(index.GetIndex()) + computed.RowGroupRowCount(index.GetIndex())) {
		// rows were appended since the operator took the row groups of the table
		throw TableChanged(*info);
	}
	auto row_group_index = index.GetIndex();
	auto offset = first_row - computed.RowGroupStart(row_group_index);

	auto &state = *gstate.row_groups[row_group_index];
	lock_guard<mutex> guard(state.lock);
	if (!state.column) {
		state.column = computed.CreateColumn();
		state.column->InitializeAppend(state.append_state);
		state.stats = ColumnStatistics::CreateEmptyStats(gstate.type);
	}
	if (offset != state.next_row) {
		// a row group can be scanned in several parts (e.g. with verify_parallelism), which may arrive out of order
		auto early_chunk = make_uniq<DataChunk>();
		early_chunk->Initialize(Allocator::DefaultAllocator(), {gstate.type}, count);
		VectorOperations::Copy(chunk.data[0], early_chunk->data[0], count, 0, 0);
		early_chunk->SetCardinality(count);
		state.early_chunks[offset] = std::move(early_chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}
	AppendRows(state, chunk.data[0], count);
	while (!state.early_chunks.empty() && state.early_chunks.begin()->first == state.next_row) {
		auto early_chunk = std::move(state.early_chunks.begin()->second);
		state.early_chunks.erase(state.early_chunks.begin());
		AppendRows(state, early_chunk->data[0], early_chunk->size());
	}
	if (state.next_row == computed.RowGroupRowCount(row_group_index)) {
		state.column->FinalizeAppend(nullptr, state.append_state);
		computed.SetColumn(row_group_index, std::move(state.column), std::move(state.stats));
		state.finished = true;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalAlterAddColumn::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<AlterAddColumnSinkState>();
	auto &computed = *gstate.computed;
	for (idx_t index = 0; index < gstate.row_groups.size(); index++) {
		auto &state = *gstate.row_groups[index];
		if (!state.finished && computed.RowGroupRowCount(index) == 0) {
			computed.SetColumn(index, computed.CreateColumn(), ColumnStatistics::CreateEmptyStats(gstate.type));
			state.finished = true;
		}
	}
	// the ALTER installs the computed column, and a column it did not install gives back its blocks
	auto &add_info = info->Cast<AddColumnInfo>();
	add_info.computed_column = std::move(gstate.computed);
	auto &catalog = Catalog::GetCatalog(context, info->GetQualifiedName().Catalog());
	try {
		catalog.Alter(context, *info);
	} catch (...) {
		add_info.computed_column.reset();
		throw;
	}
	add_info.computed_column.reset();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAlterAddColumn::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

} // namespace duckdb
