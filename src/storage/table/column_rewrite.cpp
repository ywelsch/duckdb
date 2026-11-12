#include "duckdb/storage/table/column_rewrite.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/statistics/column_statistics.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/transaction/commit_state.hpp"

namespace duckdb {

RowGroupCollectionAlter::RowGroupCollectionAlter(ClientContext &context, RowGroupCollection &source,
                                                 shared_ptr<RowGroupCollection> result_p, idx_t column_idx,
                                                 unique_ptr<Expression> expression_p,
                                                 unique_ptr<OptimisticDataWriter> writer_p,
                                                 AlterRowGroupFunction alter_p)
    : context(context), source_row_groups(source.GetRowGroups()), result(std::move(result_p)), column_idx(column_idx),
      expression(std::move(expression_p)), writer(std::move(writer_p)), alter(std::move(alter_p)) {
	if (writer && !writer->PrepareWriteColumn(source)) {
		writer.reset();
	}
	for (auto &node : source_row_groups->SegmentNodes()) {
		nodes.push_back(node);
	}
	new_row_groups.resize(nodes.size());
	new_stats.resize(nodes.size());
}

RowGroupCollectionAlter::~RowGroupCollectionAlter() {
	if (!result) {
		// Finish handed out the new collection
		return;
	}
	try {
		Rollback();
	} catch (...) { // NOLINT: the blocks stay allocated until the next checkpoint
	}
}

bool RowGroupCollectionAlter::IsVolatile() const {
	return expression->IsVolatile();
}

void RowGroupCollectionAlter::AlterRowGroup(idx_t index) {
	auto &node = nodes[index].get();
	ExpressionExecutor executor(context, *expression);
	new_stats[index] = ColumnStatistics::CreateEmptyStats(result->GetTypes()[column_idx]);
	new_row_groups[index] = alter(node, executor, *new_stats[index]);
	// a row group that is not on disk yet is written as a whole later, together with its new column
	if (writer && node.GetNode().IsPersistent()) {
		writer->WriteColumn(*new_row_groups[index], column_idx);
	}
}

shared_ptr<RowGroupCollection> RowGroupCollectionAlter::Finish() {
	{
		auto lock = result->stats.GetLock();
		auto &column_stats = result->stats.GetStats(*lock, column_idx);
		auto result_row_groups = result->GetRowGroups();
		for (idx_t index = 0; index < nodes.size(); index++) {
			new_row_groups[index]->MergeIntoStatistics(column_idx, column_stats.Statistics());
			column_stats.Merge(*new_stats[index]);
			result_row_groups->AppendSegment(std::move(new_row_groups[index]), nodes[index].get().GetRowStart());
		}
	}
	if (writer) {
		writer->FinalFlush();
	}
	return std::move(result);
}

void RowGroupCollectionAlter::Rollback() {
	if (!writer) {
		return;
	}
	// release the blocks the column was already written to
	writer->Rollback();
	CommitDropState drop_state(&result->GetBlockManager());
	for (auto &row_group : new_row_groups) {
		if (row_group) {
			row_group->CommitDropColumn(column_idx, drop_state);
		}
	}
	result->CommitDropColumn(column_idx, drop_state);
	drop_state.FinalizeCommit();
}

ComputedColumn::ComputedColumn(ClientContext &context, DataTable &table, const ColumnDefinition &new_column)
    : context(context), source_row_groups(table.GetRowGroupCollection()->GetRowGroups()),
      total_rows(table.GetRowGroupCollection()->GetTotalRows()), type(new_column.GetType()),
      compression(new_column.CompressionType()) {
	auto &source = *table.GetRowGroupCollection();
	column_idx = source.GetTypes().size();
	result = source.CreateCollectionWithColumn(type);
	writer = make_uniq<OptimisticDataWriter>(context, table);
	if (!writer->PrepareWriteColumn(source)) {
		writer.reset();
	}
	for (auto &node : source_row_groups->SegmentNodes()) {
		nodes.push_back(node);
	}
	columns.resize(nodes.size());
	column_stats.resize(nodes.size());
}

ComputedColumn::~ComputedColumn() {
	if (!result) {
		// Install handed out the new collection
		return;
	}
	try {
		Rollback();
	} catch (...) { // NOLINT: the blocks stay allocated until the next checkpoint
	}
}

optional_idx ComputedColumn::FindRowGroup(idx_t row) const {
	idx_t lower = 0;
	idx_t upper = nodes.size();
	while (lower < upper) {
		auto middle = lower + (upper - lower) / 2;
		auto &node = nodes[middle].get();
		if (row < node.GetRowStart()) {
			upper = middle;
		} else if (row >= node.GetRowStart() + node.GetNode().count) {
			lower = middle + 1;
		} else {
			return middle;
		}
	}
	return optional_idx();
}

idx_t ComputedColumn::RowGroupStart(idx_t index) const {
	return nodes[index].get().GetRowStart();
}

idx_t ComputedColumn::RowGroupRowCount(idx_t index) const {
	return nodes[index].get().GetNode().count;
}

shared_ptr<ColumnData> ComputedColumn::CreateColumn() const {
	return ColumnData::CreateColumn(result->GetBlockManager(), result->GetTableInfo(), column_idx, type);
}

void ComputedColumn::SetColumn(idx_t index, shared_ptr<ColumnData> column, shared_ptr<ColumnStatistics> stats) {
	auto &row_group = nodes[index].get().GetNode();
	if (writer && row_group.IsPersistent()) {
		// a scratch copy of the row group writes the column, Install attaches the written column to a fresh copy
		auto scratch = row_group.AddColumn(*result, std::move(column));
		column = writer->WriteColumn(*scratch, column_idx, compression);
	}
	columns[index] = std::move(column);
	column_stats[index] = std::move(stats);
}

shared_ptr<RowGroupCollection> ComputedColumn::Install(RowGroupCollection &source) {
	bool complete = true;
	for (auto &column : columns) {
		complete = complete && column;
	}
	if (!complete || source.GetRowGroups() != source_row_groups || source.GetTotalRows() != total_rows) {
		// rows were appended or reverted while the column was computed
		throw TransactionException("Catalog write-write conflict on alter with \"%s\": the table changed while the "
		                           "new column was computed",
		                           source.GetTableInfo().GetTableName());
	}
	{
		auto lock = result->stats.GetLock();
		auto &stats = result->stats.GetStats(*lock, column_idx);
		auto result_row_groups = result->GetRowGroups();
		for (idx_t index = 0; index < nodes.size(); index++) {
			auto &node = nodes[index].get();
			auto row_group = node.GetNode().AddColumn(*result, columns[index]);
			row_group->MergeIntoStatistics(column_idx, stats.Statistics());
			stats.Merge(*column_stats[index]);
			result_row_groups->AppendSegment(std::move(row_group), node.GetRowStart());
		}
	}
	if (writer) {
		writer->FinalFlush();
	}
	return std::move(result);
}

void ComputedColumn::Rollback() {
	if (!writer) {
		return;
	}
	// release the blocks the column was already written to
	writer->Rollback();
	CommitDropState drop_state(&result->GetBlockManager());
	for (idx_t index = 0; index < nodes.size(); index++) {
		if (columns[index]) {
			nodes[index].get().GetNode().AddColumn(*result, columns[index])->CommitDropColumn(column_idx, drop_state);
		}
	}
	drop_state.FinalizeCommit();
}

ColumnRewrite::ColumnRewrite(DataTable &parent, DataTableVersion previous_version, unique_ptr<StorageLockKey> read_lock)
    : parent(parent), previous_version(previous_version), read_lock(std::move(read_lock)) {
}

ColumnRewrite::~ColumnRewrite() {
}

idx_t ColumnRewrite::RowGroupCount() const {
	return row_groups->RowGroupCount() + (local_row_groups ? local_row_groups->RowGroupCount() : 0);
}

bool ColumnRewrite::IsVolatile() const {
	return row_groups->IsVolatile();
}

void ColumnRewrite::RewriteRowGroup(idx_t index) {
	auto row_group_count = row_groups->RowGroupCount();
	// indices past the table's row groups are the transaction-local rows
	if (index < row_group_count) {
		row_groups->AlterRowGroup(index);
	} else {
		local_row_groups->AlterRowGroup(index - row_group_count);
	}
}

} // namespace duckdb
