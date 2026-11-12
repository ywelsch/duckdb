//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_rewrite.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/table/segment_tree.hpp"

#include <functional>

namespace duckdb {
class BoundConstraint;
class ClientContext;
class ColumnData;
class ColumnDefinition;
class ColumnStatistics;
class DataTable;
class Expression;
class ExpressionExecutor;
class OptimisticDataWriter;
class RowGroup;
class RowGroupCollection;
class RowGroupSegmentTree;
class StorageLockKey;
enum class DataTableVersion;

//! Builds the collection of an ADD COLUMN or ALTER TYPE row group by row group, from several threads at once
class RowGroupCollectionAlter {
public:
	using AlterRowGroupFunction =
	    std::function<unique_ptr<RowGroup>(SegmentNode<RowGroup> &, ExpressionExecutor &, ColumnStatistics &)>;

	RowGroupCollectionAlter(ClientContext &context, RowGroupCollection &source, shared_ptr<RowGroupCollection> result,
	                        idx_t column_idx, unique_ptr<Expression> expression,
	                        unique_ptr<OptimisticDataWriter> writer, AlterRowGroupFunction alter);
	~RowGroupCollectionAlter();

	idx_t RowGroupCount() const {
		return nodes.size();
	}
	idx_t ColumnIndex() const {
		return column_idx;
	}
	//! Whether the row groups have to be altered one after the other in row order, as the expression is volatile
	bool IsVolatile() const;
	//! Alters the row group at the given index
	void AlterRowGroup(idx_t index);
	//! Assembles the new collection once every row group is altered
	shared_ptr<RowGroupCollection> Finish();

private:
	//! Gives back the blocks the new column was written to
	void Rollback();

private:
	ClientContext &context;
	//! The row groups of the source collection, which the nodes point into
	shared_ptr<RowGroupSegmentTree> source_row_groups;
	vector<reference<SegmentNode<RowGroup>>> nodes;
	//! The new collection, until Finish hands it out
	shared_ptr<RowGroupCollection> result;
	idx_t column_idx;
	unique_ptr<Expression> expression;
	unique_ptr<OptimisticDataWriter> writer;
	AlterRowGroupFunction alter;
	vector<unique_ptr<RowGroup>> new_row_groups;
	vector<shared_ptr<ColumnStatistics>> new_stats;
};

//! The column of an ADD COLUMN that a scan of the table computed, as one ColumnData per row group of the table
class ComputedColumn {
public:
	ComputedColumn(ClientContext &context, DataTable &table, const ColumnDefinition &new_column);
	~ComputedColumn();

	idx_t RowGroupCount() const {
		return nodes.size();
	}
	idx_t ColumnIndex() const {
		return column_idx;
	}
	//! The index of the row group that holds the given row, if it is one of the computed row groups
	optional_idx FindRowGroup(idx_t row) const;
	idx_t RowGroupStart(idx_t index) const;
	idx_t RowGroupRowCount(idx_t index) const;
	//! An empty column to fill with the values of a row group
	shared_ptr<ColumnData> CreateColumn() const;
	//! Hands over the filled column of a row group, which is written to disk if the row group is on disk
	void SetColumn(idx_t index, shared_ptr<ColumnData> column, shared_ptr<ColumnStatistics> stats);
	//! Builds the collection with the new column on the row groups of source, which must be the ones computed for
	shared_ptr<RowGroupCollection> Install(RowGroupCollection &source);

private:
	//! Gives back the blocks the column was written to
	void Rollback();

private:
	ClientContext &context;
	//! The row groups of the table as the scan saw them
	shared_ptr<RowGroupSegmentTree> source_row_groups;
	vector<reference<SegmentNode<RowGroup>>> nodes;
	idx_t total_rows;
	//! The new collection, until Install hands it out
	shared_ptr<RowGroupCollection> result;
	idx_t column_idx;
	LogicalType type;
	CompressionType compression;
	unique_ptr<OptimisticDataWriter> writer;
	vector<shared_ptr<ColumnData>> columns;
	vector<shared_ptr<ColumnStatistics>> column_stats;
};

//! The column an ADD COLUMN or ALTER TYPE computes for a new table version, see DataTable::GetColumnRewrite
class ColumnRewrite {
public:
	ColumnRewrite(DataTable &parent, DataTableVersion previous_version, unique_ptr<StorageLockKey> read_lock);
	~ColumnRewrite();

	//! The number of row groups to rewrite
	idx_t RowGroupCount() const;
	//! Whether the row groups have to be rewritten one after the other in row order, as the expression is volatile
	bool IsVolatile() const;
	//! Rewrites the row group at the given index, different row groups can be rewritten at the same time
	void RewriteRowGroup(idx_t index);

	//! The table version the column is rewritten from
	DataTable &parent;
	//! The version of the parent before the ALTER marked it altered
	DataTableVersion previous_version;
	//! Keeps the parent from reverting appends while its row groups are read
	unique_ptr<StorageLockKey> read_lock;
	//! For ALTER TYPE, the column of the transaction-local rows that the new column replaces
	optional_idx replaced_column;
	//! A constraint to verify on the rewritten column
	unique_ptr<BoundConstraint> constraint_to_verify;
	unique_ptr<RowGroupCollectionAlter> row_groups;
	//! The rows the transaction appended, if any, with a writer of their own, as their blocks can be freed alone
	unique_ptr<RowGroupCollectionAlter> local_row_groups;
};

} // namespace duckdb
