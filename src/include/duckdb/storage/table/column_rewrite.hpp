//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_rewrite.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/storage/table/segment_tree.hpp"

#include <functional>

namespace duckdb {
class BoundConstraint;
class ClientContext;
class ColumnStatistics;
class DataTable;
class Expression;
class ExpressionExecutor;
class OptimisticDataWriter;
class RowGroup;
class RowGroupCollection;
class RowGroupSegmentTree;

//! Builds the collection of an ADD COLUMN or ALTER TYPE row group by row group. With a writer, the new column of every
//! row group that is on disk is written as soon as it is filled. Different row groups can be altered at the same time.
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

//! The column that an ADD COLUMN or ALTER TYPE computes for a new table version, both for the rows of the table and for
//! the rows the transaction appended to it. The table version computes it right away, unless the ALTER leaves it to
//! its caller, which computes it on its own tasks and then calls DataTable::FinishColumnRewrite.
class ColumnRewrite {
public:
	explicit ColumnRewrite(DataTable &parent);
	~ColumnRewrite();

	//! The number of row groups to rewrite
	idx_t RowGroupCount() const;
	//! Whether the row groups have to be rewritten one after the other in row order, as the expression is volatile
	bool IsVolatile() const;
	//! Rewrites the row group at the given index, different row groups can be rewritten at the same time
	void RewriteRowGroup(idx_t index);

	//! The table version the column is rewritten from
	DataTable &parent;
	//! For ALTER TYPE, the column of the transaction-local rows that the new column replaces
	optional_idx replaced_column;
	//! A constraint to verify on the rewritten column
	unique_ptr<BoundConstraint> constraint_to_verify;
	unique_ptr<RowGroupCollectionAlter> row_groups;
	//! The rows the transaction appended to the table, if any. These have their own writer, as their blocks are
	//! given back on their own when the transaction deletes rows of them.
	unique_ptr<RowGroupCollectionAlter> local_row_groups;
};

} // namespace duckdb
