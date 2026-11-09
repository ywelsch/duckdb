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
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/storage/table/segment_tree.hpp"

#include <functional>

namespace duckdb {
class BoundConstraint;
class ColumnStatistics;
class DataTable;
class OptimisticDataWriter;
class RowGroup;
class RowGroupCollection;
class RowGroupSegmentTree;
enum class DataTableVersion;

//! Builds the collection of an ADD COLUMN or ALTER TYPE row group by row group, and writes the new column of every row
//! group that is on disk as soon as it is filled. Different row groups can be altered at the same time.
class RowGroupCollectionAlter {
public:
	using AlterRowGroupFunction = std::function<unique_ptr<RowGroup>(SegmentNode<RowGroup> &, ColumnStatistics &)>;

	RowGroupCollectionAlter(RowGroupCollection &source, shared_ptr<RowGroupCollection> result, idx_t column_idx,
	                        bool is_volatile, optional_ptr<OptimisticDataWriter> writer, AlterRowGroupFunction alter);
	~RowGroupCollectionAlter();

	idx_t RowGroupCount() const {
		return nodes.size();
	}
	bool IsVolatile() const {
		return is_volatile;
	}
	//! Alters the row group at the given index
	void AlterRowGroup(idx_t index);
	//! Assembles the new collection once every row group is altered
	shared_ptr<RowGroupCollection> Finish();

private:
	//! Gives back the blocks the new column was written to
	void Rollback();

private:
	//! The row groups of the source collection, which the nodes point into
	shared_ptr<RowGroupSegmentTree> source_row_groups;
	vector<reference<SegmentNode<RowGroup>>> nodes;
	shared_ptr<RowGroupCollection> result;
	idx_t column_idx;
	bool is_volatile;
	optional_ptr<OptimisticDataWriter> writer;
	AlterRowGroupFunction alter;
	vector<unique_ptr<RowGroup>> new_row_groups;
	vector<shared_ptr<ColumnStatistics>> new_stats;
	bool finished = false;
};

//! The column that an ADD COLUMN or ALTER TYPE computes for a new table version, both for the rows of the table and for
//! the rows the transaction appended to it. The table version runs it right away, unless the ALTER leaves it to its
//! caller, which runs it on its own tasks and then calls DataTable::FinishColumnRewrite.
class ColumnRewrite {
public:
	ColumnRewrite(DataTable &parent, idx_t column_idx, DataTableVersion previous_version);
	~ColumnRewrite();

	//! The number of row groups to rewrite
	idx_t RowGroupCount() const;
	//! Whether the row groups have to be rewritten one after the other in row order, as the expression is volatile
	bool IsVolatile() const;
	//! Rewrites the row group at the given index, different row groups can be rewritten at the same time
	void RewriteRowGroup(idx_t index);

	//! The table version the column is rewritten from
	DataTable &parent;
	//! The physical index of the rewritten column
	idx_t column_idx;
	//! The version of the parent before the ALTER marked it altered
	DataTableVersion previous_version;
	//! For ALTER TYPE, the column of the transaction-local rows that the new column replaces
	optional_idx replaced_column;
	//! A constraint to verify on the rewritten column
	unique_ptr<BoundConstraint> constraint_to_verify;
	unique_ptr<OptimisticDataWriter> writer;
	unique_ptr<OptimisticDataWriter> local_writer;
	unique_ptr<RowGroupCollectionAlter> row_groups;
	//! The rows the transaction appended to the table, if any
	unique_ptr<RowGroupCollectionAlter> local_row_groups;
};

} // namespace duckdb
