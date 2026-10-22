//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/optimistic_data_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/common/set.hpp"

namespace duckdb {
class PartialBlockManager;

struct OptimisticWriteCollection {
	~OptimisticWriteCollection();

	shared_ptr<RowGroupCollection> collection;
	set<idx_t> unflushed_row_groups;
	idx_t unflushed_data_size = 0;
	idx_t prev_allocated_size = 0;
	idx_t complete_row_groups = 0;
	vector<unique_ptr<PartialBlockManager>> partial_block_managers;

	void MergeStorage(OptimisticWriteCollection &collection);
	//! Replace the collection with the one an ALTER created from it
	void ReplaceCollection(shared_ptr<RowGroupCollection> new_collection);
	//! Whether all row groups of the collection have been written to disk
	bool IsFullyFlushed() const;
};

enum class OptimisticWritePartialManagers { PER_COLUMN, GLOBAL };

class OptimisticDataWriter {
public:
	OptimisticDataWriter(ClientContext &context, DataTable &table);
	OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent);
	~OptimisticDataWriter();

	//! Creates a collection to write to
	unique_ptr<OptimisticWriteCollection>
	CreateCollection(DataTable &storage, const vector<LogicalType> &insert_types,
	                 OptimisticWritePartialManagers type = OptimisticWritePartialManagers::PER_COLUMN);
	//! Write a new row group to disk (if possible)
	void WriteNewRowGroup(OptimisticWriteCollection &row_groups);
	//! Write any unflushed row groups of a collection to disk
	void WriteUnflushedRowGroups(OptimisticWriteCollection &row_groups);
	//! Write a single column of a row group to disk (if possible)
	void WriteColumn(RowGroup &row_group, idx_t column_idx);
	//! Final flush of the optimistic writer - fully flushes the partial block manager
	void FinalFlush();
	//! Merge the partially written blocks from one optimistic writer into another
	void Merge(OptimisticDataWriter &other);
	void Merge(unique_ptr<PartialBlockManager> &other_manager);
	//! Rollback
	void Rollback();

	//! Whether this writer can write to disk at all (not temporary / in-memory / read-only)
	bool CanWriteToDisk() const;

	//! Return the client context.
	ClientContext &GetClientContext() {
		return context;
	}

private:
	//! Prepare a write to disk
	bool PrepareWrite();
	//! Flushes a specific row group to disk
	void FlushToDisk(OptimisticWriteCollection &collection, const vector<const_reference<RowGroup>> &row_groups,
	                 const vector<int64_t> &segment_indexes);
	//! The compression type of each column of the table
	vector<CompressionType> GetCompressionTypes() const;

private:
	//! The client context in which we're writing the data.
	ClientContext &context;
	//! The table.
	DataTable &table;
	//! The partial block manager, if any.
	unique_ptr<PartialBlockManager> partial_manager;
	//! The compression types of the table's columns, used for writing single columns
	vector<CompressionType> column_compression_types;
};

} // namespace duckdb
