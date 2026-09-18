//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/update_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/transaction/undo_buffer_allocator.hpp"
#include "duckdb/transaction/transaction_data.hpp"

namespace duckdb {
class ColumnData;
class DataTable;
class DuckTableEntry;
struct SelectionVector;
class Vector;
struct UpdateInfo;
struct UpdateNode;
struct UndoBufferAllocator;

class UpdateSegment;

//! The update segment of a column. A checkpoint that rewrites a column shares the slot with the new column, so that
//! updates through either column land in the same segment, and the segment is dropped only when every column that
//! held the slot but one is gone (a superseded column keeps reading its own, older base data)
struct UpdateSlot {
	mutex lock;
	shared_ptr<UpdateSegment> updates;
	//! The number of columns holding this slot
	idx_t column_count = 1;
};

//! The UpdateSegment holds the updated values of one column of one row group: the root UpdateInfo of a vector holds
//! the newest values, the chain behind it the previous values (undo information), newest to oldest. Values are
//! absolute, not deltas, so a checkpoint that rewrites the column can share the segment with the new column (see
//! ColumnData::CarryUpdatesToCheckpointTarget).
class UpdateSegment : public enable_shared_from_this<UpdateSegment> {
public:
	UpdateSegment(ColumnData &column_data, weak_ptr<UpdateSlot> slot);
	~UpdateSegment();

public:
	const LogicalType &GetType() const {
		return type;
	}
	//! The column indexes from the top-level column down to this column, excluding the top-level column
	const vector<column_t> &GetNestedColumnPath() const {
		return nested_column_path;
	}

	bool HasUpdates() const;
	bool HasUncommittedUpdates(idx_t vector_index);
	bool HasUpdates(idx_t vector_index) const;
	bool HasUpdates(idx_t start_row_idx, idx_t end_row_idx);
	//! Whether a committed update on this segment is not yet written to disk
	bool HasUnserializedChanges() const;
	//! Whether nothing needs the segment anymore: no version chains and no unserialized updates
	bool CanBeDropped() const;
	void MarkCommitted(transaction_t commit_id);
	void MarkCheckpointed(VisibilityBound visibility_bound);

	void FetchUpdates(TransactionData transaction, idx_t vector_index, Vector &result);
	//! Fetch the newest version of the updated values of a vector, regardless of visibility
	void FetchCommitted(idx_t vector_index, Vector &result);
	//! Fetch the updated values in [start_row, start_row + count) as visible to the given bound
	void FetchCommittedRange(idx_t start_row, idx_t count, Vector &result, VisibilityBound visibility_bound);
	void Update(TransactionData transaction, DuckTableEntry &table_entry, idx_t column_index, Vector &update,
	            row_t *ids, idx_t count, Vector &base_data, idx_t row_group_start);
	void FetchRows(TransactionData transaction, const idx_t *offsets, const SelectionVector &sel, idx_t count,
	               Vector &result, idx_t result_offset);

	void RollbackUpdate(UpdateInfo &info);
	void CleanupUpdateInternal(const StorageLockKey &lock, UpdateInfo &info);
	void CleanupUpdate(UpdateInfo &info);

	unique_ptr<BaseStatistics> GetStatistics();
	StringHeap &GetStringHeap() {
		return heap;
	}

private:
	//! The type of the column
	LogicalType type;
	//! The nested column path of the column (see GetNestedColumnPath)
	vector<column_t> nested_column_path;
	//! The buffer manager the root node allocates from
	BufferManager &buffer_manager;
	//! The highest commit id of an update on this segment that no checkpoint has written to disk yet, or 0
	atomic<transaction_t> uncheckpointed_update_commit;
	//! The number of undo entries referring to this segment (they keep it alive through CanBeDropped)
	atomic<idx_t> chain_count;
	//! The slot holding this segment
	weak_ptr<UpdateSlot> slot;
	//! The lock for the update segment
	mutable StorageLock lock;
	//! The root node (if any)
	unique_ptr<UpdateNode> root;
	//! Update statistics
	SegmentStatistics stats;
	//! Stats lock
	mutex stats_lock;
	//! Internal type size
	idx_t type_size;
	//! String heap, only used for strings
	StringHeap heap;

public:
	typedef void (*initialize_update_function_t)(UpdateInfo &base_info, Vector &base_data, UpdateInfo &update_info,
	                                             UnifiedVectorFormat &update, const SelectionVector &sel);
	typedef void (*merge_update_function_t)(UpdateInfo &base_info, Vector &base_data, UpdateInfo &update_info,
	                                        UnifiedVectorFormat &update, row_t *ids, idx_t count,
	                                        const SelectionVector &sel, idx_t row_group_start);
	typedef void (*fetch_update_function_t)(const SnapshotView &view, UpdateInfo &info, Vector &result);
	typedef void (*fetch_committed_function_t)(UpdateInfo &info, Vector &result);
	typedef void (*fetch_committed_range_function_t)(UpdateInfo &info, const SnapshotView &view, idx_t start, idx_t end,
	                                                 idx_t result_offset, Vector &result);
	typedef void (*fetch_rows_function_t)(const SnapshotView &view, UpdateInfo &info, const idx_t *offsets,
	                                      const SelectionVector &sel, idx_t fetch_offset, idx_t count,
	                                      idx_t vector_offset, Vector &result, idx_t result_offset);
	typedef void (*rollback_update_function_t)(UpdateInfo &base_info, UpdateInfo &rollback_info);
	typedef idx_t (*statistics_update_function_t)(UpdateSegment *segment, SegmentStatistics &stats,
	                                              UnifiedVectorFormat &update, idx_t count, SelectionVector &sel);
	typedef idx_t (*get_effective_updates_t)(UnifiedVectorFormat &update_format, row_t *ids, idx_t count,
	                                         SelectionVector &sel, Vector &base_data, idx_t id_offset);

private:
	initialize_update_function_t initialize_update_function;
	merge_update_function_t merge_update_function;
	fetch_update_function_t fetch_update_function;
	fetch_committed_function_t fetch_committed_function;
	fetch_committed_range_function_t fetch_committed_range;
	fetch_rows_function_t fetch_rows_function;
	rollback_update_function_t rollback_update_function;
	statistics_update_function_t statistics_update_function;
	get_effective_updates_t get_effective_updates;

private:
	UndoBufferPointer GetUpdateNode(StorageLockKey &lock, idx_t vector_idx) const;
	void InitializeUpdateInfo(idx_t vector_idx);
	void InitializeUpdateInfo(UpdateInfo &info, row_t *ids, const SelectionVector &sel, idx_t count, idx_t vector_index,
	                          idx_t vector_offset);
	void ReallocateRootInfoIfNeeded(UpdateInfo &current_info, idx_t update_count, idx_t vector_index);
	//! Drops the segment from its slot if nothing needs it anymore - the caller keeps the segment alive
	void TryDropFromSlot();
};

struct UpdateNode {
	explicit UpdateNode(BufferManager &manager);
	~UpdateNode();

	UndoBufferAllocator allocator;
	vector<UndoBufferPointer> info;
};

} // namespace duckdb
