//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/hidden_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

//! A column that a table version hid and no snapshot reads any more: it stores nothing and reads as NULL
class HiddenColumnData : public ColumnData {
public:
	HiddenColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type,
	                 idx_t count);

public:
	FilterPropagateResult CheckZonemap(ColumnScanState &state, const ScanFilter &filter,
	                                   optional_idx &checked_end) override;
	FilterPropagateResult CheckZonemapAt(ColumnScanState &state, const ScanFilter &filter, idx_t row,
	                                     optional_idx &checked_end) override;
	idx_t GetMaxEntry() override;
	bool HasAnyChanges() const override;

	void InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t start_row,
	                        idx_t end_row) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;
	idx_t Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	           idx_t scan_count) override;
	void ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result) override;
	idx_t ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset = 0) override;
	void Filter(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	            SelectionVector &sel, idx_t &count, const TableFilter &filter, TableFilterState &filter_state) override;
	void Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	            SelectionVector &sel, idx_t count) override;
	void Skip(ColumnScanState &state, idx_t count = STANDARD_VECTOR_SIZE) override;

	void InitializeAppend(ColumnAppendState &state) override;
	void Append(ColumnAppendState &state, const Vector &vector, idx_t count) override;
	void AppendData(ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t count) override;
	void FinalizeAppend(ColumnDataFinalizeAppendState &finalize_state, ColumnAppendState &state) override;
	void RevertAppend(row_t new_count) override;

	void FetchRows(TransactionData transaction, ColumnFetchState &state, const StorageIndex &storage_index,
	               const idx_t *offsets, const SelectionVector &sel, idx_t count, Vector &result,
	               idx_t result_offset) override;

	void Update(TransactionData transaction, DuckTableEntry &table_entry, idx_t column_index, Vector &update_vector,
	            row_t *row_ids, idx_t update_count, idx_t row_group_start) override;
	void UpdateColumn(TransactionData transaction, DuckTableEntry &table_entry, const vector<column_t> &column_path,
	                  Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth,
	                  idx_t row_group_start) override;
	unique_ptr<BaseStatistics> GetUpdateStatistics() override;

	void VisitBlockIds(BlockIdVisitor &visitor) const override;

	unique_ptr<ColumnCheckpointState> CreateCheckpointState(const RowGroup &row_group, Context &context,
	                                                        PartialBlockManager &partial_block_manager) override;
	unique_ptr<ColumnCheckpointState> Checkpoint(const RowGroup &row_group, ColumnCheckpointInfo &info,
	                                             const BaseStatistics &old_stats) override;
	void CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t count,
	                    Vector &scan_vector) const override;
	bool IsPersistent() override;
	PersistentColumnData Serialize() override;
	void InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) override;

	void GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
	                          vector<ColumnSegmentInfo> &result, const ColumnSegmentInfoScanOptions &options) override;
	void Verify(RowGroup &parent) override;
	bool IsHiddenPlaceholder() const override {
		return true;
	}

private:
	void ReadNulls(Vector &result, idx_t count);
};

} // namespace duckdb
