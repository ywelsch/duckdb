#include "duckdb/storage/table/hidden_column_data.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

HiddenColumnData::HiddenColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                   LogicalType type, idx_t count_p)
    : ColumnData(block_manager, info, column_index, std::move(type), ColumnDataType::MAIN_TABLE, nullptr) {
	count = count_p;
	SetBlocksReleased();
}

void HiddenColumnData::ReadNulls(Vector &result, idx_t read_count) {
	ConstantVector::SetNull(result, count_t(read_count));
}

FilterPropagateResult HiddenColumnData::CheckZonemap(ColumnScanState &state, const ScanFilter &filter,
                                                     optional_idx &checked_end) {
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

FilterPropagateResult HiddenColumnData::CheckZonemapAt(ColumnScanState &state, const ScanFilter &filter, idx_t row,
                                                       optional_idx &checked_end) {
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

idx_t HiddenColumnData::GetMaxEntry() {
	return count;
}

bool HiddenColumnData::HasAnyChanges() const {
	return false;
}

void HiddenColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t start_row,
                                          idx_t end_row) {
}

void HiddenColumnData::InitializeScan(ColumnScanState &state) {
	InitializeScanWithOffset(state, 0);
}

void HiddenColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = nullptr;
	state.segment_tree = nullptr;
	state.offset_in_column = row_idx;
	state.internal_index = 0;
	state.initialized = true;
	state.scan_state.reset();
	state.last_offset = 0;
}

idx_t HiddenColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                             idx_t scan_count) {
	ReadNulls(result, scan_count);
	state.offset_in_column += scan_count;
	return scan_count;
}

void HiddenColumnData::ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t scan_count,
                                          Vector &result) {
	ReadNulls(result, scan_count);
}

idx_t HiddenColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t scan_count, idx_t result_offset) {
	if (result_offset == 0) {
		ReadNulls(result, scan_count);
	} else {
		for (idx_t i = 0; i < scan_count; i++) {
			FlatVector::SetNull(result, result_offset + i, true);
		}
	}
	state.offset_in_column += scan_count;
	return scan_count;
}

void HiddenColumnData::Filter(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                              SelectionVector &sel, idx_t &filter_count, const TableFilter &filter,
                              TableFilterState &filter_state) {
	throw InternalException("A hidden column is not filtered");
}

void HiddenColumnData::Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                              SelectionVector &sel, idx_t select_count) {
	ReadNulls(result, select_count);
	state.offset_in_column += STANDARD_VECTOR_SIZE;
}

void HiddenColumnData::Skip(ColumnScanState &state, idx_t skip_count) {
	state.offset_in_column += skip_count;
}

void HiddenColumnData::InitializeAppend(ColumnAppendState &state) {
}

void HiddenColumnData::Append(ColumnAppendState &state, const Vector &vector, idx_t append_count) {
	count += append_count;
}

void HiddenColumnData::AppendData(ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t append_count) {
	count += append_count;
}

void HiddenColumnData::FinalizeAppend(ColumnDataFinalizeAppendState &finalize_state, ColumnAppendState &state) {
}

void HiddenColumnData::RevertAppend(row_t new_count) {
	count = UnsafeNumericCast<idx_t>(new_count);
}

void HiddenColumnData::FetchRows(TransactionData transaction, ColumnFetchState &state,
                                 const StorageIndex &storage_index, const idx_t *offsets, const SelectionVector &sel,
                                 idx_t fetch_count, Vector &result, idx_t result_offset) {
	for (idx_t i = 0; i < fetch_count; i++) {
		FlatVector::SetNull(result, result_offset + i, true);
	}
}

void HiddenColumnData::Update(TransactionData transaction, DuckTableEntry &table_entry, idx_t column_index,
                              Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t row_group_start) {
	throw InternalException("A hidden column is not updated");
}

void HiddenColumnData::UpdateColumn(TransactionData transaction, DuckTableEntry &table_entry,
                                    const vector<column_t> &column_path, Vector &update_vector, row_t *row_ids,
                                    idx_t update_count, idx_t depth, idx_t row_group_start) {
	throw InternalException("A hidden column is not updated");
}

unique_ptr<BaseStatistics> HiddenColumnData::GetUpdateStatistics() {
	return nullptr;
}

void HiddenColumnData::VisitBlockIds(BlockIdVisitor &visitor) const {
}

unique_ptr<ColumnCheckpointState> HiddenColumnData::CreateCheckpointState(const RowGroup &row_group, Context &context,
                                                                          PartialBlockManager &partial_block_manager) {
	throw InternalException("A hidden column is not written");
}

unique_ptr<ColumnCheckpointState> HiddenColumnData::Checkpoint(const RowGroup &row_group, ColumnCheckpointInfo &info,
                                                               const BaseStatistics &old_stats) {
	throw InternalException("A hidden column is not written");
}

void HiddenColumnData::CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                      Vector &scan_vector) const {
	throw InternalException("A hidden column is not written");
}

bool HiddenColumnData::IsPersistent() {
	return true;
}

PersistentColumnData HiddenColumnData::Serialize() {
	throw InternalException("A hidden column is not written");
}

void HiddenColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	throw InternalException("A hidden column is not read from disk");
}

void HiddenColumnData::GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
                                            vector<ColumnSegmentInfo> &result,
                                            const ColumnSegmentInfoScanOptions &options) {
}

void HiddenColumnData::Verify(RowGroup &parent) {
}

} // namespace duckdb
