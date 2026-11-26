#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

RowGroupWriter::RowGroupWriter(TableDataWriter &writer, TableCatalogEntry &table,
                               PartialBlockManager &partial_block_manager)
    : writer(writer), table(table), partial_block_manager(partial_block_manager) {
	for (auto &col : table.GetColumns().Physical()) {
		compression_types.push_back(col.CompressionType());
	}
}

DatabaseInstance &RowGroupWriter::GetDatabase() {
	return table.ParentCatalog().GetDatabase();
}

AttachedDatabase &RowGroupWriter::GetAttachedDatabase() {
	return table.ParentCatalog().GetAttached();
}

SingleFileRowGroupWriter::SingleFileRowGroupWriter(TableCatalogEntry &table, PartialBlockManager &partial_block_manager,
                                                   TableDataWriter &writer, MetadataWriter &column_metadata_writer)
    : RowGroupWriter(writer, table, partial_block_manager), column_metadata_writer(column_metadata_writer) {
}

CheckpointOptions SingleFileRowGroupWriter::GetCheckpointOptions() const {
	return writer.GetCheckpointOptions();
}

WriteStream &SingleFileRowGroupWriter::GetPayloadWriter() {
	return column_metadata_writer;
}

MetaBlockPointer SingleFileRowGroupWriter::GetMetaBlockPointer() {
	return column_metadata_writer.GetMetaBlockPointer();
}

optional_ptr<MetadataManager> SingleFileRowGroupWriter::GetMetadataManager() {
	return column_metadata_writer.GetManager();
}

void SingleFileRowGroupWriter::StartWritingColumns(vector<MetaBlockPointer> &column_metadata) {
	column_metadata_writer.SetWrittenPointers(column_metadata);
}

void SingleFileRowGroupWriter::FinishWritingColumns() {
	column_metadata_writer.SetWrittenPointers(nullptr);
}

} // namespace duckdb
