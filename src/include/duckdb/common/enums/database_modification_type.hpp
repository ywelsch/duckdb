//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/database_modification_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

struct DatabaseModificationType {
public:
	static constexpr idx_t INSERT_DATA = 1ULL << 0ULL;
	static constexpr idx_t DELETE_DATA = 1ULL << 1ULL;
	static constexpr idx_t UPDATE_DATA = 1ULL << 2ULL;
	static constexpr idx_t ALTER_TABLE = 1ULL << 3ULL;
	static constexpr idx_t CREATE_CATALOG_ENTRY = 1ULL << 4ULL;
	static constexpr idx_t DROP_CATALOG_ENTRY = 1ULL << 5ULL;
	static constexpr idx_t SEQUENCE = 1ULL << 6ULL;
	static constexpr idx_t CREATE_INDEX = 1ULL << 7ULL;
	//! A new version of an existing table that shares the column data of the old version
	static constexpr idx_t REWRITE_TABLE_STORAGE = 1ULL << 8ULL;
	//! Table or index storage that may be dropped at commit
	static constexpr idx_t DROP_STORAGE = 1ULL << 9ULL;

	constexpr DatabaseModificationType() : value(0) {
	}
	constexpr DatabaseModificationType(idx_t value) : value(value) { // NOLINT : allow implicit conversion
	}

	inline constexpr DatabaseModificationType operator|(DatabaseModificationType b) const {
		return DatabaseModificationType(value | b.value);
	}
	inline DatabaseModificationType &operator|=(DatabaseModificationType b) {
		value |= b.value;
		return *this;
	}

	bool InsertData() const {
		return value & INSERT_DATA;
	}
	bool DeleteData() const {
		return value & DELETE_DATA;
	}
	bool UpdateData() const {
		return value & UPDATE_DATA;
	}
	bool AlterTable() const {
		return value & ALTER_TABLE;
	}
	bool CreateCatalogEntry() const {
		return value & CREATE_CATALOG_ENTRY;
	}
	bool DropCatalogEntry() const {
		return value & DROP_CATALOG_ENTRY;
	}
	bool Sequence() const {
		return value & SEQUENCE;
	}
	bool CreateIndex() const {
		return value & CREATE_INDEX;
	}
	bool RewriteTableStorage() const {
		return value & REWRITE_TABLE_STORAGE;
	}
	bool DropStorage() const {
		return value & DROP_STORAGE;
	}
	//! Whether a transaction making these modifications keeps checkpoints from running until it is cleaned up
	bool RequiresCheckpointLock() const {
		return UpdateData() || RewriteTableStorage();
	}
	//! Whether a transaction making these modifications keeps checkpoints from running while it commits
	bool RequiresCommitCheckpointLock() const {
		return DropStorage();
	}
	//! Whether a transaction making these modifications keeps full checkpoints from vacuuming
	bool RequiresVacuumLock() const {
		return InsertData() || DeleteData() || CreateIndex();
	}

private:
	idx_t value;
};

} // namespace duckdb
