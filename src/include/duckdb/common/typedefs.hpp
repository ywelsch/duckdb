//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/typedefs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {

//! a saner size_t for loop indices etc
typedef uint64_t idx_t;

//! The type used for row identifiers
typedef int64_t row_t;

//! The type used for hashes
typedef uint64_t hash_t;

//! data pointers
typedef uint8_t data_t;
typedef data_t *data_ptr_t;
typedef const data_t *const_data_ptr_t;

//! Type used for the selection vector
typedef uint32_t sel_t;
//! A point on the transaction timeline. Stamps below TRANSACTION_ID_START are commit ids, and the
//! snapshot bounds derived from them; stamps at or above it are ids of transactions that have not
//! committed, so a stamp says by itself whether the transaction that wrote it committed. Catalog
//! versions borrow the same split. Deliberately offers no ordering operators: every comparison goes
//! through a named method that says which question is being asked.
struct SnapshotId {
	//! 2^62 - the split between commit ids below and transaction ids above
	static constexpr idx_t TRANSACTION_ID_START_VALUE = 4611686018427388000ULL;

	SnapshotId() = default;
	explicit constexpr SnapshotId(idx_t value_p) : value(value_p) {
	}

	//! Whether the transaction that stamped this has committed
	constexpr bool IsCommitted() const {
		return value < TRANSACTION_ID_START_VALUE;
	}
	//! Whether this stamp is visible to a snapshot bounded by `bound`: stamps below the bound are
	//! visible, stamps at or above it are not. Not every bound is a transaction's start time -
	//! callers also derive one from a commit id, or from the last commit.
	constexpr bool VisibleTo(SnapshotId bound) const {
		return value < bound.value;
	}
	//! The lower of two stamps - for folding a horizon over the active transactions
	static constexpr SnapshotId Min(SnapshotId a, SnapshotId b) {
		return a.value < b.value ? a : b;
	}
	//! The higher of two stamps
	static constexpr SnapshotId Max(SnapshotId a, SnapshotId b) {
		return a.value > b.value ? a : b;
	}
	//! The next stamp on the timeline - turns an inclusive bound into an exclusive one
	constexpr SnapshotId Next() const {
		return SnapshotId(value + 1);
	}
	//! The previous stamp on the timeline
	constexpr SnapshotId Prev() const {
		return SnapshotId(value - 1);
	}
	//! The raw stamp - only for serialization and logging
	constexpr idx_t GetIndex() const {
		return value;
	}

	idx_t value = 0;
};

//! Free functions, not members: a member operator would not convert an atomic<SnapshotId> on the left
constexpr bool operator==(SnapshotId a, SnapshotId b) {
	return a.GetIndex() == b.GetIndex();
}
constexpr bool operator!=(SnapshotId a, SnapshotId b) {
	return a.GetIndex() != b.GetIndex();
}

//! Type used for transaction timestamps
typedef SnapshotId transaction_t;

//! Type used to identify connections
typedef idx_t connection_t;

//! Type used for column identifiers
typedef idx_t column_t;
//! Type used for storage (column) identifiers
typedef idx_t storage_t;

template <class SRC>
data_ptr_t data_ptr_cast(SRC *src) { // NOLINT: naming
	return reinterpret_cast<data_ptr_t>(src);
}

template <class SRC>
const_data_ptr_t const_data_ptr_cast(const SRC *src) { // NOLINT: naming
	return reinterpret_cast<const_data_ptr_t>(src);
}

template <class SRC>
char *char_ptr_cast(SRC *src) { // NOLINT: naming
	return reinterpret_cast<char *>(src);
}

template <class SRC>
const char *const_char_ptr_cast(const SRC *src) { // NOLINT: naming
	return reinterpret_cast<const char *>(src);
}

template <class SRC>
const unsigned char *const_uchar_ptr_cast(const SRC *src) { // NOLINT: naming
	return reinterpret_cast<const unsigned char *>(src);
}

template <class SRC>
uintptr_t CastPointerToValue(SRC *src) {
	return reinterpret_cast<uintptr_t>(src);
}

template <class SRC>
uint64_t cast_pointer_to_uint64(SRC *src) {
	return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(src));
}

template <class SRC = data_t>
SRC *cast_uint64_to_pointer(uint64_t value) {
	return reinterpret_cast<SRC *>(static_cast<uintptr_t>(value));
}

} // namespace duckdb
