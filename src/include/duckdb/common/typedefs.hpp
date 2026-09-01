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
//===--------------------------------------------------------------------===//
// The transaction timeline, as three domains rather than one number
//===--------------------------------------------------------------------===//

//! 2^62 - the split between commit ids below and transaction ids above
static constexpr idx_t TRANSACTION_ID_START_VALUE = 4611686018427388000ULL;

struct SnapshotBound;

//! A transaction that has committed. Ordered against a bound, never against a transaction id.
struct CommitId {
	CommitId() = default;
	explicit constexpr CommitId(idx_t value_p) : value(value_p) {
	}
	//! Whether this commit is below (visible to) the given bound
	bool Below(SnapshotBound bound) const;
	constexpr idx_t GetIndex() const {
		return value;
	}
	constexpr CommitId Next() const {
		return CommitId(value + 1);
	}
	idx_t value = 0;
};

//! A transaction still in flight. Only ever compared for identity.
struct TransactionId {
	TransactionId() = default;
	explicit constexpr TransactionId(idx_t value_p) : value(value_p) {
	}
	constexpr idx_t GetIndex() const {
		return value;
	}
	constexpr TransactionId Next() const {
		return TransactionId(value + 1);
	}
	idx_t value = 0;
};

//! An exclusive bound on the committed timeline: commits below it are visible, commits at or above
//! it are not. Built from a commit id, so it cannot accidentally admit uncommitted stamps.
struct SnapshotBound {
	SnapshotBound() = default;
	//! Everything this commit and earlier is visible
	static constexpr SnapshotBound Through(CommitId commit) {
		return SnapshotBound(commit.GetIndex() + 1);
	}
	//! Everything strictly below this commit is visible
	static constexpr SnapshotBound Before(CommitId commit) {
		return SnapshotBound(commit.GetIndex());
	}
	//! Every committed stamp is visible, and no uncommitted one
	static constexpr SnapshotBound AllCommitted() {
		return SnapshotBound(TRANSACTION_ID_START_VALUE);
	}
	constexpr idx_t GetIndex() const {
		return value;
	}
	idx_t value = 0;

private:
	explicit constexpr SnapshotBound(idx_t value_p) : value(value_p) {
	}
};

inline bool CommitId::Below(SnapshotBound bound) const {
	return value < bound.GetIndex();
}

//! What a version slot actually holds: a TransactionId while the writer is in flight, overwritten
//! with its CommitId when it commits. Which domain it is in is only knowable by asking.
struct Stamp {
	Stamp() = default;
	explicit constexpr Stamp(idx_t value_p) : value(value_p) {
	}
	explicit constexpr Stamp(CommitId commit) : value(commit.GetIndex()) {
	}
	explicit constexpr Stamp(TransactionId id) : value(id.GetIndex()) {
	}

	constexpr bool IsCommitted() const {
		return value < TRANSACTION_ID_START_VALUE;
	}
	constexpr CommitId AsCommitId() const {
		return CommitId(value);
	}
	constexpr TransactionId AsTransactionId() const {
		return TransactionId(value);
	}
	constexpr idx_t GetIndex() const {
		return value;
	}
	idx_t value = 0;
};

constexpr bool operator==(Stamp a, Stamp b) {
	return a.GetIndex() == b.GetIndex();
}
constexpr bool operator!=(Stamp a, Stamp b) {
	return a.GetIndex() != b.GetIndex();
}
constexpr bool operator==(TransactionId a, TransactionId b) {
	return a.GetIndex() == b.GetIndex();
}
constexpr bool operator!=(TransactionId a, TransactionId b) {
	return a.GetIndex() != b.GetIndex();
}
constexpr bool operator==(CommitId a, CommitId b) {
	return a.GetIndex() == b.GetIndex();
}
constexpr bool operator!=(CommitId a, CommitId b) {
	return a.GetIndex() != b.GetIndex();
}

//! Type used for transaction timestamps: what is stored, unless a narrower domain is known
typedef Stamp transaction_t;

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
