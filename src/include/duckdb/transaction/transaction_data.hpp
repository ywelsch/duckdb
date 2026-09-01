//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class DuckTransaction;
class Transaction;

//! The two stamps that together decide what a transaction sees: the bound it reads at, and its own
//! id, so that its own uncommitted writes stay visible to it. They are only meaningful as a pair -
//! holding them separately invites pairing the wrong two, which no type can catch once both are
//! SnapshotId.
struct SnapshotView {
	SnapshotView(transaction_t snapshot_bound_p, transaction_t transaction_id_p)
	    : snapshot_bound(snapshot_bound_p), transaction_id(transaction_id_p) {
	}

	//! The bound this reads at
	transaction_t snapshot_bound;
	//! The id of the transaction reading, so it can see what it has written itself
	transaction_t transaction_id;

	//! Whether a stamp is visible from here: committed below the bound, or written by us
	bool Sees(transaction_t stamp) const {
		return stamp.VisibleTo(snapshot_bound) || stamp == transaction_id;
	}
};

struct TransactionData {
	TransactionData(DuckTransaction &transaction_p); // NOLINT: allow implicit conversion
	TransactionData(transaction_t transaction_id_p, transaction_t snapshot_bound_p);

	optional_ptr<DuckTransaction> transaction;
	SnapshotView view;

	static TransactionData Committed() {
		return TransactionData(MAX_TRANSACTION_ID, SnapshotId(0));
	}
};

} // namespace duckdb
