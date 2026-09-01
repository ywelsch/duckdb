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
//! the same type.
struct SnapshotView {
	SnapshotView(SnapshotBound snapshot_bound_p, TransactionId transaction_id_p)
	    : snapshot_bound(snapshot_bound_p), transaction_id(transaction_id_p) {
	}

	//! The bound this reads at
	SnapshotBound snapshot_bound;
	//! The id of the transaction reading, so it can see what it has written itself
	TransactionId transaction_id;

	//! Whether a stamp is visible from here: it sorts below the bound, or we wrote it ourselves.
	//! NOTE: splitting this into "committed and below the bound, otherwise ours" is NOT equivalent.
	//! A bound above the split admits uncommitted stamps, and a stamp can equal a sentinel
	//! transaction id while being committed; both cases are load-bearing today.
	bool Sees(Stamp stamp) const {
		return stamp.Below(snapshot_bound) || stamp == Stamp(transaction_id);
	}
};

struct TransactionData {
	TransactionData(DuckTransaction &transaction_p); // NOLINT: allow implicit conversion
	TransactionData(TransactionId transaction_id_p, SnapshotBound snapshot_bound_p);

	optional_ptr<DuckTransaction> transaction;
	SnapshotView view;

	static TransactionData Committed() {
		return TransactionData(TransactionId::None(), SnapshotBound::Nothing());
	}
};

} // namespace duckdb
