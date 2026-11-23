//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"

namespace duckdb {

class ClientContext;
class InterruptState;
class MetaTransaction;
class Transaction;
class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
class TransactionContext {
public:
	explicit TransactionContext(ClientContext &context);
	~TransactionContext();

	MetaTransaction &ActiveTransaction() {
		if (!current_transaction) {
			throw InternalException("TransactionContext::ActiveTransaction called without active transaction");
		}
		return *current_transaction;
	}

	bool HasActiveTransaction() const {
		return current_transaction.get();
	}

	void BeginTransaction();
	void Commit();
	//! Commits like Commit, but returns false while the commit waits for its durability, after arranging for the
	//! interrupt state to be called back. Must be called again until it returns true
	bool TryCommit(const InterruptState &interrupt_state);
	//! Whether TryCommit has started a commit that has not finished
	bool HasPendingCommit() const {
		return committing_transaction.get();
	}
	//! Finish the commit TryCommit started, blocking until it is durable
	void FinishPendingCommit();
	void Rollback(optional_ptr<ErrorData>);
	void ClearTransaction();
	void SetAutocheckpointError(ErrorData error);

	void SetAutoCommit(bool value);
	bool IsAutoCommit() const {
		return auto_commit;
	}

	void SetReadOnly();

	idx_t GetActiveQuery();
	void ResetActiveQuery();
	void SetActiveQuery(transaction_t query_number);

	void SetInvalidationPolicy(TransactionInvalidationPolicy new_invalidation_policy);
	TransactionInvalidationPolicy GetInvalidationPolicy() {
		return invalidation_policy;
	};
	void SetAutoRollback(bool new_auto_rollback) {
		auto_rollback = new_auto_rollback;
	};
	bool GetAutoRollback() {
		return auto_rollback;
	};

private:
	void StartCommit();
	//! Ends the commit of committing_transaction
	void CompleteCommit(ErrorData error);

	ClientContext &context;
	bool auto_commit;
	TransactionInvalidationPolicy invalidation_policy = TransactionInvalidationPolicy::STANDARD_POLICY;
	bool auto_rollback = false;

	unique_ptr<MetaTransaction> current_transaction;
	//! The transaction whose commit TryCommit started and has not finished
	unique_ptr<MetaTransaction> committing_transaction;
	//! The query number of the query currently running on this connection (MAXIMUM_QUERY_ID if none)
	atomic<transaction_t> active_query_id = MAXIMUM_QUERY_ID;
	ErrorData autocheckpoint_error;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb
