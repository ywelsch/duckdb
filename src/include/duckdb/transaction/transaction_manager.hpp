//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

class AttachedDatabase;
class ClientContext;
class Context;
class MetaTransaction;
class Catalog;
struct ClientLockWrapper;
class DatabaseInstance;
class InterruptState;
class Transaction;

//! A commit that has been published and only waits until it is durable
class PendingCommit {
public:
	virtual ~PendingCommit() = default;

	//! Finish the commit once it is durable. Returns false while it still waits, after arranging for the interrupt
	//! state to be called back when it is worth calling again
	virtual bool TryFinish(const InterruptState &interrupt_state, ErrorData &error) = 0;
	//! Finish the commit, blocking until it is durable
	virtual ErrorData Finish() = 0;
};

//! The Transaction Manager is responsible for creating and managing
//! transactions
class TransactionManager {
public:
	explicit TransactionManager(AttachedDatabase &db);
	virtual ~TransactionManager();

	//! Start a new transaction as part of the given meta transaction
	virtual Transaction &StartTransaction(Context &context, MetaTransaction &meta_transaction) = 0;
	//! Commit the given transaction. Returns a non-empty error message on failure.
	virtual ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) = 0;
	//! Like CommitTransaction, but a commit that only waits for its durability may return early with `pending` set,
	//! which the caller must then finish. By default the commit completes before returning
	virtual ErrorData CommitTransactionDeferred(ClientContext &context, Transaction &transaction,
	                                            unique_ptr<PendingCommit> &pending) {
		return CommitTransaction(context, transaction);
	}
	//! Rollback the given transaction
	virtual void RollbackTransaction(ClientContext &context, Transaction &transaction) = 0;

	virtual void Checkpoint(ClientContext &context, bool force = false) = 0;

	static TransactionManager &Get(AttachedDatabase &db);

	virtual bool IsDuckTransactionManager() {
		return false;
	}

	AttachedDatabase &GetDB() {
		return db;
	}

protected:
	//! The attached database
	AttachedDatabase &db;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
