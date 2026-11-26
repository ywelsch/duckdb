//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/duck_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/enums/checkpoint_type.hpp"
#include "duckdb/common/queue.hpp"

#include <condition_variable>

namespace duckdb {
class DuckTransactionManager;
class DuckTransaction;
class WriteAheadLog;
struct UndoBufferProperties;

//! CleanupInfo collects transactions awaiting cleanup.
//! This ensures we can clean up after releasing the transaction lock.
struct DuckCleanupInfo {
	//! All transactions in a cleanup info share the same lowest_visibility_bound.
	VisibilityBound lowest_visibility_bound;
	vector<unique_ptr<DuckTransaction>> transactions;

	void Cleanup();
	bool ScheduleCleanup() noexcept;
};

//! The Transaction Manager is responsible for creating and managing
//! transactions
class DuckTransactionManager : public TransactionManager {
public:
	explicit DuckTransactionManager(AttachedDatabase &db);
	~DuckTransactionManager() override;

public:
	static DuckTransactionManager &Get(AttachedDatabase &db);

	//! Start a new transaction
	Transaction &StartTransaction(Context &context, MetaTransaction &meta_transaction) override;
	//! Commit the given transaction
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	//! Commit the given transaction; a commit that would wait for another committer's WAL sync parks instead
	bool TryCommitTransaction(ClientContext &context, Transaction &transaction, const InterruptState &interrupt_state,
	                          ErrorData &error) override;
	//! Rollback the given transaction
	void RollbackTransaction(ClientContext &context, Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	VisibilityBound LowestVisibilityBound() const {
		return lowest_visibility_bound;
	}
	transaction_t GetLastCommit() const {
		return last_commit;
	}
	//! Wait until every published commit is durable. Called under the commit lock, so no new commit can
	//! enter its sync window and the wait is bounded by the syncs in flight
	void WaitForDurability();
	optional_idx GetActiveCheckpoint() const {
		auto id = active_checkpoint.load();
		return id == 0 ? optional_idx() : optional_idx(id);
	}
	idx_t NextCheckpointId() {
		return ++next_checkpoint_id;
	}
	void SetActiveCheckpoint(idx_t checkpoint_id);
	void ResetActiveCheckpoint();

	bool IsDuckTransactionManager() override {
		return true;
	}

	//! Obtains a shared lock to the checkpoint lock
	unique_ptr<StorageLockKey> SharedCheckpointLock();
	//! Try to obtain an exclusive checkpoint lock
	unique_ptr<StorageLockKey> TryGetCheckpointLock();
	unique_ptr<StorageLockKey> TryUpgradeCheckpointLock(StorageLockKey &lock);
	unique_ptr<StorageLockKey> SharedVacuumLock();
	unique_ptr<StorageLockKey> TryGetVacuumLock();

	//! Returns the current version of the catalog (incremented whenever anything changes, not stored between restarts)
	DUCKDB_API idx_t GetCatalogVersion(Transaction &transaction);

	void PushCatalogEntry(Transaction &transaction_p, CatalogEntry &entry, data_ptr_t extra_data = nullptr,
	                      idx_t extra_data_size = 0);
	void PushAttach(Transaction &transaction_p, AttachedDatabase &db);

protected:
	struct CheckpointDecision {
		explicit CheckpointDecision(string reason_p);
		explicit CheckpointDecision(CheckpointType type);
		~CheckpointDecision();

		bool can_checkpoint;
		string reason;
		CheckpointType type;
	};

private:
	//! Generates a new commit timestamp
	transaction_t GetCommitTimestamp();
	//! Allocates the cleanup info, and reserves the space RemoveTransaction needs to re-home a transaction.
	//! RemoveTransaction is noexcept, so it cannot do this itself: it must not allocate at all. Call this with
	//! transaction_lock held, immediately before RemoveTransaction, so that a failure to allocate is reported
	//! while the transaction lists are still untouched.
	unique_ptr<DuckCleanupInfo> CreateCleanupInfo();
	//! Remove the given transaction from the list of active transactions
	unique_ptr<DuckCleanupInfo> RemoveTransaction(DuckTransaction &transaction,
	                                              unique_ptr<DuckCleanupInfo> cleanup_info) noexcept;
	//! Remove the given transaction from the list of active transactions
	unique_ptr<DuckCleanupInfo> RemoveTransaction(DuckTransaction &transaction, bool store_transaction,
	                                              unique_ptr<DuckCleanupInfo> cleanup_info) noexcept;
	//! Recompute lowest_visibility_bound over the active transactions, leaving out `exclude`, and
	//! return its index among them (their count when absent). Caller holds the transaction lock
	idx_t UpdateLowestVisibilityBound(optional_ptr<DuckTransaction> exclude) noexcept;
	//! Move the committed transactions below the cleanup info's lowest visibility bound into it.
	//! Caller holds the transaction lock; must not allocate (see CreateCleanupInfo)
	void SweepCommittedTransactions(DuckCleanupInfo &cleanup_info) noexcept;
	//! Hand a cleanup to the background cleanup thread, if it has anything to do
	void QueueCleanup(unique_ptr<DuckCleanupInfo> cleanup_info);

	//! Whether or not we can checkpoint
	CheckpointDecision CanCheckpoint(DuckTransaction &transaction, unique_ptr<StorageLockKey> &checkpoint_lock,
	                                 const UndoBufferProperties &properties);
	//! Get the checkpoint type of an automatic checkpoint
	CheckpointDecision GetCheckpointType(DuckTransaction &transaction, const UndoBufferProperties &undo_properties);

	bool HasOtherTransactions(DuckTransaction &transaction);
	void CleanupTransactions();

	bool CommitTransactionInternal(ClientContext &context, DuckTransaction &transaction,
	                               optional_ptr<const InterruptState> interrupt_state, ErrorData &result);
	//! The automatic checkpoint a commit decided on, carried to the end of the commit
	struct CommitCheckpoint {
		explicit CommitCheckpoint(CheckpointDecision decision_p) : decision(std::move(decision_p)) {
		}
		CheckpointDecision decision;
		unique_ptr<StorageLockKey> lock;
		//! The commit had updates or dropped entries, which snapshots bounded below it may still need
		bool needs_pre_commit_state = false;
		//! Held through a checkpoint that replaces the commit's WAL write
		optional_ptr<unique_lock<mutex>> commit_lock;
		//! A failed checkpoint is reported apart from a commit that reached the WAL
		bool wal_written = false;
		//! Decided when the commit finishes, as it gave up its decision when it parked
		bool decide_on_finish = false;
	};
	//! The end of a commit, with the transaction lock held: park while another thread syncs, or sync, then leave the
	//! active transactions and run the automatic checkpoint. Returns false while parked
	bool FinishCommit(ClientContext &context, DuckTransaction &transaction, unique_lock<mutex> &t_lock,
	                  optional_ptr<const InterruptState> interrupt_state, bool store_transaction,
	                  CommitCheckpoint &checkpoint, ErrorData &error);
	//! Sync as one of the running syncs, wake parked commits and remove committer; releases the caller's lock
	ErrorData SyncCommits(unique_lock<mutex> &t_lock, WriteAheadLog &wal, idx_t offset,
	                      optional_ptr<DuckTransaction> committer, bool store_committer);

	//! Whether a commit that needed a WAL sync is still in its commit path, possibly inside SyncUpTo
	bool HasUnsyncedCommits();
	struct DurableSnapshot {
		//! Every commit before this bound is durable
		VisibilityBound visibility_bound = VisibilityBound::IncludingUncommitted();
		//! The catalog version that snapshot observes
		idx_t catalog_version = DConstants::INVALID_INDEX;
	};
	//! The most recent snapshot that contains only durable commits; unbounded when none is pending
	DurableSnapshot GetDurableSnapshot();

private:
	//! The current start timestamp used by transactions
	transaction_t current_start_timestamp;
	//! The current transaction ID used by transactions
	transaction_t current_transaction_id;
	//! The lowest bound any active transaction reads at. A version preceding it is visible to
	//! every active transaction, so whatever it supersedes can be cleaned up or compacted
	atomic<VisibilityBound> lowest_visibility_bound;
	//! The last commit timestamp
	atomic<transaction_t> last_commit;
	//! The currently active checkpoint, zero when none is running
	atomic<idx_t> active_checkpoint;
	//! Source of checkpoint identities
	atomic<idx_t> next_checkpoint_id = {0};
	//! Set of currently running transactions
	vector<unique_ptr<DuckTransaction>> active_transactions;
	//! Set of recently committed transactions
	vector<unique_ptr<DuckTransaction>> recently_committed_transactions;
	//! The lock used for transaction operations
	mutex transaction_lock;
	//! The checkpoint lock
	StorageLock checkpoint_lock;
	//! The vacuum lock - necessary to start vacuum operations
	StorageLock vacuum_lock;
	//! Lock necessary to start transactions only - used by FORCE CHECKPOINT to prevent new transactions from starting
	mutex start_transaction_lock;

	//! Every commit before this bound is durable; it only ever advances. A transaction stays in
	//! active_transactions until its commit is durable, so new snapshots are bounded below commits
	//! that are not yet durable
	VisibilityBound durable_bound;
	//! Signalled when running_syncs drops to zero
	std::condition_variable durability_cv;
	//! Threads inside SyncCommits: commits park only while one runs, and the drain waits for none
	idx_t running_syncs = 0;
	//! A commit below this bound that is not durable never will be: its WAL failed to sync with sync_failure
	VisibilityBound failed_bound;
	ErrorData sync_failure;

	atomic<idx_t> last_uncommitted_catalog_version = {TRANSACTION_ID_START};
	idx_t last_committed_version = 0;

	//! Only one cleanup can be active at any time.
	mutex cleanup_lock;
	//! Changes to the cleanup queue must be synchronized.
	mutex cleanup_queue_lock;
	//! Cleanups have to happen in-order.
	//! E.g., if one transaction drops a table, and another creates a table,
	//! inverting the cleanup order can result in catalog errors.
	queue<unique_ptr<DuckCleanupInfo>> cleanup_queue;

protected:
	virtual void OnCommitCheckpointDecision(const CheckpointDecision &decision, DuckTransaction &transaction) {
	}
};

} // namespace duckdb
