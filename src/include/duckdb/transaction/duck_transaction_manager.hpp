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
#include "duckdb/common/reference_map.hpp"
#include "duckdb/parallel/interrupt.hpp"

#include <condition_variable>

namespace duckdb {
class DataTable;
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
	//! Commit; with external_commit_sync, a commit that needs a WAL sync parks until a sync covers it
	bool TryCommitTransaction(ClientContext &context, Transaction &transaction, const InterruptState &interrupt_state,
	                          ErrorData &error) override;
	//! Sync the WAL over every published commit that is not durable, and wake the commits awaiting a sync.
	//! Returns false if there was nothing to sync
	bool SyncCommits();
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
	//! Ends the checkpoint. If it succeeded, frees the tables that drops handed over to it
	void ResetActiveCheckpoint(bool checkpoint_succeeded);
	//! Hands dropped tables over to the running checkpoint, which frees them. False if none runs: the commit does
	bool DeferDroppedTables(const vector<shared_ptr<DataTable>> &tables);
	//! The next start time, and the earliest of the active transactions: the same if none, 0 if a cleanup waits
	void GetStartTimes(transaction_t &next_start, transaction_t &lowest_active_start);

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

	//! A commit between its steps: PublishCommit, SyncCommit when it needs a WAL sync, then FinishCommit
	struct PublishedCommit {
		explicit PublishedCommit(DuckTransaction &transaction_p)
		    : transaction(transaction_p), checkpoint_decision("no checkpoint decided") {
		}
		DuckTransaction &transaction;
		//! The WAL to sync up to the transaction's wal_sync_offset before the commit is acknowledged; null when none
		optional_ptr<WriteAheadLog> wal;
		//! Passed to RemoveTransaction
		bool store_transaction = false;
		CheckpointDecision checkpoint_decision;
		unique_ptr<StorageLockKey> checkpoint_lock;
		//! The commit had updates or dropped entries, which snapshots bounded below it may still need
		bool needs_pre_commit_state = false;
		//! Held through a checkpoint that replaces the commit's WAL write
		unique_lock<mutex> commit_lock;
		//! A failed checkpoint after a commit that reached the WAL is reported apart from the commit
		bool wal_written = false;
		//! Await an external WAL sync instead of syncing, also when the commit is alone
		bool await_sync = false;
		//! The parked committer, called back when a sync covered the commit or failed
		InterruptState sync_callback;
	};
	//! Write the WAL and publish without syncing; a commit that needs no sync leaves the active transactions
	ErrorData PublishCommit(ClientContext &context, PublishedCommit &commit);
	//! Make a published commit durable, and so visible to new snapshots, then remove it from the active transactions
	ErrorData SyncCommit(PublishedCommit &commit);
	//! Clean up after the commit and run its automatic checkpoint
	void FinishCommit(ClientContext &context, PublishedCommit &commit, ErrorData &error);
	//! Park a published commit until a sync covers it. Returns false while parked
	bool AwaitSync(ClientContext &context, DuckTransaction &transaction, unique_lock<mutex> &t_lock,
	               optional_ptr<const InterruptState> interrupt_state, ErrorData &error);
	//! An awaiting commit that a sync covered or failed, removed from the active transactions; null while it waits
	unique_ptr<PublishedCommit> TakeSyncedCommit(DuckTransaction &transaction, ErrorData &error);
	//! Sync for the published commits that are not durable, releasing t_lock meanwhile. False if nothing to sync
	bool SyncPublishedCommits(unique_lock<mutex> &t_lock);
	//! Advance the durable bound over the commits a sync of wal covered; returns the awaiting commits to wake
	vector<InterruptState> CompleteSync(WriteAheadLog &wal, const ErrorData &sync_error);
	//! No committer is in its own sync and no external sync runs, so the WAL can be replaced
	bool NoSyncsRunning();
	//! Lets the block manager reuse the blocks that checkpoints freed before every active transaction started
	void ReuseFreedBlocks(transaction_t lowest_start) noexcept;
	//! The earliest start time of the active transactions, checkpoints aside. Caller holds the transaction lock
	transaction_t LowestActiveStart();

	//! Whether a commit that needed a WAL sync is still in its commit path, possibly inside SyncUpTo. Commits
	//! awaiting an external sync are not
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
	//! Guards the dropped tables that are handed over to a checkpoint
	mutex deferred_tables_lock;
	//! The dropped tables that the running checkpoint frees once it has written its header
	vector<shared_ptr<DataTable>> deferred_tables;
	//! The lowest start time of the transactions that stayed active when the last one ended, checkpoints aside
	atomic<transaction_t> lowest_remaining_start;
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
	//! Signalled when NoSyncsRunning() may have become true
	std::condition_variable durability_cv;
	//! Published commits that await a sync they do not run themselves
	reference_map_t<DuckTransaction, unique_ptr<PublishedCommit>> awaiting_commits;
	//! Threads in SyncPublishedCommits
	idx_t external_syncs = 0;
	//! A commit below this bound that is not durable never will be: a sync of its WAL failed with sync_failure
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
