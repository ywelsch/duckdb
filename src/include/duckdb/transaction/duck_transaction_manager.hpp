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
#include "duckdb/common/deque.hpp"

#include <condition_variable>

namespace duckdb {
class DuckTransactionManager;
class DuckTransaction;
struct UndoBufferProperties;

//! CleanupInfo collects transactions awaiting cleanup.
//! This ensures we can clean up after releasing the transaction lock.
struct DuckCleanupInfo {
	//! All transactions in a cleanup info share the same lowest_start_time.
	transaction_t lowest_start_time;
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
	Transaction &StartTransaction(ClientContext &context) override;
	//! Commit the given transaction
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	//! Rollback the given transaction
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	transaction_t LowestActiveId() const {
		return lowest_active_id;
	}
	transaction_t LowestActiveStart() const {
		return lowest_active_start;
	}
	transaction_t GetLastCommit() const {
		return last_commit;
	}
	//! Wait until every published commit is durable (i.e. its WAL flush marker is synced)
	void WaitForDurability();
	transaction_t GetActiveCheckpoint() const {
		return active_checkpoint;
	}
	void SetActiveCheckpoint(transaction_t checkpoint_id);
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
	//! Remove the given transaction from the list of active transactions
	unique_ptr<DuckCleanupInfo> RemoveTransaction(DuckTransaction &transaction) noexcept;
	//! Remove the given transaction from the list of active transactions
	unique_ptr<DuckCleanupInfo> RemoveTransaction(DuckTransaction &transaction, bool store_transaction) noexcept;

	//! Whether or not we can checkpoint
	CheckpointDecision CanCheckpoint(DuckTransaction &transaction, unique_ptr<StorageLockKey> &checkpoint_lock,
	                                 const UndoBufferProperties &properties);
	//! Get the checkpoint type of an automatic checkpoint
	CheckpointDecision GetCheckpointType(DuckTransaction &transaction, const UndoBufferProperties &undo_properties);

	bool HasOtherTransactions(DuckTransaction &transaction);
	void CleanupTransactions();

private:
	//! The current start timestamp used by transactions
	transaction_t current_start_timestamp;
	//! The current transaction ID used by transactions
	transaction_t current_transaction_id;
	//! The lowest active transaction id
	atomic<transaction_t> lowest_active_id;
	//! The lowest active transaction timestamp
	atomic<transaction_t> lowest_active_start;
	//! The last commit timestamp
	atomic<transaction_t> last_commit;
	//! The currently active checkpoint
	atomic<transaction_t> active_checkpoint;
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

	//! Commits are published before their WAL flush marker is synced to disk. Until the sync
	//! completes they are tracked here, and new transactions bound their snapshot below them,
	//! so that no transaction can observe a commit that a crash could still lose.
	//! The durable bound advances as soon as a sync covers an entry's flush marker (so a
	//! committer's ack implies its commit is observable, even if threads wake out of order),
	//! but an entry is only *removed* by its own committing thread, after that thread has
	//! fully left WriteAheadLog::SyncUpTo - this makes WaitForDurability a quiescence barrier
	//! for the WAL object, so a checkpoint can destroy it without shared ownership.
	struct UnsyncedCommit {
		//! The commit id of the published commit
		transaction_t commit_id;
		//! The WAL offset covering the commit's flush marker
		idx_t wal_offset;
		//! Whether the committing thread has left WriteAheadLog::SyncUpTo
		bool thread_done;
	};
	//! Protects unsynced_commits and durability_failed
	mutex durability_lock;
	//! Published commits whose flush marker is not yet durable (in commit order)
	deque<UnsyncedCommit> unsynced_commits;
	//! Signalled when unsynced_commits becomes empty, or when a sync fails
	std::condition_variable durability_cv;
	//! The durable bound: the highest commit id for which it and all lower commits are durable
	atomic<transaction_t> durable_commit_bound = {0};
	//! Number of entries in unsynced_commits
	atomic<idx_t> unsynced_commit_count = {0};
	//! Set when a WAL sync has failed (the database is poisoned)
	bool durability_failed = false;

	//! Register a published commit whose flush marker is not yet synced.
	//! Called while holding the transaction lock and the WAL lock.
	void RegisterUnsyncedCommit(transaction_t commit_id, idx_t wal_offset);
	//! Advance the durable bound over every commit covered by a completed sync, mark this
	//! thread's own commit as finished, and remove the finished prefix. Must only be called
	//! after the thread has fully left WriteAheadLog::SyncUpTo.
	void FinishCommitDurability(transaction_t commit_id, idx_t synced_offset);
	//! Mark that a WAL sync has failed, waking up durability waiters
	void MarkDurabilityFailed();
	//! Sweep transactions that were pinned only by not-yet-durable commits. Committed
	//! transactions keep their version information (and their shared checkpoint lock) alive
	//! while bounded snapshots may still need it; once the durable bound advances past them,
	//! nothing else re-triggers the garbage collection sweep - this does.
	void GarbageCollectDurableTransactions();
	bool HasUnsyncedCommits() const {
		return unsynced_commit_count.load() > 0;
	}
	//! The snapshot bound while unsynced commits exist: a transaction starting at this
	//! timestamp sees every durable commit and no unsynced commit
	transaction_t BoundedSnapshotStart() const {
		return durable_commit_bound.load() + 1;
	}

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
