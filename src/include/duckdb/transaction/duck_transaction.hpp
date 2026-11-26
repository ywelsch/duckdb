//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/duck_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/enums/active_transaction_state.hpp"

namespace duckdb {
class CommitDropState;
class DuckTableEntry;
class RowGroupCollection;
class RowGroupVersionInfo;
class RowVersionManager;
class VacuumSealLatch;
struct ReaderRowGroups;
class DuckTransactionManager;
class StorageLockKey;
class StorageCommitState;
struct UndoBufferProperties;
struct WrittenRows;

struct CommitInfo {
	transaction_t commit_id;
	ActiveTransactionState active_transactions = ActiveTransactionState::UNSET;
	optional_ptr<CommitDropState> drop_state;
	//! WAL offset covering the commit's flush marker (0 if no WAL was written)
	idx_t wal_sync_offset = 0;
};

class DuckTransaction : public Transaction {
public:
	DuckTransaction(DuckTransactionManager &manager, ClientContext &context, transaction_t start_time,
	                SnapshotView view, idx_t catalog_version);
	~DuckTransaction() override;

	//! The start timestamp of this transaction
	transaction_t start_time;
	//! What this transaction sees: its own writes, and everything before its visibility bound
	SnapshotView view;
	//! The commit id of this transaction, if it has successfully been committed
	transaction_t commit_id;
	//! WAL offset covering the commit's flush marker, set when the commit is published; the commit
	//! is durable once the WAL is synced up to it (0 while uncommitted or when nothing was written)
	idx_t wal_sync_offset = 0;
	//! The committed catalog version just before this commit published
	idx_t catalog_version_before_commit = 0;

	atomic<idx_t> catalog_version;

	//! Transactions undergo Cleanup, after (1) removing them directly in RemoveTransaction,
	//! or (2) after they enter cleanup_queue.
	//! Some (after rollback) enter cleanup_queue, but do not require Cleanup.
	bool awaiting_cleanup;

public:
	static DuckTransaction &Get(ClientContext &context, AttachedDatabase &db);
	static DuckTransaction &Get(ClientContext &context, Catalog &catalog);
	LocalStorage &GetLocalStorage();

	void PushCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data, idx_t extra_data_size);
	void PushAttach(AttachedDatabase &db);

	//! Acquire a shared checkpoint lock for a statement, or return null if this transaction already holds one.
	unique_ptr<StorageLockKey> GetCheckpointLockForStatement();
	void SetModifications(DatabaseModificationType type) override;
	void ReleaseStatementState() override;
	//! Whether the transaction holds the shared vacuum lock
	bool HoldsVacuumLock();
	//! Holds the shared vacuum lock until the transaction ends: the row ids it hands out stay valid
	void KeepVacuumLockForTransaction();
	//! The row groups the running statement reads a table from: the live ones, or, once a checkpoint moved rows,
	//! the ones from before
	ReaderRowGroups GetRowGroups(const shared_ptr<RowGroupCollection> &collection);
	//! The same, without making the statement a reader of the table (for use before it holds its locks)
	ReaderRowGroups PeekRowGroups(const shared_ptr<RowGroupCollection> &collection);
	//! Keeps a row group this transaction writes deletes or update markers to out of merges
	void RegisterRowGroupWrite(const shared_ptr<RowGroupVersionInfo> &version_info);
	//! Waits until the checkpoints that merged row groups this transaction wrote to are durable
	ErrorData WaitForVacuumSeals();

	bool ShouldWriteToWAL(AttachedDatabase &db);
	ErrorData PreFlushOptimisticBlocks(AttachedDatabase &db) noexcept;
	//! Appends the local storage to the tables; with a WAL, the commit state records the optimistically written blocks
	ErrorData AppendLocalStorage(ClientContext &context, AttachedDatabase &db,
	                             unique_ptr<StorageCommitState> &commit_state) noexcept;
	//! Writes the undo buffer to the WAL, with the commit state of AppendLocalStorage
	ErrorData WriteToWAL(ClientContext &context, AttachedDatabase &db,
	                     unique_ptr<StorageCommitState> &commit_state) noexcept;
	//! Commit the current transaction with the given commit identifier. Returns an error message if the transaction
	//! commit failed, or an empty string if the commit was successful
	ErrorData Commit(AttachedDatabase &db, CommitInfo &commit_info,
	                 unique_ptr<StorageCommitState> commit_state) noexcept;
	//! Returns whether or not a commit of this transaction should trigger an automatic checkpoint
	bool AutomaticCheckpoint(AttachedDatabase &db, const UndoBufferProperties &properties);

	//! Rollback
	ErrorData Rollback();
	//! Cleanup the undo buffer
	void Cleanup(VisibilityBound lowest_visibility_bound);

	bool ChangesMade();
	UndoBufferProperties GetUndoProperties();

	//! Pushes the undo entry of a delete (DELETE_TUPLE) or of an update's markers (UPDATE_ROWS) in one vector
	void PushWrite(UndoFlags type, DuckTableEntry &table_entry, RowVersionManager &info, idx_t vector_idx,
	               idx_t base_row, const WrittenRows &written);
	void PushSequenceUsage(SequenceCatalogEntry &entry, const SequenceData &data);
	void PushAppend(DuckTableEntry &table_entry, idx_t row_start, idx_t row_count);
	UndoBufferReference CreateUpdateInfo(DuckTableEntry &table_entry, idx_t type_size, idx_t capacity,
	                                     idx_t value_capacity, idx_t row_group_start);

	DuckTransactionManager &GetTransactionManager();
	bool IsDuckTransaction() const override {
		return true;
	}
	SnapshotView GetSnapshotView() const override;
	transaction_t GetTransactionId() const {
		return view.transaction_id;
	}

	unique_ptr<StorageLockKey> TryGetCheckpointLock();

	void SetIsCheckpointTransaction() {
		is_checkpoint_transaction = true;
	}

private:
	void ReleaseVacuumLock();
	//! Unregisters the running statement as a reader of the tables it read
	void ReleaseRowGroups(bool transaction_ends);

	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;
	//! The set of uncommitted appends for the transaction
	unique_ptr<LocalStorage> storage;
	//! Lock that prevents checkpoints from starting
	unique_ptr<StorageLockKey> checkpoint_lock;
	//! Lock for taking and releasing the vacuum lock
	mutex vacuum_lock_mutex;
	//! Lock that prevents vacuums from starting
	unique_ptr<StorageLockKey> vacuum_lock;
	//! Whether the vacuum lock is released when the transaction commits or rolls back, not when it is destroyed
	atomic<bool> transaction_holds_vacuum_lock;
	//! Lock for the tables read
	mutex read_tables_lock;
	//! The tables read by the running statement, with the row id version it reads each of them at
	reference_map_t<RowGroupCollection, pair<shared_ptr<RowGroupCollection>, idx_t>> read_tables;
	//! Whether tables are read from the live row groups only: the transaction is ending
	bool reads_live_row_groups = false;
	//! Lock for the row groups and latches below
	mutex row_group_write_lock;
	//! The row groups this transaction wrote deletes or update markers to
	reference_map_t<RowGroupVersionInfo, shared_ptr<RowGroupVersionInfo>> written_row_groups;
	//! The checkpoint that has to be durable before this transaction commits
	shared_ptr<VacuumSealLatch> seal_latch;
	//! Lock for accessing sequence_usage
	mutex sequence_lock;
	//! Map of all sequences that were used during the transaction and the value they had in this transaction
	reference_map_t<SequenceCatalogEntry, reference<SequenceValue>> sequence_usage;
	//! Flag to prevent auto-checkpointing inside a checkpoint transaction.
	bool is_checkpoint_transaction = false;
};

} // namespace duckdb
