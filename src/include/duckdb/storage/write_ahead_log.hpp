//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/write_ahead_log.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/block.hpp"

#include <condition_variable>

namespace duckdb {

struct AlterInfo;

class AttachedDatabase;
class Catalog;
class DatabaseInstance;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class ScalarMacroCatalogEntry;
class ViewCatalogEntry;
class TriggerCatalogEntry;
class TypeCatalogEntry;
class TableCatalogEntry;
class Transaction;
class TransactionManager;
class WriteAheadLogDeserializer;
struct PersistentCollectionData;

enum class WALInitState { NO_WAL, UNINITIALIZED, UNINITIALIZED_REQUIRES_TRUNCATE, INITIALIZED };

//! The WriteAheadLog (WAL) is a log that is used to provide durability. Prior
//! to committing a transaction it writes the changes the transaction made to
//! the database to the log, which can then be replayed upon startup in case the
//! server crashes or is shut down.
class WriteAheadLog {
public:
	//! Initialize the WAL in the specified directory
	explicit WriteAheadLog(StorageManager &storage_manager, const string &wal_path, idx_t wal_size = 0ULL,
	                       WALInitState state = WALInitState::NO_WAL,
	                       optional_idx checkpoint_iteration = optional_idx());
	virtual ~WriteAheadLog();

public:
	//! Replay and initialize the WAL, QueryContext is passed for metric collection purposes only!!
	static unique_ptr<WriteAheadLog> Replay(QueryContext context, StorageManager &storage_manager,
	                                        const string &wal_path);

	AttachedDatabase &GetDatabase();
	StorageManager &GetStorageManager();

	const string &GetPath() const {
		return wal_path;
	}
	//! Gets the total bytes written to the WAL since startup
	idx_t GetTotalWritten() const;

	//! A WAL is initialized, if a writer to a file exists.
	bool Initialized() const;
	//! Initializes the file of the WAL by creating the file writer.
	BufferedFileWriter &Initialize();

	//! Write the WAL header.
	void WriteHeader();

	virtual void WriteCreateTable(const TableCatalogEntry &entry);
	void WriteDropTable(const TableCatalogEntry &entry);

	void WriteCreateSchema(const SchemaCatalogEntry &entry);
	void WriteDropSchema(const SchemaCatalogEntry &entry);

	void WriteCreateView(const ViewCatalogEntry &entry);
	void WriteDropView(const ViewCatalogEntry &entry);

	void WriteCreateSequence(const SequenceCatalogEntry &entry);
	void WriteDropSequence(const SequenceCatalogEntry &entry);
	void WriteSequenceValue(SequenceValue val);

	void WriteCreateMacro(const ScalarMacroCatalogEntry &entry);
	void WriteDropMacro(const ScalarMacroCatalogEntry &entry);

	void WriteCreateTableMacro(const TableMacroCatalogEntry &entry);
	void WriteDropTableMacro(const TableMacroCatalogEntry &entry);

	void WriteCreateIndex(const IndexCatalogEntry &entry);
	void WriteDropIndex(const IndexCatalogEntry &entry);

	void WriteCreateType(const TypeCatalogEntry &entry);
	void WriteDropType(const TypeCatalogEntry &entry);

	void WriteCreateTrigger(const TriggerCatalogEntry &entry);
	void WriteDropTrigger(const TriggerCatalogEntry &entry);
	//! Sets the table used for subsequent insert/delete/update commands
	void WriteSetTable(const Identifier &schema, const Identifier &table);

	void WriteAlter(CatalogEntry &entry, const AlterInfo &info);

	void WriteInsert(DataChunk &chunk);
	void WriteRowGroupData(const PersistentCollectionData &data);
	void WriteDelete(DataChunk &chunk);
	//! Write a single (sub-) column update to the WAL. Chunk must be a pair of (COL, ROW_ID).
	//! The column_path vector is a *path* towards a column within the table
	//! i.e. if we have a table with a single column S STRUCT(A INT, B INT)
	//! and we update the validity mask of "S.B"
	//! the column path is:
	//! 0 (first column of table)
	//! -> 1 (second subcolumn of struct)
	//! -> 0 (first subcolumn of INT)
	void WriteUpdate(DataChunk &chunk, const vector<column_t> &column_path);

	//! Truncate the WAL to a previous size, and clear anything currently set in the writer.
	//! Used during RevertCommit.
	void Truncate(idx_t size);
	//! Write a WAL_FLUSH marker, push all buffered data to the operating system and fsync (fully durable).
	void Flush();
	//! Only writes a WAL_FLUSH marker and pushes the buffered bytes to the OS page cache, WITHOUT issuing an fsync.
	//! Returns the WAL offset that a subsequent GroupSync() call must reach to make the data durable. Callers must be
	//! serialized (the WAL append lock on the deferred path, the exclusive WAL lock otherwise) so the published
	//! flushed_offset stays monotonic.
	//! If requires_block_sync is set, the commit completed by this marker references optimistically written
	//! row group data: the database file is fsynced (once, batched) by an fsync lane BEFORE any WAL fsync
	//! that makes this marker durable, so that the referenced blocks are always durable first.
	idx_t WriteFlushMarker(bool requires_block_sync = false);
	//! Group commit: make every WAL byte up to (at least) target_offset (as returned by WriteFlushMarker) durable.
	//! Safe to call without holding the WAL lock: a committer fsyncs itself - overlapping with fsyncs already in
	//! flight, up to the file system's declared sync parallelism - unless an in-flight fsync's target already covers
	//! its bytes, in which case it parks until durability advances. Returns only once durable_offset >= target_offset.
	void GroupSync(idx_t target_offset);
	unique_lock<mutex> LockFlush() {
		return unique_lock<mutex>(flush_lock);
	}
	//! Increment the WAL entry count, which is used for the auto-checkpoint threshold.
	void IncrementWALEntriesCount();
	void WriteCheckpoint(MetaBlockPointer meta_block);

protected:
	StorageManager &storage_manager;
	mutex wal_lock;
	unique_ptr<BufferedFileWriter> writer;
	string wal_path;
	atomic<WALInitState> init_state;
	optional_idx checkpoint_iteration;

	//! Serializes writes into the in-memory WAL buffer (entry appends, flush markers), truncation, header writes,
	//! and updates of flushed_offset - see LockFlush().
	//! mutable so the const GetTotalWritten() reader can serialize against concurrent buffer writes.
	mutable mutex flush_lock;

private:
	//! Park until sync_epoch differs from current (slow path only; fast paths never touch the mutex).
	void WaitSyncEpochChange(uint64_t current);
	//! Bump sync_epoch and wake every parked committer (called after durable_offset advances or the WAL is poisoned).
	void BumpSyncEpochNotify();

	//! Group commit fsync engine (adapted from the design in duckdb/duckdb#23655).
	//! Offsets are logical byte counters (BufferedFileWriter::GetTotalWritten). They only ever advance at COMPLETED
	//! flush markers, so they increase monotonically across committed markers: truncation of reverted entries removes
	//! only marker-less bytes (a reverted commit never completed its marker), which no published offset ever covered.
	//! An fsync therefore always covers every published offset at or below its snapshotted target, and no generation
	//! tracking around truncation is needed.
	//!
	//! Maximum number of concurrent fsyncs worth issuing on this WAL's storage, set at initialization from the file
	//! system's declared sync semantics (FileSystem::SyncParallelism): unbounded where a sync is a per-call round
	//! trip that overlaps (network file systems), 1 where a sync commits a shared journal and concurrent syncs only
	//! add cost (local file systems) - there the single stream's late-snapped targets batch every commit that arrived
	//! during the previous fsync.
	idx_t sync_lane_cap = 1;
	//! Number of fsyncs currently in flight (bounded by sync_lane_cap).
	atomic<idx_t> active_syncs {0};
	//! Raise-only maximum target of any in-flight (or completed) fsync: a committer whose bytes are already covered
	//! by an in-flight fsync parks instead of claiming a lane for a redundant fsync.
	atomic<idx_t> syncing_target {0};
	//! Parking word for committers waiting on durability: bumped on every durable_offset advance and on failure.
	atomic<uint64_t> sync_epoch {0};
	//! Terminal poison flag: after a failed fsync the OS may have dropped the failed dirty pages as clean, so a
	//! retried fsync could falsely report success for bytes that never reached disk - durability can no longer be
	//! promised for any pending offset. A checkpoint replaces the WAL (object and file), which resets this.
	atomic<bool> sync_failed {false};
	//! Guards only the parking of sync waiters; committers that are covered by a completed fsync, and committers
	//! issuing their own fsync, never take it.
	std::mutex sync_wait_mutex;
	std::condition_variable sync_wait_cv;
	//! Highest WAL byte offset known durable. Raise-only; stored by an fsyncer strictly AFTER its Sync() returns,
	//! acquire-loaded as the sole durable-before-ack predicate.
	atomic<idx_t> durable_offset {0};
	//! Highest WAL byte offset pushed to the page cache. Release-stored under flush_lock by WriteFlushMarker (callers
	//! are serialized, so it is monotonic), acquire-loaded by fsyncers as their target.
	atomic<idx_t> flushed_offset {0};
	//! Highest marker offset whose commit references optimistically written row group data.
	//! Updated in WriteFlushMarker BEFORE flushed_offset advances, so that any fsync lane whose target covers
	//! the marker observes the block sync requirement.
	atomic<idx_t> block_sync_pending_offset {0};
	//! Marker offset up to which the referenced row group blocks are known durable in the database file (raise-only).
	atomic<idx_t> block_synced_offset {0};
};

} // namespace duckdb
