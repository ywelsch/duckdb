#include "duckdb/execution/operator/schema/physical_alter_column.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_rewrite.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalAlterColumn::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	return SinkResultType::NEED_MORE_INPUT;
}

class AlterColumnTask : public ExecutorTask {
public:
	AlterColumnTask(ClientContext &client, shared_ptr<Event> event_p, const PhysicalOperator &op)
	    : ExecutorTask(client, std::move(event_p), op), client(client) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

	string TaskType() const override {
		return "AlterColumnTask";
	}

private:
	ClientContext &client;
};

class AlterColumnEvent : public BasePipelineEvent {
public:
	AlterColumnEvent(Pipeline &pipeline_p, ClientContext &context, const PhysicalOperator &op,
	                 DuckTableEntry &table_entry)
	    : BasePipelineEvent(pipeline_p), context(context), op(op), table_entry(table_entry),
	      rewrite(*table_entry.GetStorage().GetColumnRewrite()), next_row_group(0) {
	}

public:
	void Schedule() override {
		// a volatile expression is computed in row order, by a single task
		idx_t task_count = 1;
		if (!rewrite.IsVolatile()) {
			auto thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
			task_count = MaxValue<idx_t>(MinValue<idx_t>(thread_count, rewrite.RowGroupCount()), 1);
		}
		vector<shared_ptr<Task>> tasks;
		for (idx_t i = 0; i < task_count; i++) {
			tasks.push_back(make_uniq<AlterColumnTask>(context, shared_from_this(), op));
		}
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		auto &storage = table_entry.GetStorage();
		storage.FinishColumnRewrite(context);
		// the catalog entry was created before the transaction-local rows moved over to its storage
		LocalStorage::Get(context, storage.db).SetTableEntry(table_entry);
	}

	//! Rewrites the next row group, returns false if all of them were claimed
	bool RewriteNextRowGroup() {
		auto index = next_row_group++;
		if (index >= rewrite.RowGroupCount()) {
			return false;
		}
		rewrite.RewriteRowGroup(index);
		return true;
	}

private:
	ClientContext &context;
	const PhysicalOperator &op;
	DuckTableEntry &table_entry;
	ColumnRewrite &rewrite;
	atomic<idx_t> next_row_group;
};

TaskExecutionResult AlterColumnTask::ExecuteTask(TaskExecutionMode mode) {
	// one row group per step, so that an interrupted query stops between row groups
	client.BlockingInterruptCheck();
	if (event->Cast<AlterColumnEvent>().RewriteNextRowGroup()) {
		return TaskExecutionResult::TASK_NOT_FINISHED;
	}
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

SinkFinalizeType PhysicalAlterColumn::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &catalog = Catalog::GetCatalog(context, info->GetQualifiedName().Catalog());
	// the ALTER leaves computing the column to this operator
	info->defer_column_rewrite = true;
	catalog.Alter(context, *info);

	// the new table version holds the column the ALTER left to us, unless it had nothing to do
	EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, info->GetQualifiedName());
	auto entry = Catalog::GetEntry(context, lookup_info, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		return SinkFinalizeType::READY;
	}
	auto table = entry->Cast<TableCatalogEntry>().TryGetDuckTableEntry();
	if (!table || !table->GetStorage().GetColumnRewrite()) {
		return SinkFinalizeType::READY;
	}
	event.InsertEvent(make_shared_ptr<AlterColumnEvent>(pipeline, context, *this, *table));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAlterColumn::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

} // namespace duckdb
