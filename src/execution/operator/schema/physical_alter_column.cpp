#include "duckdb/execution/operator/schema/physical_alter_column.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_rewrite.hpp"

namespace duckdb {

PhysicalAlterColumn::PhysicalAlterColumn(PhysicalPlan &physical_plan, unique_ptr<AlterInfo> info_p,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::ALTER, {LogicalType::BOOLEAN}, estimated_cardinality),
      info(std::move(info_p)) {
	// computing the column is left to this operator
	info->defer_column_rewrite = true;
}

bool PhysicalAlterColumn::RewritesColumn(ClientContext &context, const AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		return false;
	}
	switch (info.Cast<AlterTableInfo>().alter_table_type) {
	case AlterTableType::ADD_COLUMN:
	case AlterTableType::ALTER_COLUMN_TYPE:
	case AlterTableType::ADD_FIELD:
	case AlterTableType::REMOVE_FIELD:
	case AlterTableType::RENAME_FIELD:
		break;
	default:
		return false;
	}
	auto catalog = Catalog::GetCatalogEntry(context, info.GetQualifiedName().Catalog());
	return catalog && catalog->IsDuckCatalog();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class AlterColumnGlobalState : public GlobalSinkState {
public:
	~AlterColumnGlobalState() override {
		if (table) {
			// a column that was not computed completely gives back the blocks it was written to so far
			table->AbortColumnRewrite();
		}
	}

	//! The table version whose column is computed
	shared_ptr<DataTable> table;
};

unique_ptr<GlobalSinkState> PhysicalAlterColumn::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<AlterColumnGlobalState>();
}

SinkResultType PhysicalAlterColumn::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	return SinkResultType::NEED_MORE_INPUT;
}

class AlterColumnEvent;

class AlterColumnTask : public ExecutorTask {
public:
	AlterColumnTask(ClientContext &client, shared_ptr<Event> event_p, const PhysicalOperator &op,
	                AlterColumnEvent &alter_event)
	    : ExecutorTask(client, std::move(event_p), op), client(client), alter_event(alter_event) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

	string TaskType() const override {
		return "AlterColumnTask";
	}

private:
	ClientContext &client;
	AlterColumnEvent &alter_event;
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
			tasks.push_back(make_uniq<AlterColumnTask>(context, shared_from_this(), op, *this));
		}
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		table_entry.GetStorage().FinishColumnRewrite(context, table_entry);
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
	if (client.GetContext().IsInterrupted()) {
		throw InterruptException();
	}
	if (alter_event.RewriteNextRowGroup()) {
		return TaskExecutionResult::TASK_NOT_FINISHED;
	}
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

SinkFinalizeType PhysicalAlterColumn::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<AlterColumnGlobalState>();
	auto &catalog = Catalog::GetCatalog(context, info->GetQualifiedName().Catalog());
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
	gstate.table = table->GetStorage().shared_from_this();
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
