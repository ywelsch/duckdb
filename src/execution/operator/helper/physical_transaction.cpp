#include "duckdb/execution/operator/helper/physical_transaction.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/callback_async_task.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

class TransactionGlobalSourceState : public GlobalSourceState {
public:
	bool commit_scheduled = false;
};

unique_ptr<GlobalSourceState> PhysicalTransaction::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TransactionGlobalSourceState>();
}

SourceResultType PhysicalTransaction::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<TransactionGlobalSourceState>();
	if (state.commit_scheduled) {
		// the commit ran on the async pool, and an error it threw has failed the query
		return SourceResultType::FINISHED;
	}
	auto &client = context.client;
	if (info->type != TransactionType::COMMIT || !input.interrupt_state.CanCallback() || !context.pipeline) {
		client.RunTransactionStatementInternal(*info);
		return SourceResultType::FINISHED;
	}
	// a COMMIT can wait for a WAL fsync: run it as blocking I/O on the async pool rather than on this thread
	state.commit_scheduled = true;
	client.SuppressInterrupts();
	vector<unique_ptr<AsyncTask>> tasks;
	tasks.push_back(make_uniq<CallbackAsyncTask>([&client, this]() { client.RunTransactionStatementInternal(*info); }));
	AsyncResult commit(std::move(tasks), TaskSchedulerType::ASYNC);
	commit.ScheduleTasks(input.interrupt_state, context.pipeline->executor);
	return SourceResultType::BLOCKED;
}

} // namespace duckdb
