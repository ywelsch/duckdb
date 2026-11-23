#include "duckdb/execution/operator/helper/physical_transaction.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/interrupt.hpp"

namespace duckdb {

SourceResultType PhysicalTransaction::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	optional_ptr<const InterruptState> interrupt_state;
	if (input.interrupt_state.CanCallback()) {
		interrupt_state = &input.interrupt_state;
	}
	if (!context.client.RunTransactionStatementInternal(*info, interrupt_state)) {
		return SourceResultType::BLOCKED;
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
