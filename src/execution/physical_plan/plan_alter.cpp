#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_alter_column.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalAlter &op) {
	if (PhysicalAlterColumn::RewritesColumn(context, *op.info)) {
		// a sink, so that the column can be computed by the tasks of an event from its Finalize
		auto &dummy_scan = Make<PhysicalDummyScan>(vector<LogicalType> {LogicalType::BOOLEAN}, 1);
		auto &alter = Make<PhysicalAlterColumn>(std::move(op.info), op.estimated_cardinality);
		alter.children.push_back(dummy_scan);
		return alter;
	}
	return Make<PhysicalAlter>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
