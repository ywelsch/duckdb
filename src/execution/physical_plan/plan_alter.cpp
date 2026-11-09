#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/operator/logical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_alter_column.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

namespace duckdb {

//! Whether the ALTER computes a column of a DuckDB table, which PhysicalAlterColumn does on the executor's tasks
static bool ComputesColumn(ClientContext &context, const AlterInfo &info) {
	if (!info.RewritesColumn()) {
		return false;
	}
	auto catalog = Catalog::GetCatalogEntry(context, info.GetQualifiedName().Catalog());
	return catalog && catalog->IsDuckCatalog();
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalAlter &op) {
	if (ComputesColumn(context, *op.info)) {
		// a sink, so that the column can be computed by the tasks of an event from its Finalize
		auto &dummy_scan = Make<PhysicalDummyScan>(vector<LogicalType> {LogicalType::BOOLEAN}, 1);
		auto &alter = Make<PhysicalAlterColumn>(std::move(op.info), op.estimated_cardinality);
		alter.children.push_back(dummy_scan);
		return alter;
	}
	return Make<PhysicalAlter>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
