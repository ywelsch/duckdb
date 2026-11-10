#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/operator/logical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_alter_column.hpp"

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
		// the ALTER leaves computing the column, and installing the altered table, to the operator above it
		auto &alter = Make<PhysicalAlter>(std::move(op.info), op.estimated_cardinality, true);
		return Make<PhysicalAlterColumn>(alter.Cast<PhysicalAlter>(), op.estimated_cardinality);
	}
	return Make<PhysicalAlter>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
