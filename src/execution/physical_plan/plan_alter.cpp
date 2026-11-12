#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/operator/logical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_alter_add_column.hpp"
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

//! Whether an expression of the plan is volatile
static bool HasVolatileExpression(LogicalOperator &op) {
	for (auto &expression : op.expressions) {
		if (expression->IsVolatile()) {
			return true;
		}
	}
	for (auto &child : op.children) {
		if (HasVolatileExpression(*child)) {
			return true;
		}
	}
	return false;
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalAlter &op) {
	if (!op.children.empty()) {
		// the child computes the value of the added column for every row slot of the table
		auto is_volatile = HasVolatileExpression(*op.children[0]);
		auto &child = CreatePlan(*op.children[0]);
		auto &alter = Make<PhysicalAlterAddColumn>(std::move(op.info), is_volatile, op.estimated_cardinality);
		alter.children.push_back(child);
		return alter;
	}
	if (ComputesColumn(context, *op.info)) {
		// the ALTER leaves computing the column to the sink above it
		auto table = op.info->GetQualifiedName();
		auto &alter = Make<PhysicalAlter>(std::move(op.info), op.estimated_cardinality, true);
		auto &alter_column = Make<PhysicalAlterColumn>(std::move(table), op.estimated_cardinality);
		alter_column.children.push_back(alter);
		return alter_column;
	}
	return Make<PhysicalAlter>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
