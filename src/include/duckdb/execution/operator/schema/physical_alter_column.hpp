//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_alter_column.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

//! PhysicalAlterColumn computes the column of an ALTER TABLE of a DuckDB table (ADD COLUMN, ALTER TYPE, and the struct
//! field changes). The PhysicalAlter below it creates the new table version but leaves its column to this operator,
//! which computes it row group by row group on the tasks of an event from its Finalize.
class PhysicalAlterColumn : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::ALTER;

public:
	PhysicalAlterColumn(PhysicalPlan &physical_plan, QualifiedName table, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::ALTER, {LogicalType::BOOLEAN}, estimated_cardinality),
	      table(std::move(table)) {
	}

	//! The altered table
	QualifiedName table;

public:
	string GetName() const override {
		return "ALTER_COLUMN";
	}

	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
};

} // namespace duckdb
