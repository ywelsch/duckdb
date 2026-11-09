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
//! which then computes it as a parallel source, one row group per call.
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
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}
	OrderPreservationType SourceOrder() const override {
		// the operator outputs no rows, so the result does not need to keep them in order
		return OrderPreservationType::NO_ORDER;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
};

} // namespace duckdb
