//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_alter_add_column.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
struct AlterInfo;

//! Adds a column to a DuckDB table, whose values its child computes for every row slot of a scan of the table
class PhysicalAlterAddColumn : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::ALTER;

public:
	PhysicalAlterAddColumn(PhysicalPlan &physical_plan, unique_ptr<AlterInfo> info, bool is_volatile,
	                       idx_t estimated_cardinality);

	unique_ptr<AlterInfo> info;
	//! Whether the values are volatile, so that a single thread computes them in row order
	bool is_volatile;

public:
	string GetName() const override {
		return "ALTER_ADD_COLUMN";
	}

	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return !is_volatile;
	}
};

} // namespace duckdb
