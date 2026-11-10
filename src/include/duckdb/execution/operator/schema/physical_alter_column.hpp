//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_alter_column.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class PhysicalAlter;
struct AlterInfo;

//! Computes the column of an ADD COLUMN or ALTER TYPE whose table the PhysicalAlter below built, then installs it
class PhysicalAlterColumn : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::ALTER;

public:
	PhysicalAlterColumn(PhysicalPlan &physical_plan, PhysicalAlter &alter, idx_t estimated_cardinality);

	//! The ALTER of the PhysicalAlter below, which leaves its altered entry to this operator
	AlterInfo &info;

public:
	//! Gives up on an altered entry that was not installed
	static void DiscardAlteredEntry(ClientContext &context, AlterInfo &info);

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
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
};

} // namespace duckdb
