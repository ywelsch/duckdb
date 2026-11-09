//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_alter_column.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"

namespace duckdb {

//! PhysicalAlterColumn represents an ALTER TABLE that computes a column of a DuckDB table (ADD COLUMN, ALTER TYPE, and
//! the struct field changes). The ALTER creates the new table version, and the column is then computed row group by
//! row group on the executor's tasks.
class PhysicalAlterColumn : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::ALTER;

public:
	PhysicalAlterColumn(PhysicalPlan &physical_plan, unique_ptr<AlterInfo> info, idx_t estimated_cardinality);

	unique_ptr<AlterInfo> info;

public:
	//! Whether the ALTER computes a column of a table in the given catalog
	static bool RewritesColumn(ClientContext &context, const AlterInfo &info);

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
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
