//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/aggregate_object.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

class BoundAggregateExpression;
class BoundWindowExpression;

struct FunctionDataWrapper {
	explicit FunctionDataWrapper(unique_ptr<FunctionData> function_data_p) : function_data(std::move(function_data_p)) {
	}

	unique_ptr<FunctionData> function_data;
};

struct AggregateObject { // NOLINT: work-around bug in clang-tidy
	AggregateObject(BoundAggregateFunction function, FunctionData *bind_data, idx_t child_count, idx_t payload_size,
	                AggregateType aggr_type, PhysicalType return_type, optional_ptr<const Expression> filter = nullptr);
	explicit AggregateObject(BoundAggregateExpression &aggr);
	explicit AggregateObject(BoundAggregateExpression *aggr);
	explicit AggregateObject(const BoundWindowExpression &window);

	FunctionData *GetFunctionData() const {
		return bind_data_wrapper ? bind_data_wrapper->function_data.get() : nullptr;
	}

	BoundAggregateFunction function;
	shared_ptr<FunctionDataWrapper> bind_data_wrapper;
	idx_t child_count;
	idx_t payload_size;
	AggregateType aggr_type;
	PhysicalType return_type;
	optional_ptr<const Expression> filter = nullptr;

public:
	bool IsDistinct() const {
		return aggr_type == AggregateType::DISTINCT;
	}
	static vector<AggregateObject> CreateAggregateObjects(const vector<BoundAggregateExpression *> &bindings);
};

struct AggregateFilterData {
	AggregateFilterData(ClientContext &context, const Expression &filter_expr, DataChunk &filtered_payload);

	//! Filters the payload and slices the columns [column_start, column_start + column_count) into filtered_payload
	idx_t ApplyFilter(DataChunk &payload, idx_t column_start, idx_t column_count);
	//! Filters the payload and slices the given columns into filtered_payload
	idx_t ApplyFilter(DataChunk &payload, const vector<idx_t> &columns);

	ExpressionExecutor filter_executor;
	//! Shared by all filters of a set: only the columns sliced by the latest ApplyFilter are valid
	DataChunk &filtered_payload;
	SelectionVector true_sel;
};

struct AggregateFilterDataSet {
	AggregateFilterDataSet();

	//! One filtered payload for the whole set, as the payload holds the inputs of every aggregate
	unique_ptr<DataChunk> filtered_payload;
	vector<unique_ptr<AggregateFilterData>> filter_data;

public:
	void Initialize(ClientContext &context, const vector<AggregateObject> &aggregates,
	                const vector<LogicalType> &payload_types);

	AggregateFilterData &GetFilterData(idx_t aggr_idx);
};

} // namespace duckdb
