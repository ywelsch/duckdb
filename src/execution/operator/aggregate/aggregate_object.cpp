#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

AggregateObject::AggregateObject(BoundAggregateFunction function, FunctionData *bind_data, idx_t child_count,
                                 idx_t payload_size, AggregateType aggr_type, PhysicalType return_type,
                                 optional_ptr<const Expression> filter)
    : function(std::move(function)),
      bind_data_wrapper(bind_data ? make_shared_ptr<FunctionDataWrapper>(bind_data->Copy()) : nullptr),
      child_count(child_count), payload_size(payload_size), aggr_type(aggr_type), return_type(return_type),
      filter(filter) {
}

AggregateObject::AggregateObject(BoundAggregateExpression &aggr)
    : AggregateObject(aggr.Function(), aggr.BindInfoMutable().get(), aggr.GetChildren().size(),
                      AlignValue(aggr.Function().GetStateSize(aggr.BindInfoMutable().get())), aggr.GetAggregateType(),
                      aggr.GetReturnType().InternalType(), aggr.GetFilter().get()) {
}

AggregateObject::AggregateObject(BoundAggregateExpression *aggr) : AggregateObject(*aggr) {
}

AggregateObject::AggregateObject(const BoundWindowExpression &window)
    : AggregateObject(*window.AggregateFunction(), window.BindInfo().get(), window.GetChildren().size(),
                      AlignValue(window.AggregateFunction()->GetStateSize(window.BindInfo().get())),
                      window.Distinct() ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT,
                      window.GetReturnType().InternalType(), window.Filter().get()) {
}

vector<AggregateObject> AggregateObject::CreateAggregateObjects(const vector<BoundAggregateExpression *> &bindings) {
	vector<AggregateObject> aggregates;
	aggregates.reserve(bindings.size());
	for (auto &binding : bindings) {
		aggregates.emplace_back(binding);
	}
	return aggregates;
}

AggregateFilterData::AggregateFilterData(ClientContext &context, const Expression &filter_expr,
                                         DataChunk &filtered_payload)
    : filter_executor(context, &filter_expr), filtered_payload(filtered_payload), true_sel(STANDARD_VECTOR_SIZE) {
}

void AggregateFilterData::DropStaleColumns(DataChunk &chunk) {
#ifdef DEBUG
	for (auto &col : chunk.data) {
		col.SetBuffer(nullptr);
	}
#endif
}

idx_t AggregateFilterData::ApplyFilter(DataChunk &payload, idx_t column_start, idx_t column_count) {
	D_ASSERT(payload.ColumnCount() == filtered_payload.ColumnCount());
	auto count = filter_executor.SelectExpression(payload, true_sel);
	DropStaleColumns(filtered_payload);
	for (idx_t col_idx = column_start; col_idx < column_start + column_count; col_idx++) {
		filtered_payload.data[col_idx].Slice(payload.data[col_idx], true_sel, count);
	}
	filtered_payload.SetCardinalityUnsafe(count);
	return count;
}

idx_t AggregateFilterData::ApplyFilter(DataChunk &payload, const vector<idx_t> &columns) {
	D_ASSERT(payload.ColumnCount() == filtered_payload.ColumnCount());
	auto count = filter_executor.SelectExpression(payload, true_sel);
	DropStaleColumns(filtered_payload);
	for (auto col_idx : columns) {
		filtered_payload.data[col_idx].Slice(payload.data[col_idx], true_sel, count);
	}
	filtered_payload.SetCardinalityUnsafe(count);
	return count;
}

AggregateFilterDataSet::AggregateFilterDataSet() {
}

void AggregateFilterDataSet::Initialize(ClientContext &context, const vector<AggregateObject> &aggregates,
                                        const vector<LogicalType> &payload_types) {
	bool has_filters = false;
	for (auto &aggregate : aggregates) {
		if (aggregate.filter) {
			has_filters = true;
			break;
		}
	}
	if (!has_filters) {
		// no filters: nothing to do
		return;
	}
	// the filtered payload only references slices of the payload, so it needs no buffers of its own
	filtered_payload = make_uniq<DataChunk>();
	filtered_payload->InitializeEmpty(payload_types);
	filter_data.resize(aggregates.size());
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggr = aggregates[aggr_idx];
		if (aggr.filter) {
			filter_data[aggr_idx] = make_uniq<AggregateFilterData>(context, *aggr.filter, *filtered_payload);
		}
	}
}

AggregateFilterData &AggregateFilterDataSet::GetFilterData(idx_t aggr_idx) {
	D_ASSERT(aggr_idx < filter_data.size());
	D_ASSERT(filter_data[aggr_idx]);
	return *filter_data[aggr_idx];
}
} // namespace duckdb
