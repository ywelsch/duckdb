#include "duckdb/storage/table/partition_key.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

const char *PartitionTransformName(PartitionTransform transform) {
	switch (transform) {
	case PartitionTransform::YEAR:
		return "year";
	case PartitionTransform::MONTH:
		return "month";
	case PartitionTransform::DAY:
		return "day";
	case PartitionTransform::HOUR:
		return "hour";
	default:
		return nullptr;
	}
}

bool TryParsePartitionTransform(const Identifier &name, PartitionTransform &result) {
	if (StringUtil::CIEquals(name.GetIdentifierName(), "year")) {
		result = PartitionTransform::YEAR;
	} else if (StringUtil::CIEquals(name.GetIdentifierName(), "month")) {
		result = PartitionTransform::MONTH;
	} else if (StringUtil::CIEquals(name.GetIdentifierName(), "day")) {
		result = PartitionTransform::DAY;
	} else if (StringUtil::CIEquals(name.GetIdentifierName(), "hour")) {
		result = PartitionTransform::HOUR;
	} else {
		return false;
	}
	return true;
}

bool PartitionTransformSupportsType(PartitionTransform transform, const LogicalType &type) {
	switch (transform) {
	case PartitionTransform::IDENTITY:
		return true;
	case PartitionTransform::HOUR:
		// a date has no time of day
		return type.id() == LogicalTypeId::TIMESTAMP;
	default:
		return type.id() == LogicalTypeId::DATE || type.id() == LogicalTypeId::TIMESTAMP;
	}
}

LogicalType PartitionTransformReturnType(PartitionTransform transform, const LogicalType &input) {
	if (transform == PartitionTransform::IDENTITY) {
		return input;
	}
	return LogicalType::BIGINT;
}

//! Extract the requested part of a date. Dates have no time of day, so HOUR never reaches here.
static int64_t ExtractDatePart(PartitionTransform transform, date_t date) {
	switch (transform) {
	case PartitionTransform::YEAR:
		return Date::ExtractYear(date);
	case PartitionTransform::MONTH:
		return Date::ExtractMonth(date);
	case PartitionTransform::DAY:
		return Date::ExtractDay(date);
	default:
		throw InternalException("Unsupported partition transform for DATE");
	}
}

static int64_t ExtractTimestampPart(PartitionTransform transform, timestamp_t ts) {
	if (transform == PartitionTransform::HOUR) {
		return Timestamp::GetTime(ts).value / Interval::MICROS_PER_HOUR;
	}
	return ExtractDatePart(transform, Timestamp::GetDate(ts));
}

void ApplyPartitionTransform(PartitionTransform transform, Vector &input, Vector &result, idx_t count) {
	if (transform == PartitionTransform::IDENTITY) {
		result.Reference(input);
		return;
	}
	switch (input.GetType().id()) {
	case LogicalTypeId::DATE:
		UnaryExecutor::Execute<date_t, int64_t>(input, result, count,
		                                        [&](date_t date) { return ExtractDatePart(transform, date); });
		break;
	case LogicalTypeId::TIMESTAMP:
		UnaryExecutor::Execute<timestamp_t, int64_t>(
		    input, result, count, [&](timestamp_t ts) { return ExtractTimestampPart(transform, ts); });
		break;
	default:
		throw InternalException("Unsupported column type for partition transform");
	}
}

Value ApplyPartitionTransform(PartitionTransform transform, const Value &input) {
	if (transform == PartitionTransform::IDENTITY) {
		return input;
	}
	if (input.IsNull()) {
		return Value(LogicalType::BIGINT);
	}
	switch (input.type().id()) {
	case LogicalTypeId::DATE:
		return Value::BIGINT(ExtractDatePart(transform, input.GetValue<date_t>()));
	case LogicalTypeId::TIMESTAMP:
		return Value::BIGINT(ExtractTimestampPart(transform, input.GetValue<timestamp_t>()));
	default:
		throw InternalException("Unsupported value type for partition transform");
	}
}

//! How coarse a transform is, for deciding whether the units above a periodic transform are pinned. IDENTITY pins
//! everything about its own column, so it is treated as the coarsest.
static idx_t TransformGranularity(PartitionTransform transform) {
	switch (transform) {
	case PartitionTransform::IDENTITY:
		return 0;
	case PartitionTransform::YEAR:
		return 1;
	case PartitionTransform::MONTH:
		return 2;
	case PartitionTransform::DAY:
		return 3;
	default:
		return 4;
	}
}

static bool TransformIsPeriodic(PartitionTransform transform) {
	return transform == PartitionTransform::MONTH || transform == PartitionTransform::DAY ||
	       transform == PartitionTransform::HOUR;
}

bool PartitionKeysAreDerivable(const vector<PartitionKey> &keys) {
	for (idx_t i = 0; i < keys.size(); i++) {
		if (!TransformIsPeriodic(keys[i].transform)) {
			continue;
		}
		// every coarser unit of the same column has to appear as an earlier key, otherwise equal values at the
		// bounds do not imply that the rows in between agree
		auto granularity = TransformGranularity(keys[i].transform);
		for (idx_t coarser = 1; coarser < granularity; coarser++) {
			bool found = false;
			for (idx_t j = 0; j < i; j++) {
				if (keys[j].column == keys[i].column && TransformGranularity(keys[j].transform) == coarser) {
					found = true;
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
	}
	return true;
}

} // namespace duckdb
