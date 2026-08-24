//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/partition_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

//! The transform applied to a column to obtain the value a table is partitioned by. This is a fixed vocabulary
//! rather than arbitrary expressions for two reasons: each of these can be evaluated without a ClientContext, which
//! the append path does not have, and their behaviour is known well enough to decide when a row group's partition
//! value can be derived from that column's min/max statistics.
//! The names and results follow DuckLake, so a partition specification means the same thing in both.
enum class PartitionTransform : uint8_t {
	//! the column value itself
	IDENTITY,
	//! the year of a date or timestamp, e.g. 2023
	YEAR,
	//! the month of a date or timestamp, 1..12
	MONTH,
	//! the day of a date or timestamp, 1..31
	DAY,
	//! the hour of a timestamp, 0..23
	HOUR
};

//! A column plus the transform applied to it. `column` is a logical column index on a TableCatalogEntry and a
//! storage index on a DataTableInfo.
struct PartitionKey {
	PartitionKey() : column(0), transform(PartitionTransform::IDENTITY) {
	}
	PartitionKey(idx_t column, PartitionTransform transform) : column(column), transform(transform) {
	}

	idx_t column;
	PartitionTransform transform;
};

//! The function name a transform is written as, or nullptr for IDENTITY
const char *PartitionTransformName(PartitionTransform transform);
//! Recognise a transform from a function name
bool TryParsePartitionTransform(const Identifier &name, PartitionTransform &result);
//! Whether a transform can be applied to a column of this type
bool PartitionTransformSupportsType(PartitionTransform transform, const LogicalType &type);
//! The type a transform produces for the given input type
LogicalType PartitionTransformReturnType(PartitionTransform transform, const LogicalType &input);

//! Apply a transform to every value of a vector. Used by the append path to compute partition values of a chunk.
void ApplyPartitionTransform(PartitionTransform transform, Vector &input, Vector &result, idx_t count);
//! Apply a transform to a single value. Used to derive a row group's partition value from its statistics.
Value ApplyPartitionTransform(PartitionTransform transform, const Value &input);

//! Whether the partition value of a row group can be derived by applying the transforms to the column's minimum and
//! maximum and checking that the results agree.
//!
//! IDENTITY and YEAR are order-preserving, so equal results at the bounds mean every row in between agrees too.
//! MONTH, DAY and HOUR are periodic - a row group spanning January 2023 to January 2024 has month 1 at both bounds
//! while holding every month in between - so they are only sound once every coarser unit is pinned to a single
//! value by an earlier key. That makes (year, month) and (year, month, day) derivable, while (month) or
//! (year, day) on their own are not.
bool PartitionKeysAreDerivable(const vector<PartitionKey> &keys);

} // namespace duckdb
