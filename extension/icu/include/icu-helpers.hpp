//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "unicode/timezone.h"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

struct ICUHelpers {
	//! Gets a time zone - throws an error if the timezone is not found
	static unique_ptr<icu::TimeZone> GetTimeZone(string &tz_str, string *error_message = nullptr);

	static TimestampComponents GetComponents(timestamp_tz_t ts, icu::Calendar *calendar);

	static timestamp_t ToTimestamp(TimestampComponents data);
};

} // namespace duckdb
