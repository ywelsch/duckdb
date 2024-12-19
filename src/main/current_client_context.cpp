#include "duckdb/main/current_client_context.hpp"

namespace duckdb {

optional_ptr<ClientContext> thread_local CurrentClientContext::current_context {};

optional_ptr<DatabaseInstance> thread_local CurrentClientContext::current_database_instance {};

} // namespace duckdb
