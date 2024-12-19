//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/current_client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;

class CurrentClientContext {
public:
	struct ScopeGuard {
		explicit ScopeGuard(ClientContext& context) : ScopeGuard(&context) {

		}

		explicit ScopeGuard(DatabaseInstance& database_instance) : ScopeGuard(&database_instance) {

		}

		explicit ScopeGuard(ClientContext* context) {
			previous_context = current_context;
			previous_database_instance = current_database_instance;
			current_context = context;
			current_database_instance = context ? context->db.get() : nullptr;
		}

		explicit ScopeGuard(DatabaseInstance* database_instance) {
			previous_context = current_context;
			previous_database_instance = current_database_instance;
			current_context = nullptr;
			current_database_instance = database_instance;
		}

		~ScopeGuard() {
			current_context = previous_context;
			current_database_instance = previous_database_instance;
		}

		optional_ptr<ClientContext> previous_context;
		optional_ptr<DatabaseInstance> previous_database_instance;
	};

	static optional_ptr<ClientContext> CurrentContext() {
		return optional_ptr<ClientContext>(current_context);
	}

	static optional_ptr<DatabaseInstance> CurrentDatabase() {
		return optional_ptr<DatabaseInstance>(current_database_instance);
	}

	private:
	// the currently active client context for the current thread
	static thread_local optional_ptr<ClientContext> current_context;
	// the currently active database instace for the current thread
	static thread_local optional_ptr<DatabaseInstance> current_database_instance;
};

} // namespace duckdb
