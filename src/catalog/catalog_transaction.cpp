#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

CatalogTransaction::CatalogTransaction(Catalog &catalog, ClientContext &context) {
	auto &transaction = Transaction::Get(context, catalog);
	this->db = &DatabaseInstance::GetDatabase(context);
	if (!transaction.IsDuckTransaction()) {
		this->transaction_id = TransactionId::None();
		this->snapshot_bound = SnapshotBound::IncludingUncommitted();
	} else {
		auto &dtransaction = transaction.Cast<DuckTransaction>();
		this->transaction_id = dtransaction.transaction_id;
		this->snapshot_bound = SnapshotBound::Before(dtransaction.start_time);
	}
	this->transaction = &transaction;
	this->context = &context;
}

CatalogTransaction::CatalogTransaction(DatabaseInstance &db, TransactionId transaction_id_p,
                                       SnapshotBound snapshot_bound_p)
    : db(&db), context(nullptr), transaction(nullptr), transaction_id(transaction_id_p),
      snapshot_bound(snapshot_bound_p) {
}

ClientContext &CatalogTransaction::GetContext() {
	if (!context) {
		throw InternalException("Attempting to get a context in a CatalogTransaction without a context");
	}
	return *context;
}

CatalogTransaction CatalogTransaction::GetSystemCatalogTransaction(ClientContext &context) {
	return CatalogTransaction(Catalog::GetSystemCatalog(context), context);
}

CatalogTransaction CatalogTransaction::GetSystemTransaction(DatabaseInstance &db) {
	// the bound is one past the bootstrap stamp: the system transaction sees the bootstrap entries
	// and nothing else, since the timestamp counter starts above them
	// NOTE: the bootstrap entries are stamped with the bootstrap stamp itself, which sits in the
	// commit domain - so the system transaction's *id* is a value below the split
	return CatalogTransaction(db, TransactionId(SYSTEM_TRANSACTION_TIMESTAMP.GetIndex()),
	                          SnapshotBound::Through(SYSTEM_TRANSACTION_TIMESTAMP));
}

} // namespace duckdb
