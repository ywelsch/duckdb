#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

using namespace duckdb;
using std::string;

namespace {

// A secret type that stands in for s3 and friends
struct GatedSecretType {
	static constexpr const char *NAME = "gated";

	static void Register(DatabaseInstance &db) {
		SecretType type;
		type.name = NAME;
		type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		type.default_provider = "config";
		SecretManager::Get(db).RegisterSecretType(type);
	}
};

// A secret with a catalog is invisible to path lookups that do not present that catalog, and outranks every global
// secret for lookups that do.
struct CatalogScopedSecretsFixture {
	CatalogScopedSecretsFixture() {
		DBConfig config;
		config.SetOptionByName("allow_persistent_secrets", Value::BOOLEAN(false));
		db = make_uniq<DuckDB>(nullptr, &config);
		con = make_uniq<Connection>(*db);
		GatedSecretType::Register(*db->instance);
	}

	//! Registers a temporary `gated` secret for `scope`, scoped to `catalog` (empty: global), carrying `token`
	void Register(const string &name, const string &scope, const string &catalog, const string &token) {
		auto secret = make_uniq<KeyValueSecret>(duckdb::vector<string> {scope}, GatedSecretType::NAME, "config", name);
		secret->secret_map["token"] = token;
		secret->SetCatalog(catalog);
		auto transaction = CatalogTransaction::GetSystemTransaction(*db->instance);
		SecretManager::Get(*db->instance)
		    .RegisterSecret(transaction, std::move(secret), OnCreateConflict::ERROR_ON_CONFLICT,
		                    SecretPersistType::TEMPORARY);
	}

	//! The name of the secret a lookup for `path` resolves when the operation presents `catalog`
	string Resolved(const string &path, const string &catalog = string()) {
		auto transaction = CatalogTransaction::GetSystemTransaction(*db->instance);
		auto match = SecretManager::Get(*db->instance).LookupSecret(transaction, path, GatedSecretType::NAME, catalog);
		return match.HasMatch() ? match.GetSecret().GetName() : string();
	}

	//! The token KeyValueSecretReader reads for `path` through the client's opener, with `info_catalog` on the open
	string Token(const string &path, const string &info_catalog = string()) {
		string token;
		// the reader resolves through the opener's catalog transaction, which needs an active transaction
		con->context->RunFunctionInTransaction([&]() {
			FileOpenerInfo info {path, info_catalog};
			KeyValueSecretReader reader(*ClientData::Get(*con->context).file_opener, &info, GatedSecretType::NAME);
			if (!reader.TryGetSecretKey<string>("token", token)) {
				token.clear();
			}
		});
		return token;
	}

	duckdb::unique_ptr<DuckDB> db;
	duckdb::unique_ptr<Connection> con;
};

const string LAKE_FILE = "gated://bucket/lake/main/t/data.parquet";
const string DEEP_FILE = "gated://bucket/lake/deep/data.parquet";
const string OTHER_FILE = "gated://bucket/other/data.parquet";

} // namespace

TEST_CASE_METHOD(CatalogScopedSecretsFixture, "A catalog-scoped secret resolves only for its catalog",
                 "[secret][catalog_scoped_secrets]") {
	Register("global_bucket", "gated://bucket/", "", "global");
	Register("lake_secret", "gated://bucket/lake/", "lake", "lake");
	Register("other_secret", "gated://bucket/", "other", "other");

	SECTION("a path lookup without a catalog only sees global secrets") {
		REQUIRE(Resolved(LAKE_FILE) == "global_bucket");
		REQUIRE(Resolved(OTHER_FILE) == "global_bucket");
	}

	SECTION("the presented catalog's own secret outranks any global one") {
		REQUIRE(Resolved(LAKE_FILE, "lake") == "lake_secret");
		// a shorter scope than the global secret's still wins
		Register("global_lake", "gated://bucket/lake/main/", "", "global");
		REQUIRE(Resolved(LAKE_FILE) == "global_lake");
		REQUIRE(Resolved(LAKE_FILE, "lake") == "lake_secret");
		// within the catalog the longest scope wins, as for global secrets
		Register("lake_deep", "gated://bucket/lake/deep/", "lake", "lake-deep");
		REQUIRE(Resolved(DEEP_FILE, "lake") == "lake_deep");
		REQUIRE(Resolved(LAKE_FILE, "lake") == "lake_secret");
	}

	SECTION("another catalog never sees it, and falls back to global secrets") {
		REQUIRE(Resolved(LAKE_FILE, "other") == "other_secret");
		REQUIRE(Resolved(OTHER_FILE, "lake") == "global_bucket");
		REQUIRE(Resolved(LAKE_FILE, "nobody") == "global_bucket");
		REQUIRE(Resolved("gated://elsewhere/data.parquet", "lake").empty());
	}

	SECTION("which_secret presents no catalog, so it only sees global secrets") {
		auto result = con->Query("SELECT name FROM which_secret('" + LAKE_FILE + "', 'gated')");
		REQUIRE(CHECK_COLUMN(result, 0, {"global_bucket"}));
	}
}

TEST_CASE_METHOD(CatalogScopedSecretsFixture, "A file operation presents its catalog through the OpenFileInfo",
                 "[secret][catalog_scoped_secrets]") {
	Register("global_bucket", "gated://bucket/", "", "global");
	Register("lake_secret", "gated://bucket/lake/", "lake", "lake");
	Register("other_secret", "gated://bucket/", "other", "other");

	REQUIRE(Token(LAKE_FILE) == "global");
	REQUIRE(Token(LAKE_FILE, "lake") == "lake");
	REQUIRE(Token(DEEP_FILE, "lake") == "lake");
	REQUIRE(Token(LAKE_FILE, "other") == "other");
	REQUIRE(Token(OTHER_FILE, "lake") == "global");
}

TEST_CASE_METHOD(CatalogScopedSecretsFixture, "A catalog-scoped secret is neither reached by its name nor serialized",
                 "[secret][catalog_scoped_secrets]") {
	Register("global_bucket", "gated://bucket/", "", "global");
	Register("lake_secret", "gated://bucket/lake/", "lake", "lake");
	auto &secret_manager = SecretManager::Get(*db->instance);
	auto transaction = CatalogTransaction::GetSystemTransaction(*db->instance);

	REQUIRE(secret_manager.GetSecretByName(transaction, "global_bucket"));
	REQUIRE(!secret_manager.GetSecretByName(transaction, "lake_secret"));
	REQUIRE(!secret_manager.GetSecretByName(transaction, "lake_secret", "memory"));
	REQUIRE_FAIL(con->Query("DROP SECRET lake_secret"));
	REQUIRE(Resolved(LAKE_FILE, "lake") == "lake_secret");

	auto serialize = [](const BaseSecret &secret) {
		MemoryStream stream;
		BinarySerializer serializer(stream);
		serializer.Begin();
		secret.Serialize(serializer);
		serializer.End();
	};
	REQUIRE_NOTHROW(serialize(secret_manager.LookupSecret(transaction, LAKE_FILE, GatedSecretType::NAME).GetSecret()));
	REQUIRE_THROWS(
	    serialize(secret_manager.LookupSecret(transaction, LAKE_FILE, GatedSecretType::NAME, "lake").GetSecret()));
}
