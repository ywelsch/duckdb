# TEMPORARY (CI only, not for review): test_utils (duckdb/bwc-test-utils) is disabled on this
# branch. The pinned commit predates the Extension::Load(ExtensionLoader&) API migration, so it
# fails to compile ("Load(DuckDB&) marked override, but does not override"). main only avoids this
# via a warm ccache that never recompiles the extension; a cold build (like this branch's CI)
# surfaces it. Drop this commit once the pin is bumped to a commit using the ExtensionLoader API.
# duckdb_extension_load(test_utils
#   GIT_URL https://github.com/duckdb/bwc-test-utils
#   # Use the commit before "Update extensions" (that contains the binaries of
#   # the commit before that).
#   GIT_TAG 61e38820957b51bfbc734e8b6c0403feb7e4883b
#   APPLY_PATCHES
#   # For local dev:
#   # SOURCE_DIR "${EXTENSION_CONFIG_BASE_DIR}/../../../../test-utils"
# )

include("${EXTENSION_CONFIG_BASE_DIR}/../in_tree_extensions.cmake")
