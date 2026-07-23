#!/bin/bash
# TSAN-DEBUG: loop the failing test with the raw test binary so the full ThreadSanitizer
# report (all stacks) reaches the log; halt on the first report
set -x
export TSAN_OPTIONS="$TSAN_OPTIONS:halt_on_error=1"
BIN=build/reldebug/test/unittest
for i in $(seq 1 20); do
  echo "=== iteration $i ==="
  $BIN --test-config test/configs/threadsan.json \
    "test/sql/parallelism/interquery/concurrent_drop_schema_macro_table.test_slow" || exit 1
done
