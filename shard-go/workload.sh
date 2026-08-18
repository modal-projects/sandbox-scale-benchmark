#!/usr/bin/env bash
set -u
cd /sqlite 2>/dev/null || { echo '{"status":"build_failed","error":"cd /sqlite"}'; exit 0; }

as_tester() { runuser -u tester -- "$@"; }

b0=$(date +%s%3N)
as_tester make testfixture >/tmp/build.log 2>&1
brc=$?
b1=$(date +%s%3N)
build_ms=$((b1 - b0))

if [ "$brc" -ne 0 ] || [ ! -x ./testfixture ]; then
  echo "{\"status\":\"build_failed\",\"build_ms\":$build_ms}"
  exit 0
fi

echo "{\"status\":\"success\",\"build_ms\":$build_ms}"
