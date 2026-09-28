#!/usr/bin/env bash
set -u

# Liveness signals for the canvas server (million-sandboxes): one UDP datagram
# [token][SIGNAL_ID u32 little-endian][1] when the workload starts, [..][0]
# when it exits. Best-effort and off unless SIGNAL_HOST is set.
signal() {
  [ -n "${SIGNAL_HOST:-}" ] || return 0
  local id=${SIGNAL_ID:-0} fmt
  fmt=$(printf '%%s\\x%02x\\x%02x\\x%02x\\x%02x\\x%02x' \
    $((id & 255)) $(((id >> 8) & 255)) $(((id >> 16) & 255)) $(((id >> 24) & 255)) "$1")
  # shellcheck disable=SC2059
  printf "$fmt" "${SIGNAL_TOKEN:-}" >"/dev/udp/$SIGNAL_HOST/${SIGNAL_PORT:-7777}" 2>/dev/null || true
}
trap 'signal 0' EXIT
signal 1

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
