#!/usr/bin/env bash
set -u

# Liveness signal for the canvas server (million-sandboxes): one UDP datagram
# [token][SIGNAL_ID u32 little-endian][1] when the workload starts. The shard
# sends the matching OFF after it terminates the sandbox, so the tile stays lit
# for the whole sandbox lifetime. Best-effort and off unless SIGNAL_HOST is set.
signal() {
  [ -n "${SIGNAL_HOST:-}" ] || return 0
  local id=${SIGNAL_ID:-0} fmt
  fmt=$(printf '%%s\\x%02x\\x%02x\\x%02x\\x%02x\\x%02x' \
    $((id & 255)) $(((id >> 8) & 255)) $(((id >> 16) & 255)) $(((id >> 24) & 255)) "$1")
  # shellcheck disable=SC2059
  printf "$fmt" "${SIGNAL_TOKEN:-}" >"/dev/udp/$SIGNAL_HOST/${SIGNAL_PORT:-7777}" 2>/dev/null || true
}
signal 1

b0=$(date +%s%3N)

ok=1
release=$(cat /etc/os-release) || ok=0
meminfo_lines=$(wc -l < /proc/meminfo) || ok=0
digest=$(printf '%s' "$release" | cksum | cut -d' ' -f1) || ok=0

b1=$(date +%s%3N)
build_ms=$((b1 - b0))

if [ "$ok" -ne 1 ] || [ -z "$digest" ] || [ "${meminfo_lines:-0}" -lt 1 ]; then
  echo "{\"status\":\"build_failed\",\"build_ms\":$build_ms}"
  exit 0
fi

echo "{\"status\":\"success\",\"build_ms\":$build_ms}"
