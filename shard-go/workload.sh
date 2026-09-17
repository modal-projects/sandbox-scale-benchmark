#!/usr/bin/env bash
set -u

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
