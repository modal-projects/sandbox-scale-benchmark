"""Pure aggregation: shard payloads -> the run's ``meta.json``.

No IO, no ``modal`` import, so it is fully unit-testable.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from ._results import BUILD_FAILED, CREATE_FAILED, SUCCESS, WORKLOAD_ERROR

# Modal object IDs (ta-…, sb-…, im-…) make otherwise-identical error messages
# unique; strip them so failures of the same kind group together.
_ID_RE = re.compile(r"\b[a-z]{2}-[A-Za-z0-9]{8,}\b")


def _normalize_reason(msg: str) -> str:
    return _ID_RE.sub("<id>", msg)


def _pct(values: list[int], q: float) -> int:
    if not values:
        return 0
    s = sorted(values)
    return s[min(len(s) - 1, int(len(s) * q))]


def distribution(values: list[int]) -> Optional[dict[str, int]]:
    if not values:
        return None
    s = sorted(values)
    return {
        "count": len(s),
        "min_ms": s[0],
        "p50_ms": _pct(s, 0.50),
        "p90_ms": _pct(s, 0.90),
        "p95_ms": _pct(s, 0.95),
        "p99_ms": _pct(s, 0.99),
        "max_ms": s[-1],
        "mean_ms": round(sum(s) / len(s)),
    }


def _span(starts: list[int], ends: list[int]) -> int:
    """Wall-clock span from the earliest start to the latest end (epoch ms),
    across all shards — captures cross-shard skew. 0 if unavailable."""
    if not starts or not ends:
        return 0
    return max(ends) - min(starts)


def aggregate(
    shard_dicts: list[dict[str, Any]],
    *,
    run_id: str,
    total: int,
    num_shards: int,
    shards_completed: int,
    wall_ms: int,
) -> dict[str, Any]:
    statuses = {SUCCESS: 0, BUILD_FAILED: 0, CREATE_FAILED: 0, WORKLOAD_ERROR: 0}
    build_ms: list[int] = []
    sandboxes_created = 0
    failure_reasons: dict[str, int] = {}
    shard_errors: list[str] = []
    create_starts: list[int] = []
    all_created: list[int] = []
    build_starts: list[int] = []
    build_dones: list[int] = []

    for sd in shard_dicts:
        sandboxes_created += sd.get("sandboxes_created", 0)
        for key, bucket in (
            ("create_start_epoch_ms", create_starts),
            ("all_created_epoch_ms", all_created),
            ("build_start_epoch_ms", build_starts),
            ("build_done_epoch_ms", build_dones),
        ):
            if sd.get(key):
                bucket.append(sd[key])
        if sd.get("error"):
            shard_errors.append(f"shard {sd.get('shard_index')}: {sd['error']}")
        for r in sd.get("results", []):
            status = r.get("status", WORKLOAD_ERROR)
            statuses[status] = statuses.get(status, 0) + 1
            if r.get("build_ms") is not None:
                build_ms.append(r["build_ms"])
            if status != SUCCESS:
                reason = r.get("error") or status
                key = f"{status}: {_normalize_reason(str(reason))}"[:160]
                failure_reasons[key] = failure_reasons.get(key, 0) + 1

    attempted = sum(statuses.values())
    total_compute_seconds = round(sum(build_ms) / 1000)
    return {
        "run_id": run_id,
        "total_target": total,
        "num_shards": num_shards,
        "shards_completed": shards_completed,
        "sandboxes_attempted": attempted,
        "sandboxes_created": sandboxes_created,
        "builds_succeeded": statuses[SUCCESS],
        "build_failed": statuses[BUILD_FAILED],
        "create_failed": statuses[CREATE_FAILED],
        "workload_error": statuses[WORKLOAD_ERROR],
        "status_histogram": statuses,
        "build_ms_distribution": distribution(build_ms),
        "time_to_create_all_ms": _span(create_starts, all_created),
        "time_to_build_all_ms": _span(build_starts, build_dones),
        "total_compute_seconds": total_compute_seconds,
        "failure_reasons": failure_reasons,
        "shard_errors": shard_errors,
        "wall_ms": wall_ms,
    }
