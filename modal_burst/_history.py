"""Pre-flight scale check against prior runs recorded under ``results/``.

Jumping straight to a large target without first proving a smaller one tends to
burn a lot of capacity discovering a limit that a cheaper run would have found.
The check reads every ``results/<run_id>/meta.json`` and refuses a target that
is more than ``MAX_STEP_FACTOR`` times the largest run that completed cleanly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

MAX_STEP_FACTOR = 2
NO_HISTORY_MAX = 500  # largest --total allowed before any run has been proven
MIN_SUCCESS_RATIO = 0.9  # created / target for a run to count as proven


def load_prior_runs(results_dir: Path) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    if not results_dir.is_dir():
        return runs
    for meta_path in sorted(results_dir.glob("*/meta.json")):
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(meta, dict):
            runs.append(meta)
    return runs


def largest_proven_target(runs: list[dict[str, Any]]) -> Optional[int]:
    best: Optional[int] = None
    for meta in runs:
        target = meta.get("total_target")
        created = meta.get("sandboxes_created", 0)
        if not isinstance(target, int) or target <= 0 or not isinstance(created, int):
            continue
        if meta.get("shard_errors"):
            continue
        if created < target * MIN_SUCCESS_RATIO:
            continue
        if best is None or target > best:
            best = target
    return best


def scale_warning(total: int, proven: Optional[int]) -> Optional[str]:
    """Return a message if ``total`` is too big a jump from ``proven``, else None."""
    if proven is None:
        if total <= NO_HISTORY_MAX:
            return None
        return (
            f"no proven prior run found under results/ but --total is {total:,}; "
            f"run something smaller first (e.g. --total {NO_HISTORY_MAX}) before scaling up"
        )
    limit = proven * MAX_STEP_FACTOR
    if total <= limit:
        return None
    return (
        f"--total {total:,} is more than {MAX_STEP_FACTOR}x the largest proven run "
        f"({proven:,} target, >={MIN_SUCCESS_RATIO:.0%} created); "
        f"try --total {limit:,} or less first"
    )
