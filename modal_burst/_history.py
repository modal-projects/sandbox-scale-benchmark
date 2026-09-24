"""Pre-flight scale check against prior runs in ``results/``.

Reads every ``results/<run_id>/meta.json``, finds the largest run that completed
cleanly, and asks for confirmation before running more than ``MAX_STEP_FACTOR``x
that. ``results/`` is gitignored, so the history is per checkout.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

MAX_STEP_FACTOR = 2
NO_HISTORY_MAX = 500  # max --total with no prior runs
MIN_SUCCESS_RATIO = 0.9  # created/target needed for a run to count as proven


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


def suggested_total(total: int, proven: Optional[int]) -> Optional[int]:
    """Reduced load to propose, or None if ``total`` is within the allowed step."""
    limit = NO_HISTORY_MAX if proven is None else proven * MAX_STEP_FACTOR
    return None if total <= limit else limit


def scale_warning(total: int, proven: Optional[int]) -> Optional[str]:
    """Explain why ``total`` is a big jump, or None if it isn't."""
    suggestion = suggested_total(total, proven)
    if suggestion is None:
        return None
    if proven is None:
        return (
            f"no proven prior run found under results/; proposed test is {total:,} "
            f"(max {NO_HISTORY_MAX:,} without history)"
        )
    return (
        f"previous proven run was {proven:,} (>={MIN_SUCCESS_RATIO:.0%} created); "
        f"proposed test is {total:,} ({total / proven:.1f}x previous)"
    )


def confirm_total(total: int, proven: Optional[int], ask=input) -> int:
    """Ask [Y/n] to run at the reduced load; return the total to actually run."""
    suggestion = suggested_total(total, proven)
    if suggestion is None:
        return total
    print(scale_warning(total, proven))
    answer = ask(f"Proposing load be set at {suggestion:,}. Accept? [Y/n] ").strip().lower()
    if answer in ("", "y", "yes"):
        print(f"Running test at {suggestion:,}...")
        return suggestion
    reason = f"{total / proven:.1f}x previous test" if proven else "no proven prior run"
    print(f"Load reduction rejected. Proceeding with {total:,} (WARN: {reason})...")
    return total
