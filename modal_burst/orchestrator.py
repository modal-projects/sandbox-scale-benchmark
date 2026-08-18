"""Top-level orchestration (local).

Builds the Go shard binary, creates one shard *sandbox* per group of work, execs
the binary in each (exec-each-shard), collects each shard's JSON from stdout, and
aggregates. The shard binary does the actual high-concurrency inner-sandbox
creation, so the outer orchestration's latency never enters the measured numbers.

Run as a plain script so the Modal Apps are persistent (looked up, not ephemeral)
and stay Live in the dashboard after the run:

    uv run python -m modal_burst.orchestrator --total 10000 --shard-size 5000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import Any

import modal

from . import app as appmod
from ._stats import aggregate
from .app import APP_NAME, SANDBOX_APP_NAME, SHARD_REMOTE, SHARD_SRC, shard_image, worker_env
from .config import Config


def _ensure_binary() -> None:
    print(f"building {SHARD_SRC.name}/burst-shard …")
    subprocess.run(
        ["go", "build", "-o", str(appmod.SHARD_BIN), "."],
        cwd=str(SHARD_SRC),
        check=True,
        env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64"},
    )
    if not appmod.SHARD_BIN.is_file():
        raise SystemExit("go build did not produce the shard binary")


def _shard_env(idx: int, size: int, base: int, cfg: Config, wenv: dict[str, str]) -> dict[str, str]:
    return {
        "SHARD_INDEX": str(idx),
        "SHARD_SIZE": str(size),
        "BASE_IDX": str(base),
        "GROUP_SIZE": str(cfg.group_size),
        "RAMP_MS": str(int(cfg.ramp_s * 1000)),
        "CLIENTS": str(cfg.clients),
        "WAIT_FOR_SHARD_CREATES": "1" if cfg.wait_for_shard_creates else "0",
        "SANDBOX_TIMEOUT_S": str(cfg.sandbox_timeout_s),
        "SANDBOX_APP_NAME": SANDBOX_APP_NAME,
        **wenv,
    }


def _parse_shard_json(stdout: str, stderr: str, idx: int) -> dict[str, Any]:
    for line in reversed((stdout or stderr).splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                return obj
    return {"shard_index": idx, "results": [], "error": f"no JSON; tail: {(stderr or stdout)[-300:]}"}


async def _create_shard(host_app: modal.App, image: modal.Image, cfg: Config) -> modal.Sandbox:
    return await modal.Sandbox._experimental_create.aio(
        "sleep",
        "infinity",
        app=host_app,
        image=image,
        cpu=cfg.shard_cpu,
        memory=cfg.shard_memory_mb,
        timeout=cfg.shard_timeout_s,
    )


async def _warmup_shard(sb: modal.Sandbox) -> None:
    """The first exec blocks until the sandbox container is booted. Doing a tiny
    warmup exec here — and gathering it across all shards before the real exec —
    absorbs per-VM boot skew so every shard starts creating on a warm container,
    roughly in lockstep."""
    proc = await sb.exec.aio("true", timeout=120)
    await proc.wait.aio()


async def _run_shard(
    sb: modal.Sandbox, idx: int, size: int, base: int, cfg: Config, wenv: dict[str, str]
) -> dict[str, Any]:
    try:
        proc = await sb.exec.aio(
            SHARD_REMOTE, env=_shard_env(idx, size, base, cfg, wenv), timeout=cfg.shard_timeout_s
        )
        stdout, stderr = await asyncio.gather(proc.stdout.read.aio(), proc.stderr.read.aio())
        await proc.wait.aio()
        return _parse_shard_json(stdout, stderr, idx)
    except Exception as err:  # noqa: BLE001 - surface as a shard error, keep others going
        return {"shard_index": idx, "results": [], "error": str(err)[:300]}


async def run(cfg: Config) -> None:
    wenv = worker_env()
    if "MODAL_TOKEN_ID" not in wenv:
        raise SystemExit("no Modal token found (run `modal token new` or set MODAL_TOKEN_ID/SECRET)")

    _ensure_binary()
    image = shard_image()
    # Persistent (looked-up) App so it stays Live in the dashboard after the run,
    # instead of the ephemeral App that `modal run` would stop on completion.
    host_app = await modal.App.lookup.aio(APP_NAME, create_if_missing=True)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    num_shards = cfg.num_shards
    shards = [
        (i, min(cfg.shard_size, cfg.total - i * cfg.shard_size), i * cfg.shard_size)
        for i in range(num_shards)
    ]
    print(
        f"run_id={run_id} total={cfg.total} shard_size={cfg.shard_size} "
        f"num_shards={num_shards} ramp_s={cfg.ramp_s}"
    )

    t0 = time.perf_counter()
    sbs: list[modal.Sandbox] = []
    try:
        print(f"creating {num_shards} shard sandbox(es)…")
        sbs = list(await asyncio.gather(*[_create_shard(host_app, image, cfg) for _ in shards]))
        print("warming up shard sandboxes (absorb container boot)…")
        await asyncio.gather(*[_warmup_shard(sb) for sb in sbs])
        print("execing shard orchestrators…")
        outputs = list(
            await asyncio.gather(
                *[_run_shard(sb, idx, sz, base, cfg, wenv) for sb, (idx, sz, base) in zip(sbs, shards)]
            )
        )
    finally:
        await asyncio.gather(*[sb.terminate.aio() for sb in sbs], return_exceptions=True)

    wall_ms = round((time.perf_counter() - t0) * 1000)
    completed = sum(1 for o in outputs if not o.get("error"))
    meta = aggregate(
        outputs,
        run_id=run_id,
        total=cfg.total,
        num_shards=num_shards,
        shards_completed=completed,
        wall_ms=wall_ms,
    )
    out_dir = _dump_results(run_id, outputs, meta)
    _print_summary(meta)
    print(f"  results:     {out_dir}/  (meta.json, raw.jsonl)")


def _dump_results(run_id: str, outputs: list[dict[str, Any]], meta: dict[str, Any]):
    out_dir = appmod.REPO_ROOT / "results" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    with (out_dir / "raw.jsonl").open("w") as f:
        for shard in outputs:
            base = shard.get("base_idx", 0)
            shard_index = shard.get("shard_index", 0)
            for i, r in enumerate(shard.get("results", [])):
                f.write(json.dumps({"sandbox_idx": base + i, "shard_index": shard_index, **r}) + "\n")
    return out_dir


def _print_summary(meta: dict[str, Any]) -> None:
    created = meta.get("sandboxes_created", 0)
    succeeded = meta["builds_succeeded"]
    build = meta.get("build_ms_distribution") or {}
    print()
    print("=" * 64)
    print(" modal-burst :: SQLite build")
    print("=" * 64)
    print(f"  run_id:        {meta['run_id']}")
    print(f"  target:        {meta['total_target']:,} ({meta['num_shards']} shards)")
    print(f"  created:       {created:,}")
    print(f"  builds ok:     {succeeded:,}")
    print(
        f"  failures:      create {meta['create_failed']:,} / build {meta['build_failed']:,}"
        f" / workload {meta['workload_error']:,}"
    )
    print(f"  build p50:     {build.get('p50_ms', '-')}ms")
    print("  --- time ---")
    print(f"  TIME TO COMPLETE CREATES: {meta['time_to_create_all_ms'] / 1000:.1f}s")
    print(f"  TIME TO COMPLETE BUILDS:  {meta['time_to_build_all_ms'] / 1000:.1f}s")
    print(f"  real compute done:  {meta['total_compute_seconds'] / 3600:.1f} CPU-hours")
    print(f"  wall (incl. setup): {meta['wall_ms'] / 1000:.1f}s")
    reasons = meta.get("failure_reasons") or {}
    if reasons:
        print("  failure reasons (top 5):")
        for key, count in sorted(reasons.items(), key=lambda kv: -kv[1])[:5]:
            print(f"    {count:>6,}  {key}")
    if meta["shard_errors"]:
        print(f"  shard errors: {len(meta['shard_errors'])}")
        for line in meta["shard_errors"][:5]:
            print(f"    {line}")
    print("=" * 64)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def _parse_args() -> Config:
    d = Config()  # single source of truth for defaults
    p = argparse.ArgumentParser(prog="modal-burst")
    p.add_argument("--total", type=_positive_int, default=d.total)
    p.add_argument("--shard-size", type=_positive_int, default=d.shard_size)
    p.add_argument("--group-size", type=_positive_int, default=d.group_size)
    p.add_argument("--ramp-s", type=_nonnegative_float, default=d.ramp_s)
    p.add_argument("--clients", type=_nonnegative_int, default=d.clients)
    p.add_argument("--wait-for-shard-creates", action="store_true")
    p.add_argument("--shard-cpu", type=_positive_float, default=d.shard_cpu)
    p.add_argument("--shard-memory-mb", type=_positive_int, default=d.shard_memory_mb)
    p.add_argument("--sandbox-timeout-s", type=_positive_int, default=d.sandbox_timeout_s)
    p.add_argument("--shard-timeout-s", type=_positive_int, default=d.shard_timeout_s)
    a = p.parse_args()
    return Config(
        total=a.total,
        shard_size=a.shard_size,
        group_size=a.group_size,
        ramp_s=a.ramp_s,
        clients=a.clients,
        wait_for_shard_creates=a.wait_for_shard_creates,
        shard_cpu=a.shard_cpu,
        shard_memory_mb=a.shard_memory_mb,
        sandbox_timeout_s=a.sandbox_timeout_s,
        shard_timeout_s=a.shard_timeout_s,
    )


if __name__ == "__main__":
    asyncio.run(run(_parse_args()))
