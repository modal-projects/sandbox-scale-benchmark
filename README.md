# modal-burst

A burst benchmark for [Modal](https://modal.com) sandboxes. It creates a target
number of sandboxes, runs a short shell workload in each one, and holds each
sandbox up for a fixed lifetime so the run reaches a concurrency plateau.

It measures two spans on a common wall clock:

- **Time to complete creates** — earliest inner-sandbox create start to the last
  shard completing its create attempts.
- **Time to complete builds** — earliest workload start to the last shard
  completing its workloads. The lifetime hold happens after this span, so it
  does not inflate the number.

## Architecture

```
uv run python -m modal_burst.orchestrator           (local Python)
  └─ go build shard-go → bin/burst-shard
  └─ look up a persistent App
  └─ create N shard sandboxes
  └─ exec /app/burst-shard in each and read stdout JSON
  └─ aggregate → results

each shard sandbox (Go, shard-go/main.go):
  pool of `clients` Modal clients
  create shard_size inner sandboxes concurrently
  run the workload in each successful sandbox, hold it for the rest of its
    lifetime, then tear it down
  print one JSON line of results
```

The inner image is the stock `debian:bookworm-slim`, pinned in
`shard-go/main.go`. `shard-go/workload.sh` is embedded in the Go binary; it
reads two files and checksums one of them, so it finishes in milliseconds and
needs nothing installed in the image.

By default, each sandbox starts building as soon as its own create call returns.
`--wait-for-shard-creates` instead waits for every create call in a shard before
starting that shard's builds. There is no cross-shard barrier, so
`sandboxes_created` is the number of successful creates, not a claim that every
sandbox was alive simultaneously.

The outer Python process only creates shard sandboxes and reads their output.
Create and build spans are measured inside the Go shards on epoch clocks, so
outer orchestration latency is excluded.

## Requirements

- [uv](https://docs.astral.sh/uv/)
- The Go version declared in `shard-go/go.mod`
- Modal auth (`uv run modal token new`, or `MODAL_TOKEN_ID`/`MODAL_TOKEN_SECRET`)

## Quick start

```bash
uv sync
uv run python -m modal_burst.orchestrator --total 100 --shard-size 100
```

Run the orchestrator as a plain script rather than with `modal run` so the Apps
remain available for the shard sandboxes.

## Scaling up

```bash
uv run python -m modal_burst.orchestrator --total 10000 --shard-size 5000
```

Per-shard concurrency is approximately `clients × HTTP/2 streams per
connection`. By default the Go shard uses about `shard_size/100` clients;
override this with `--clients`. Larger runs may require increased sandbox and
container limits.

## Tuning knobs

| flag | default | meaning |
|---|---|---|
| `--total` | `1000` | total inner sandboxes |
| `--shard-size` | `5000` | inner sandboxes per shard |
| `--ramp-s` | `0` | spread a shard's creates over this many seconds; `0` fires them at once |
| `--group-size` | `50` | batch size for the create ramp |
| `--clients` | `0` | Go client pool per shard; `0` selects about one client per 100 sandboxes |
| `--wait-for-shard-creates` | off | wait for all creates in each shard before starting that shard's builds |
| `--shard-cpu` / `--shard-memory-mb` | `4.0` / `4096` | shard sandbox resources |
| `--sandbox-lifetime-s` | `300` | how long a shard holds each inner sandbox up before terminating it |
| `--sandbox-timeout-s` | `3600` | inner-sandbox hard cap enforced by Modal; the backstop if a shard dies |
| `--shard-timeout-s` | `3600` | shard-sandbox lifetime and exec timeout |

Inner sandboxes use Modal's default resources. Numeric arguments reject negative
values, and sizes and timeouts must be greater than zero.

`--sandbox-timeout-s` is the only thing that reaps inner sandboxes if a shard
crashes mid-run, so keep it close to `--sandbox-lifetime-s`. Leaving it at the
`3600` default while running a 5-minute lifetime means a dead shard leaks its
sandboxes for an hour.

## Results

Each run writes `results/<run_id>/meta.json` and `raw.jsonl`. The aggregate
contains create/workload spans, workload timing distributions, status counts,
and grouped failure reasons. Each JSONL row contains `sandbox_idx`,
`shard_index`, `status`, `sandbox_id`, `create_ms`, `build_ms`, and `error`.

`build_ms` and the `build_failed` status keep their names from when the workload
was a SQLite build; they now mean workload duration and workload failure.

Workload and exec-start failures are recorded as their own benchmark outcomes.

## Operational notes

- Sandbox creation uses experimental Modal APIs and may need changes when the
  SDK changes.
- The orchestrator creates persistent `modal-burst` and
  `modal-burst-sandboxes` Apps. Stop or delete them from the Modal dashboard
  when they are no longer needed.
- Modal credentials are forwarded to shard sandboxes through environment
  variables so each Go client can create inner sandboxes. Only run this in an
  environment where those sandboxes are trusted.
- The inner image tag is pinned as `innerImageTag` in `shard-go/main.go`.

## Layout

```
modal_burst/
  app.py           Modal objects, shard image, token plumbing
  config.py        run configuration
  orchestrator.py  build binary → create shard sandboxes → exec Go → aggregate
  _stats.py        result aggregation
  _results.py      status taxonomy
shard-go/
  main.go          inner-sandbox creation and build orchestration
  workload.sh      embedded shell workload
tests/
  test_stats.py
```

## Tests

```bash
uv run pytest
cd shard-go && go test ./...
```

## License

[MIT](LICENSE)
