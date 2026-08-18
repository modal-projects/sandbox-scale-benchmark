from modal_burst._results import BUILD_FAILED, CREATE_FAILED, SUCCESS, WORKLOAD_ERROR
from modal_burst._stats import aggregate, distribution


def test_distribution_empty():
    assert distribution([]) is None


def test_distribution_percentiles():
    d = distribution(list(range(1, 101)))
    assert d["count"] == 100
    assert d["min_ms"] == 1
    assert d["max_ms"] == 100
    assert d["p50_ms"] == 51
    assert d["p99_ms"] == 100
    assert d["mean_ms"] == 50


def _result(status, *, build_ms=None, error=None):
    result = {"status": status}
    if build_ms is not None:
        result["build_ms"] = build_ms
    if error is not None:
        result["error"] = error
    return result


def test_aggregate_counts_and_timings():
    base = 1_000_000_000_000
    shards = [
        {
            "shard_index": 0,
            "sandboxes_created": 2,
            "create_start_epoch_ms": base,
            "all_created_epoch_ms": base + 30_000,
            "build_start_epoch_ms": base + 30_000,
            "build_done_epoch_ms": base + 200_000,
            "results": [
                _result(SUCCESS, build_ms=40_000),
                _result(SUCCESS, build_ms=42_000),
            ],
        },
        {
            "shard_index": 1,
            "sandboxes_created": 2,
            "create_start_epoch_ms": base + 10_000,
            "all_created_epoch_ms": base + 45_000,
            "build_start_epoch_ms": base + 45_000,
            "build_done_epoch_ms": base + 230_000,
            "results": [
                _result(BUILD_FAILED, build_ms=5_000, error="make testfixture for sb-abcdefgh1234"),
                _result(WORKLOAD_ERROR, error="connection closed"),
            ],
        },
        {
            "shard_index": 2,
            "sandboxes_created": 0,
            "results": [_result(CREATE_FAILED, error="quota exceeded")],
        },
        {"shard_index": 3, "results": [], "error": "boom"},
    ]
    meta = aggregate(shards, run_id="r1", total=5, num_shards=4, shards_completed=3, wall_ms=300_000)

    assert meta["sandboxes_attempted"] == 5
    assert meta["sandboxes_created"] == 4
    assert meta["builds_succeeded"] == 2
    assert meta["build_failed"] == 1
    assert meta["create_failed"] == 1
    assert meta["workload_error"] == 1
    assert meta["status_histogram"] == {
        SUCCESS: 2,
        BUILD_FAILED: 1,
        CREATE_FAILED: 1,
        WORKLOAD_ERROR: 1,
    }
    assert meta["build_ms_distribution"]["count"] == 3
    assert meta["shard_errors"] == ["shard 3: boom"]
    assert meta["time_to_create_all_ms"] == 45_000
    assert meta["time_to_build_all_ms"] == 200_000
    assert meta["total_compute_seconds"] == 87
    assert meta["failure_reasons"]["build_failed: make testfixture for <id>"] == 1


def test_aggregate_empty():
    meta = aggregate([], run_id="r0", total=0, num_shards=0, shards_completed=0, wall_ms=0)
    assert meta["sandboxes_attempted"] == 0
    assert meta["sandboxes_created"] == 0
    assert meta["time_to_create_all_ms"] == 0
    assert meta["time_to_build_all_ms"] == 0
    assert meta["build_ms_distribution"] is None
