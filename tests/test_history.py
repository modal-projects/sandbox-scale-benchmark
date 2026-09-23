import json

from modal_burst._history import largest_proven_target, load_prior_runs, scale_warning


def _meta(target: int, created: int, shard_errors=()):
    return {"total_target": target, "sandboxes_created": created, "shard_errors": list(shard_errors)}


def test_load_prior_runs_skips_missing_and_bad(tmp_path):
    assert load_prior_runs(tmp_path / "nope") == []
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "meta.json").write_text(json.dumps(_meta(100, 100)))
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "meta.json").write_text("not json")
    runs = load_prior_runs(tmp_path)
    assert [r["total_target"] for r in runs] == [100]


def test_largest_proven_target_requires_clean_run():
    runs = [
        _meta(1000, 1000),
        _meta(5000, 4000),  # 80% created: not proven
        _meta(8000, 8000, shard_errors=["shard 0: boom"]),  # shard error: not proven
        _meta(2000, 1900),  # 95%: proven
    ]
    assert largest_proven_target(runs) == 2000
    assert largest_proven_target([]) is None


def test_scale_warning():
    assert scale_warning(500, None) is None
    assert scale_warning(501, None) is not None
    assert scale_warning(10_000, None) is not None
    assert scale_warning(10_000, 5000) is None
    assert scale_warning(10_000, 4000) is not None
    assert "8,000" in scale_warning(10_000, 4000)
