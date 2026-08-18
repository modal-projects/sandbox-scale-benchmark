from modal_burst.config import Config
from modal_burst.orchestrator import _shard_env


def test_wait_for_shard_creates_env():
    assert _shard_env(0, 1, 0, Config(), {})["WAIT_FOR_SHARD_CREATES"] == "0"
    cfg = Config(wait_for_shard_creates=True)
    assert _shard_env(0, 1, 0, cfg, {})["WAIT_FOR_SHARD_CREATES"] == "1"
