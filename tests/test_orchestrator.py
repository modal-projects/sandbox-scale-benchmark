from modal_burst.config import Config
from modal_burst.orchestrator import _shard_env


def test_wait_for_shard_creates_env():
    assert _shard_env(0, 1, 0, Config(), {})["WAIT_FOR_SHARD_CREATES"] == "0"
    cfg = Config(wait_for_shard_creates=True)
    assert _shard_env(0, 1, 0, cfg, {})["WAIT_FOR_SHARD_CREATES"] == "1"


def test_signal_env_off_by_default():
    env = _shard_env(0, 1, 0, Config(), {})
    assert not any(k.startswith("SIGNAL_") for k in env)


def test_signal_env_forwarded():
    cfg = Config(signal_host="nlb.example", signal_port=7777, signal_token="tok", signal_base=5000)
    env = _shard_env(0, 1, 0, cfg, {})
    assert env["SIGNAL_HOST"] == "nlb.example"
    assert env["SIGNAL_PORT"] == "7777"
    assert env["SIGNAL_TOKEN"] == "tok"
    assert env["SIGNAL_BASE"] == "5000"
