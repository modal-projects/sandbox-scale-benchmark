from __future__ import annotations

import os
from pathlib import Path

import modal

APP_NAME = "modal-burst"
# Inner sandboxes (the ones under test) live in their own App.
SANDBOX_APP_NAME = "modal-burst-sandboxes"

REPO_ROOT = Path(__file__).resolve().parents[1]
SHARD_SRC = REPO_ROOT / "shard-go"
SHARD_BIN = REPO_ROOT / "bin" / "burst-shard"
SHARD_REMOTE = "/app/burst-shard"


def shard_image() -> modal.Image:
    """Image for the shard sandboxes: just the static Go binary + CA certs.

    Built lazily (after the binary exists) so a fresh checkout doesn't need a
    committed binary.
    """
    return (
        modal.Image.debian_slim()
        .apt_install("ca-certificates")
        .add_local_file(str(SHARD_BIN), SHARD_REMOTE, copy=True)
        .run_commands(f"chmod +x {SHARD_REMOTE}")
    )


def worker_env() -> dict[str, str]:
    """Modal auth to forward into the shard sandbox so the Go client can reach
    the API. Passed via env (not a Secret) to avoid SecretCreate rate limits."""
    from modal.config import config as modal_config

    out: dict[str, str] = {}
    pairs = (
        ("MODAL_TOKEN_ID", "token_id"),
        ("MODAL_TOKEN_SECRET", "token_secret"),
        ("MODAL_SERVER_URL", "server_url"),
        ("MODAL_ENVIRONMENT", "environment"),
    )
    for env_key, cfg_key in pairs:
        value = os.environ.get(env_key) or modal_config.get(cfg_key)
        if value:
            out[env_key] = value
    return out
