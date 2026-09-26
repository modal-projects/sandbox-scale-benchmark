"""Publish the runner Image that burst-go's -shards mode boots.

The Go SDK has no API for adding a local file to an Image, so the runner Image
is built here and referenced from Go by name. The tag is a prefix of the
binary's own SHA-256, and the Go driver derives the same tag from the binary it
is running, so a stale Image is a lookup failure rather than a silently wrong
benchmark.

    uv run python burst-go/build_runner_image.py burst-go/burst
"""

from __future__ import annotations

import argparse
import hashlib
import pathlib

import modal

# Alpine ships a CA bundle, which runners need to reach Modal over TLS.
BASE_IMAGE = "alpine:3.21"
RUNNER_PATH = "/usr/local/bin/burst"


def image_tag(binary: pathlib.Path) -> str:
    return hashlib.sha256(binary.read_bytes()).hexdigest()[:12]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path, help="statically linked burst binary")
    parser.add_argument("--name", default="burst-runner", help="published Image name")
    parser.add_argument("--app", default="sandbox-burst-load-test", help="App to build in")
    args = parser.parse_args()

    if not args.binary.is_file():
        raise SystemExit(f"no such binary: {args.binary}")

    ref = f"{args.name}:{image_tag(args.binary)}"
    image = (
        modal.Image.from_registry(BASE_IMAGE)
        .add_local_file(str(args.binary), RUNNER_PATH, copy=True)
        .run_commands(f"chmod +x {RUNNER_PATH}")
    )
    app = modal.App.lookup(args.app, create_if_missing=True)
    with modal.enable_output():
        image = image.build(app)
    image.publish(ref)
    print(ref)


if __name__ == "__main__":
    main()
