# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check videos can be decoded and measure their properties. This script runs inside a Kubernetes pod
using the Cosmos Curator image."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from fractions import Fraction
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import msc_credentials

logger = logging.getLogger(__name__)


def ffprobe_decode(path: Path) -> dict:
    """Decode video frames with the ffprobe sidecar."""
    completed = subprocess.run(
        [
            "/opt/ffmpeg/bin/ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v",
            "-show_entries",
            "stream=codec_name,avg_frame_rate:frame=width,height",
            "-of",
            "json",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    errors = completed.stderr.strip()
    if completed.returncode != 0 or errors:
        raise ValueError(
            f"ffprobe could not decode the video: {errors or f'exit {completed.returncode}'}"
        )
    return json.loads(completed.stdout)


def validate_video_file(path: Path, expected: dict) -> dict:
    """Decode every frame and return a checksum and measured media properties."""
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    probed = ffprobe_decode(path)
    streams = probed.get("streams") or []
    if len(streams) != 1:
        raise ValueError("Generated output must contain exactly one video stream")

    stream = streams[0]
    numerator, _, denominator = (stream.get("avg_frame_rate") or "0/0").partition("/")
    if not int(numerator) or not int(denominator or 1):
        raise ValueError("Generated output has no frame rate")

    fps = float(Fraction(int(numerator), int(denominator or 1)))
    codec = stream.get("codec_name")
    decoded = probed.get("frames") or []
    if not decoded:
        raise ValueError("Video has no decodable frames")

    width, height = decoded[0]["width"], decoded[0]["height"]
    if expected.get("size"):
        width, height = map(int, expected["size"].split("x"))
    if any((frame.get("width"), frame.get("height")) != (width, height) for frame in decoded):
        raise ValueError("Generated dimensions do not match requested size")

    frames = len(decoded)
    if ("num_frames" in expected and frames != expected["num_frames"]) or (
        "fps" in expected and abs(fps - expected["fps"]) > 0.01
    ):
        raise ValueError(f"Generated frame contract mismatch: {frames} frames at {fps} FPS")
    return {
        "passed": True,
        "sha256": digest,
        "bytes": path.stat().st_size,
        "frames": frames,
        "fps": fps,
        "width": width,
        "height": height,
        "codec": codec,
    }


def s3_client_for_uri(uri: str, endpoint_url: str | None):
    """Use the URI's MSC profile, without caching credentials in the environment."""
    credentials = msc_credentials.aws_env_from_msc(
        os.environ.get(msc_credentials.MSC_ENV_VAR, ""), uri
    )
    options = {
        "aws_access_key_id": credentials.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": credentials.get("AWS_SECRET_ACCESS_KEY"),
        "region_name": credentials.get("AWS_REGION"),
        "endpoint_url": endpoint_url or credentials.get("S3_ENDPOINT_URL"),
    }
    return boto3.client("s3", **{key: value for key, value in options.items() if value})


def fetch(uri: str, destination: Path, endpoint_url: str | None) -> Path:
    if not uri.startswith("s3://"):
        shutil.copyfile(uri, destination)
        return destination
    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    s3_client_for_uri(uri, endpoint_url).download_file(bucket, key, str(destination))
    return destination


def store(body: bytes, uri: str, endpoint_url: str | None) -> None:
    if not uri.startswith("s3://"):
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_bytes(body)
        return
    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    s3_client_for_uri(uri, endpoint_url).put_object(Bucket=bucket, Key=key, Body=body)


def probe_items(
    items: list[Mapping[str, Any]], work_dir: Path, endpoint_url: str | None
) -> list[dict[str, Any]]:
    results = []
    for index, item in enumerate(items, 1):
        local = work_dir / "media.mp4"
        try:
            fetch(item["uri"], local, endpoint_url)
            measured = validate_video_file(local, dict(item["expected"]))
            results.append({"id": item["id"], **measured})
        except Exception as exc:
            results.append({"id": item["id"], "passed": False, "error": str(exc)})
        finally:
            local.unlink(missing_ok=True)
        logger.info("Probed media %s/%s: passed=%s", index, len(items), results[-1]["passed"])
    return results


def count_embeddings(uris: list[str], work_dir: Path, endpoint_url: str | None) -> int:
    """Count distinct exported spans using the Curator image's parquet support."""
    if not uris:
        return 0
    import pandas as pd

    embedded = set()
    local = work_dir / "embeddings.parquet"
    for uri in uris:
        try:
            fetch(uri, local, endpoint_url)
            embedded.update(pd.read_parquet(local, columns=["id"])["id"].astype(str))
        finally:
            local.unlink(missing_ok=True)
    return len(embedded)


def run_probe_from_env(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Entry point that runs the media probe inside the pod."""
    values = dict(os.environ if env is None else env)
    request_uri = values["PROBE_REQUEST_URI"]
    result_uri = values["PROBE_RESULT_URI"]
    endpoint = values.get("OBJECT_STORE_ENDPOINT") or values.get("S3_ENDPOINT_URL") or None

    with tempfile.TemporaryDirectory(prefix="media-probe-") as directory:
        work_dir = Path(directory)
        request_path = fetch(request_uri, work_dir / "request.json", endpoint)
        request = json.loads(request_path.read_text(encoding="utf-8"))
        result = {"items": probe_items(request.get("items") or [], work_dir, endpoint)}
        if "embedding_uris" in request:
            result["embedding_count"] = count_embeddings(
                request["embedding_uris"], work_dir, endpoint
            )
    store(json.dumps(result, indent=2).encode(), result_uri, endpoint)
    failed = sum(not item["passed"] for item in result["items"])
    logger.info("Probed %s video(s), %s failed; wrote %s", len(result["items"]), failed, result_uri)
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_probe_from_env()
