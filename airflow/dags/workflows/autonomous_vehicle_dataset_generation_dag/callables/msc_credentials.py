# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Convert Multi-Storage Client (MSC) configuration into S3 credential formats the workflow’s tools need"""

from __future__ import annotations

import argparse
import ast
import configparser
import json
import logging
import os
import shlex
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

MSC_ENV_VAR = "MULTISTORAGECLIENT_CONFIGURATION"
DEFAULT_S3_PROFILE_PATH = "/dev/shm/s3_creds_file"


def parse_msc_config(raw: str) -> dict[str, Any]:
    """Parse the MSC secret, which is JSON but has historically been a Python literal."""
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            return {}
    return parsed if isinstance(parsed, dict) else {}


def bucket_of(uri: str | None) -> str | None:
    """Return the bucket for an object-store URI, or None for a local path."""
    if not uri:
        return None
    parsed = urlparse(str(uri))
    if parsed.scheme in {"s3", "gs", "msc"} and parsed.netloc:
        return parsed.netloc
    return None


def _select_profile(profiles: Mapping[str, Any], bucket: str | None) -> dict[str, Any]:
    """Choose the MSC storage profile for a bucket."""
    if bucket:
        if bucket in profiles:
            candidate = profiles[bucket]
            return dict(candidate) if isinstance(candidate, Mapping) else {}
        for name, candidate in profiles.items():
            if not isinstance(candidate, Mapping):
                continue
            options = (candidate.get("storage_provider") or {}).get("options") or {}
            if options.get("base_path") == bucket or name == bucket:
                return dict(candidate)
        return {}
    for candidate in profiles.values():
        if isinstance(candidate, Mapping):
            return dict(candidate)
    return {}


def aws_env_from_msc(raw: str, uri: str | None = None) -> dict[str, str]:
    """Convert an MSC storage profile into a dictionary of S3 environment variables."""
    config = parse_msc_config(raw)
    profiles = config.get("profiles")
    if not isinstance(profiles, Mapping) or not profiles:
        return {}
    profile = _select_profile(profiles, bucket_of(uri))
    if not profile:
        return {}

    storage_options = (profile.get("storage_provider") or {}).get("options") or {}
    credential_options = (profile.get("credentials_provider") or {}).get("options") or {}

    access_key = credential_options.get("access_key")
    secret_key = credential_options.get("secret_key")
    region = storage_options.get("region_name")
    endpoint = storage_options.get("endpoint_url")
    env: dict[str, str] = {}
    env = {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_REGION": region,
        "AWS_DEFAULT_REGION": region,
        "S3_ENDPOINT_URL": endpoint,
    }
    return {key: str(value) for key, value in env.items() if value}


def s3_profile_ini(raw: str, uris: Mapping[str, str | None]) -> configparser.ConfigParser:
    """Build the S3 credentials configuration that Cosmos Curator reads"""
    config = configparser.ConfigParser()
    first: str | None = None
    for role in ("input", "output", "query"):
        uri = uris.get(role)
        bucket = bucket_of(uri)
        if not bucket or config.has_section(bucket):
            if bucket and first is None:
                first = bucket
            continue
        env = aws_env_from_msc(raw, uri)
        if not env.get("AWS_ACCESS_KEY_ID"):
            continue
        config[bucket] = {
            "aws_access_key_id": env["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": env.get("AWS_SECRET_ACCESS_KEY", ""),
            **({"endpoint_url": env["S3_ENDPOINT_URL"]} if env.get("S3_ENDPOINT_URL") else {}),
            **({"region": env["AWS_REGION"]} if env.get("AWS_REGION") else {}),
        }
        if first is None:
            first = bucket
    if first and config.has_section(first) and not config.has_section("default"):
        config["default"] = dict(config[first])
    return config


def write_s3_profile_file(
    path: str | Path,
    uris: Mapping[str, str | None],
    raw: str | None = None,
) -> Path:
    """Write Curator’s S3 credentials to the supplied path."""
    raw = os.environ.get(MSC_ENV_VAR, "") if raw is None else raw
    config = s3_profile_ini(raw, uris)
    if not config.sections():
        raise SystemExit(
            "No S3 profiles could be derived from the multistorageclient configuration; "
            "Curator would fail with 'Not found S3 creds'."
        )
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        config.write(handle)
    target.chmod(0o600)
    return target


def download(uri: str, destination: str) -> str:
    """Download one S3 object to a local file."""
    import boto3

    raw = os.environ.get(MSC_ENV_VAR, "")
    for key, value in aws_env_from_msc(raw, uri).items():
        os.environ.setdefault(key, value)

    if not uri.startswith("s3://"):
        raise ValueError(f"Expected an s3:// URI, got {uri!r}")
    bucket, _, key = uri[len("s3://") :].partition("/")
    endpoint = os.environ.get("OBJECT_STORE_ENDPOINT") or os.environ.get("S3_ENDPOINT_URL")

    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    boto3.client("s3", endpoint_url=endpoint or None).download_file(bucket, key, str(target))
    return str(target)


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write-s3-profile",
        nargs="?",
        const=DEFAULT_S3_PROFILE_PATH,
        help="Write Curator's S3 profile INI here instead of emitting exports.",
    )
    parser.add_argument(
        "--download",
        metavar="URI",
        help="Download this object-store URI to --dest instead of emitting exports.",
    )
    parser.add_argument(
        "--dest",
        help="Local path for --download.",
    )
    parser.add_argument("--write-curator-timing", type=float)
    args = parser.parse_args()
    if args.write_curator_timing is not None:
        import boto3

        uri = os.environ["CURATOR_OUTPUT_URI"].rstrip("/") + "/pipeline_timing.json"
        values = aws_env_from_msc(os.environ.get(MSC_ENV_VAR, ""), uri)
        for key, value in values.items():
            os.environ.setdefault(key, value)
        bucket, _, key = uri[5:].partition("/")
        endpoint = os.environ.get("OBJECT_STORE_ENDPOINT") or os.environ.get("S3_ENDPOINT_URL")
        boto3.client("s3", endpoint_url=endpoint or None).put_object(
            Bucket=bucket, Key=key, Body=json.dumps({"seconds": args.write_curator_timing}).encode()
        )
        return

    if args.download:
        if not args.dest:
            parser.error("--download requires --dest")
        logger.info("Downloaded %s to %s", args.download, download(args.download, args.dest))
        return

    uris = {
        "input": os.environ.get("CURATOR_INPUT_URI"),
        "output": os.environ.get("CURATOR_OUTPUT_URI"),
        "query": os.environ.get("CURATOR_QUERY_INPUT_URI"),
    }

    if args.write_s3_profile:
        written = write_s3_profile_file(args.write_s3_profile, uris)
        sections = ", ".join(s3_profile_ini(os.environ.get(MSC_ENV_VAR, ""), uris).sections())
        logger.info("Wrote S3 profiles [%s] to %s", sections, written)
        return

    for key, value in aws_env_from_msc(
        os.environ.get(MSC_ENV_VAR, ""), uris["input"] or uris["output"]
    ).items():
        if key not in os.environ:
            sys.stdout.write(f"export {key}={shlex.quote(value)}\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main()
