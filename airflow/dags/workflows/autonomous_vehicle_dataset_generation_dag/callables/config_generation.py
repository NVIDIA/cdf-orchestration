# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generate the Curator config and the runtime scripts the CDS ingest pod needs
before it is executed."""

from __future__ import annotations

import ast
import base64
import copy
import json
import logging
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml
from airflow.exceptions import AirflowFailException

from dags.shared.utils.msc_utils import write_file_to_directory
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.msc_credentials import (
    bucket_of,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.models import (
    AVDatasetGenerationPayload,
)

logger = logging.getLogger(__name__)

CONFIG_FILENAME = "pipeline_config.yaml"
CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs"
CURATOR_CONFIG_PATH = CONFIG_DIR / "cosmos_curator_config.yaml"
CALLABLES_DIR = Path(__file__).resolve().parent
CDS_INGEST_SCRIPT_PATH = CALLABLES_DIR / "cds_ingest.py"
MSC_CREDENTIALS_SCRIPT_PATH = CALLABLES_DIR / "msc_credentials.py"
MEDIA_PROBE_SCRIPT_PATH = CALLABLES_DIR / "media_probe.py"
CONTROL_VIDEO_PROBE_SCRIPT_PATH = CALLABLES_DIR / "control_video_probe.py"


def to_dict(value: Any) -> dict[str, Any]:
    """Format an Airflow value into a dict."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        parsed = ast.literal_eval(text)
        if not isinstance(parsed, dict):
            raise AirflowFailException("Templated value must be a mapping")
        return parsed
    raise AirflowFailException(f"Unsupported templated value type: {type(value).__name__}")


def _validate_payload(
    payload: str | dict[str, Any],
) -> AVDatasetGenerationPayload:
    payload_dict = ast.literal_eval(payload) if isinstance(payload, str) else payload
    return AVDatasetGenerationPayload.model_validate(payload_dict)


def to_b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def join_output(output_directory: str, run_id: str, leaf: str) -> str:
    """Builds an output path from the base directory, run ID,
    and file or subdirectory name."""
    parts = [output_directory.rstrip("/")]
    if run_id:
        parts.append(re.sub(r"[^A-Za-z0-9._-]", "-", run_id.strip("/")))
    parts.append(leaf.strip("/"))
    return "/".join(parts)


def create_avdg_configs(
    payload: str | dict[str, Any],
    run_id: str = "",
) -> dict[str, str]:
    """Build the Curator config and the CDS ingest knobs from the
    validated payload."""

    # Validate and load the payload.
    validated = _validate_payload(payload)
    with open(CURATOR_CONFIG_PATH, encoding="utf-8") as config_file:
        curator_config = yaml.safe_load(config_file)

    # Setup paths, profiles, and embeddings.
    curator_output = join_output(validated.output_directory, run_id, "split")
    run_output = join_output(validated.output_directory, run_id, "")
    curator_config["input_video_path"] = validated.input_video_path
    curator_config["output_clip_path"] = curator_output

    input_bucket = bucket_of(validated.input_video_path) or "default"
    output_bucket = bucket_of(curator_output) or "default"
    curator_config["input_s3_profile_name"] = input_bucket
    curator_config["output_s3_profile_name"] = output_bucket
    curator_config["model_weights_path"] = "/config/models"
    curator_config["generate_embeddings"] = True
    curator_config["embedding_algorithm"] = "cosmos-embed1-224p"
    if validated.limit is not None:
        curator_config["limit"] = validated.limit
    else:
        curator_config.pop("limit", None)

    # Package configs and pod scripts.
    parquet_uri = join_output(validated.output_directory, run_id, "cds_ingest.parquet")
    curator_yaml = yaml.safe_dump(curator_config, sort_keys=False)
    result = {
        "curator_config_b64": to_b64(curator_yaml),
        "cds_ingest_script_b64": to_b64(CDS_INGEST_SCRIPT_PATH.read_text(encoding="utf-8")),
        "msc_credentials_script_b64": to_b64(
            MSC_CREDENTIALS_SCRIPT_PATH.read_text(encoding="utf-8")
        ),
        "media_probe_script_b64": to_b64(MEDIA_PROBE_SCRIPT_PATH.read_text(encoding="utf-8")),
        "control_video_probe_script_b64": to_b64(
            CONTROL_VIDEO_PROBE_SCRIPT_PATH.read_text(encoding="utf-8")
        ),
        "input_video_path": validated.input_video_path,
        "curator_output_dir": curator_output,
        "run_output_dir": run_output,
        "parquet_uri": parquet_uri,
        "collection_name": validated.collection_name,
        "pipeline": validated.pipeline,
        "ingest_mode": validated.ingest_mode,
        "query_video": validated.query_video or "",
        "search_mode": validated.search_mode,
        "top_k": str(validated.top_k),
        "pixi_env": validated.pixi_env,
        "object_store_endpoint": validated.object_store_endpoint or "",
        "job_timeout_seconds": str(validated.job_timeout_seconds),
    }

    # Configure query video processing.
    query_video = (validated.query_video or "").strip()
    query_url = urlsplit(query_video)
    local_query = not query_url.scheme and ("/" in query_video or "." in query_video)
    if validated.search_mode == "embedding" and (query_url.scheme == "s3" or local_query):
        query_output = join_output(validated.output_directory, run_id, "query_split")
        query_parent = (
            str(Path(query_video).parent) if local_query else query_video.rsplit("/", 1)[0]
        )
        if query_output.startswith(query_parent + "/") or query_parent.startswith(
            query_output + "/"
        ):
            raise AirflowFailException(
                "Curator requires the query video's parent prefix and its output prefix "
                "to be separate; change query_video or output_directory"
            )

        # Reuse settings for the query video.
        query_config = copy.deepcopy(curator_config)
        input_video_list_json_path = join_output(
            validated.output_directory, run_id, "query_input_videos.json"
        )
        query_config.update(
            input_video_path=query_parent,
            output_clip_path=query_output,
            input_s3_profile_name=query_url.netloc or "default",
            limit=1,
            input_video_list_json_path=input_video_list_json_path,
            input_video_list_s3_profile_name=output_bucket,
        )
        query_curator_config_uri = join_output(
            validated.output_directory, run_id, "query_pipeline_config.yaml"
        )
        result.update(
            query_video=query_video,
            query_curator_config_b64=to_b64(yaml.safe_dump(query_config, sort_keys=False)),
            query_curator_config_uri=query_curator_config_uri,
            query_curator_output_dir=query_output,
        )
    logger.info(
        "Generated Curator config input=%s output=%s collection=%s search_mode=%s",
        validated.input_video_path,
        curator_output,
        validated.collection_name,
        validated.search_mode,
    )
    return result


def build_curator_args(configs: Any = None, prepared_input: Any = None, **_context: Any) -> str:
    """Write the Curator configuration to storage."""
    configs = to_dict(configs)

    encoded = configs["curator_config_b64"]
    output_dir = configs["curator_output_dir"].rstrip("/")

    config = yaml.safe_load(base64.b64decode(encoded))
    query_encoded = configs.get("query_curator_config_b64")
    if query_encoded:
        query_video = configs["query_video"]
        query_config = yaml.safe_load(base64.b64decode(query_encoded))
        write_file_to_directory(
            query_config["input_video_list_json_path"], json.dumps([query_video]).encode()
        )
        write_file_to_directory(
            configs["query_curator_config_uri"],
            yaml.safe_dump(query_config, sort_keys=False).encode(),
        )
        logger.info("Staged a single-video Curator embedding pass for query %s", query_video)
    prepared = to_dict(prepared_input)
    paths = [item["video_path"] for item in prepared["videos"]]
    list_uri = f"{output_dir}/input_videos.json"
    write_file_to_directory(list_uri, json.dumps(paths, indent=2).encode())
    config["input_video_list_json_path"] = list_uri
    config["input_video_list_s3_profile_name"] = config.get("output_s3_profile_name", "default")

    config_uri = f"{output_dir}/{CONFIG_FILENAME}"
    write_file_to_directory(config_uri, yaml.safe_dump(config, sort_keys=False).encode("utf-8"))
    logger.info("Staged Curator config at %s", config_uri)
    return config_uri
