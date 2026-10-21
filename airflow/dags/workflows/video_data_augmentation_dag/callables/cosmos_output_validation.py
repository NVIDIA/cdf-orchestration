# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Output validation callable for Video Data Augmentation Cosmos augmentation."""

import ast
import logging
from typing import Any

import multistorageclient as msc
import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import CosmosOutputResult, CosmosTaskConfig
from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.utils.msc_utils import (
    COSMOS_OUTPUT_MP4_PATTERNS,
    convert_to_msc_url,
    is_file,
    list_directory,
    read_file_as_dict,
)


def _read_yaml_mapping(path: str) -> dict[str, Any]:
    try:
        with msc.open(path, "r") as config_file:
            config = yaml.safe_load(config_file)
    except Exception as exc:
        raise AirflowFailException(f"Error loading Cosmos config from {path}: {exc}") from exc
    if not isinstance(config, dict):
        raise AirflowFailException(f"Cosmos config at {path} must contain a YAML mapping")
    return config


def _configured_cosmos_evaluator_gates(config: dict[str, Any]) -> list[str]:
    evaluators = config.get("evaluators")
    if not isinstance(evaluators, list):
        raise AirflowFailException("Candidate config has no Cosmos Evaluator configuration")

    for entry in evaluators:
        if not isinstance(entry, dict):
            continue
        evaluator = entry.get("cosmos_evaluator")
        if not isinstance(evaluator, dict):
            continue
        if evaluator.get("enabled", True) is not True:
            continue
        checks = evaluator.get("checks")
        if not isinstance(checks, list):
            raise AirflowFailException("Candidate Cosmos Evaluator checks must be a list")
        gates = []
        for check in checks:
            if not isinstance(check, dict):
                raise AirflowFailException("Candidate Cosmos Evaluator check must be a mapping")
            if check.get("gate", False) is True:
                name = check.get("name")
                if not isinstance(name, str) or not name:
                    raise AirflowFailException(
                        "Candidate Cosmos Evaluator gate must have a non-empty name"
                    )
                gates.append(name)
        return gates

    raise AirflowFailException("Candidate config has no Cosmos Evaluator configuration")


def _cosmos_evaluator_rejection_reason(
    metadata: dict[str, Any], config: dict[str, Any]
) -> str | None:
    if "cosmos_evaluator" not in metadata:
        return None
    result = metadata["cosmos_evaluator"]
    if not isinstance(result, dict):
        return "cosmos_evaluator metadata is not a mapping"
    if result.get("operational_success") is not True:
        return "Cosmos Evaluator did not complete successfully"
    if result.get("quality_passed") is not True:
        return "Cosmos Evaluator quality gate did not pass"

    stream_key = result.get("stream_key")
    if not isinstance(stream_key, str) or not stream_key:
        return "Cosmos Evaluator metadata has no stream key"
    results_by_checker = result.get("checks")
    if not isinstance(results_by_checker, dict):
        return "Cosmos Evaluator metadata has no checker results"

    for gate_name in _configured_cosmos_evaluator_gates(config):
        checker_result = results_by_checker.get(gate_name)
        stream_result = checker_result.get(stream_key) if isinstance(checker_result, dict) else None
        if not isinstance(stream_result, dict) or stream_result.get("passed") is not True:
            return f"Cosmos Evaluator gate {gate_name!r} did not pass"
    return None


def validate_cosmos_outputs(
    cosmos_config: str | None = None,
    run_id: str = "",
    **context,
) -> dict[str, Any]:
    """Validate Video Data Augmentation Cosmos outputs and return normalized XCom metadata."""
    try:
        cosmos_task_config = CosmosTaskConfig.model_validate(ast.literal_eval(cosmos_config))
        prepared = require_prepared_input_from_xcom(context["ti"])
        cosmos_dir = str(URL(cosmos_task_config.output_directory) / run_id / "cosmos")

        try:
            actual_outputs = sorted(
                [obj.key for obj in list_directory(cosmos_dir, patterns=COSMOS_OUTPUT_MP4_PATTERNS)]
            )
        except Exception as e:
            logging.error("Error listing cosmos directory %s: %s", cosmos_dir, e)
            raise

        if not actual_outputs:
            raise AirflowFailException(
                "Cosmos augmentation produced no output videos. "
                "Expected one output per (input video, augmentation index). "
                f"Checked directory: {cosmos_dir}"
            )

        actual_output_set_msc = {convert_to_msc_url(p) for p in actual_outputs}
        videos = []
        missing_video_keys = []
        incomplete_candidate_keys = []
        rejected_candidate_keys = []
        for prepared_video in prepared.videos:
            for aug_idx in range(cosmos_task_config.num_augmentation):
                expected_output = f"{cosmos_dir}/{prepared_video.video_key}/{aug_idx}/output.mp4"
                expected_output_msc = convert_to_msc_url(expected_output)
                if expected_output_msc not in actual_output_set_msc:
                    missing_video_keys.append(f"{prepared_video.video_key}[{aug_idx}]")
                    continue
                candidate_key = f"{prepared_video.video_key}[{aug_idx}]"
                candidate_dir_msc = expected_output_msc.rsplit("/", 1)[0]
                missing_sidecars = [
                    filename
                    for filename in ("caption.txt", "metadata.json")
                    if not is_file(f"{candidate_dir_msc}/{filename}")
                ]
                if missing_sidecars:
                    incomplete_candidate_keys.append(
                        f"{candidate_key}: missing {', '.join(missing_sidecars)}"
                    )
                    continue
                try:
                    metadata = read_file_as_dict(f"{candidate_dir_msc}/metadata.json")
                    if isinstance(metadata, dict) and "cosmos_evaluator" in metadata:
                        config = _read_yaml_mapping(f"{candidate_dir_msc}/config.yaml")
                        rejection_reason = _cosmos_evaluator_rejection_reason(metadata, config)
                        if rejection_reason is not None:
                            rejected_candidate_keys.append(f"{candidate_key}: {rejection_reason}")
                            continue
                except AirflowFailException as e:
                    incomplete_candidate_keys.append(f"{candidate_key}: {e}")
                    continue
                videos.append(
                    {
                        "video_key": prepared_video.video_key,
                        "video_path": expected_output,
                    }
                )

        if missing_video_keys:
            logging.warning(
                "Cosmos augmentation produced outputs with missing expected video keys. "
                "Proceeding with available outputs only. "
                "Missing keys: %s (total missing: %d).",
                missing_video_keys,
                len(missing_video_keys),
            )
        if rejected_candidate_keys:
            logging.warning(
                "Cosmos Evaluator rejected candidates. Proceeding with passing outputs only. "
                "Rejected candidates: %s (total rejected: %d).",
                rejected_candidate_keys,
                len(rejected_candidate_keys),
            )
        if incomplete_candidate_keys:
            logging.warning(
                "Cosmos augmentation produced incomplete candidates. Proceeding with complete "
                "outputs only. Incomplete candidates: %s (total incomplete: %d).",
                incomplete_candidate_keys,
                len(incomplete_candidate_keys),
            )
        if not videos:
            if rejected_candidate_keys and incomplete_candidate_keys:
                raise AirflowFailException(
                    "Cosmos output validation found no complete candidates that passed "
                    "Cosmos Evaluator."
                )
            if rejected_candidate_keys:
                raise AirflowFailException(
                    "Cosmos output validation found no candidates that passed Cosmos Evaluator."
                )
            if incomplete_candidate_keys:
                raise AirflowFailException("Cosmos output validation found no complete candidates.")
            raise AirflowFailException(
                "Cosmos output validation found no matched outputs for expected video keys. "
                "Listed outputs exist, but none matched expected keys."
            )
        return CosmosOutputResult(length=len(videos), videos=videos).model_dump()

    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(f"Cosmos output validation failed: {e}") from e
