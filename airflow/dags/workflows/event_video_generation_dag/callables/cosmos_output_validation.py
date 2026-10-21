# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Best-effort artifact validation for Event Video Generation Cosmos3 augmentation."""

import ast
import logging
from typing import Any

import multistorageclient as msc
import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.utils.msc_utils import is_file
from dags.workflows.event_video_generation_dag.models import (
    EventVideoGenerationAugmentationOutputResult,
    EventVideoGenerationCosmosTaskConfig,
)


def _read_yaml_mapping(path: str) -> dict[str, Any]:
    try:
        with msc.open(path, "r") as f:
            value = yaml.safe_load(f)
    except Exception as e:
        raise AirflowFailException(
            f"Unable to read Event Video Generation artifact {path}: {e}"
        ) from e
    if not isinstance(value, dict):
        raise AirflowFailException(f"Event Video Generation artifact {path} must contain a mapping")
    return value


def _enabled_cosmos_evaluator(config: dict[str, Any]) -> dict[str, Any] | None:
    evaluators = config.get("evaluators")
    if not isinstance(evaluators, list):
        return None
    for entry in evaluators:
        evaluator = entry.get("cosmos_evaluator") if isinstance(entry, dict) else None
        if isinstance(evaluator, dict) and evaluator.get("enabled", True) is True:
            return evaluator
    return None


def _configured_cosmos_evaluator_gates(config: dict[str, Any]) -> list[str]:
    evaluator = _enabled_cosmos_evaluator(config)
    if evaluator is None:
        raise AirflowFailException("Candidate config has no enabled Cosmos Evaluator configuration")
    checks = evaluator.get("checks")
    if not isinstance(checks, list):
        raise AirflowFailException("Candidate Cosmos Evaluator checks must be a list")
    gates: list[str] = []
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


def _cosmos_evaluator_rejection_reason(
    metadata: dict[str, Any], config: dict[str, Any]
) -> str | None:
    result = metadata.get("cosmos_evaluator")
    if result is None:
        if _enabled_cosmos_evaluator(config) is not None:
            return "Cosmos Evaluator metadata is missing"
        return None
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


def _parse_config(value: str | dict[str, Any] | None) -> EventVideoGenerationCosmosTaskConfig:
    raw = ast.literal_eval(value) if isinstance(value, str) else value
    return EventVideoGenerationCosmosTaskConfig.model_validate(raw or {})


def validate_event_video_generation_cosmos_outputs(
    cosmos_config: str | dict[str, Any] | None = None,
    run_id: str = "",
    **context,
) -> dict[str, Any]:
    """Return complete, passing augmentations and fail only when none remain."""
    try:
        task_config = _parse_config(cosmos_config)
        prepared = require_prepared_input_from_xcom(context["ti"])
        output_root = str(URL(task_config.output_directory) / run_id / "cosmos")
        augmentations = []
        videos = []

        for prepared_image in prepared.videos:
            for augmentation_index in range(task_config.num_augmentation):
                output_base = f"{output_root}/{prepared_image.video_key}/{augmentation_index}"
                paths = {
                    "config": f"{output_base}/config.yaml",
                    "video": f"{output_base}/output.mp4",
                    "caption": f"{output_base}/caption.txt",
                    "metadata": f"{output_base}/metadata.json",
                }
                missing_artifacts = [
                    f"{artifact_name}: {artifact_path}"
                    for artifact_name, artifact_path in paths.items()
                    if not is_file(artifact_path)
                ]
                if missing_artifacts:
                    logging.warning(
                        "Skipping incomplete Event Video Generation augmentation %s[%d]. "
                        "Missing artifacts: %s",
                        prepared_image.video_key,
                        augmentation_index,
                        missing_artifacts,
                    )
                    continue

                try:
                    config = _read_yaml_mapping(paths["config"])
                    metadata = _read_yaml_mapping(paths["metadata"])
                    rejection_reason = _cosmos_evaluator_rejection_reason(metadata, config)
                except AirflowFailException as e:
                    logging.warning(
                        "Skipping invalid Event Video Generation augmentation %s[%d]: %s",
                        prepared_image.video_key,
                        augmentation_index,
                        e,
                    )
                    continue
                if rejection_reason is not None:
                    logging.warning(
                        "Skipping Event Video Generation augmentation %s[%d] rejected by "
                        "Cosmos Evaluator: %s",
                        prepared_image.video_key,
                        augmentation_index,
                        rejection_reason,
                    )
                    continue

                augmentations.append(
                    {
                        "input_key": prepared_image.video_key,
                        "augmentation_index": augmentation_index,
                        "video_path": paths["video"],
                        "caption_path": paths["caption"],
                        "metadata_path": paths["metadata"],
                    }
                )
                videos.append(
                    {
                        "video_key": prepared_image.video_key,
                        "video_path": paths["video"],
                    }
                )

        if not augmentations:
            raise AirflowFailException(
                "No complete Event Video Generation augmentations passed output validation"
            )

        return EventVideoGenerationAugmentationOutputResult(
            length=len(augmentations),
            augmentations=augmentations,
            videos=videos,
        ).model_dump()
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Event Video Generation Cosmos output validation failed: {e}"
        ) from e
