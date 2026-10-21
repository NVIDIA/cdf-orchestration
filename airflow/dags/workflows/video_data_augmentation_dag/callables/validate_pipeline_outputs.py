# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Video Data Augmentation end-of-pipeline output validation callable."""

from __future__ import annotations

import ast
import logging
from typing import Any

from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.utils.msc_utils import (
    VIDEO_PATTERNS,
    is_file,
    list_directory,
)
from dags.shared.utils.video_input_utils import (
    extract_format_from_path,
    get_relative_storage_path,
    normalize_directory_path,
)
from dags.workflows.video_data_augmentation_dag.models import VideoDataAugmentationDagConfig

REQUIRED_COSMOS_FILES = ("output.mp4", "caption.txt", "metadata.json")


class PipelineOutputValidationError(Exception):
    """Raised when required Video Data Augmentation outputs are missing or invalid."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


def _run_base_url(output_directory: str, run_id: str) -> str:
    return str(URL(output_directory.rstrip("/")) / run_id)


def discover_cosmos_run_entries(output_directory: str, run_id: str) -> list[tuple[str, int]]:
    """Return sorted ``(video_key, augmentation_idx)`` entries under ``.../cosmos/``."""
    cosmos_dir = normalize_directory_path(
        str(URL(_run_base_url(output_directory, run_id)) / "cosmos")
    )
    run_dirs: set[tuple[str, int]] = set()
    try:
        for obj in list_directory(cosmos_dir):
            rel = get_relative_storage_path(obj.key, cosmos_dir)
            if not rel:
                continue
            parts = rel.split("/")
            if len(parts) >= 2 and parts[1].isdigit():
                run_dirs.add((parts[0], int(parts[1])))
    except Exception:
        return []
    return sorted(run_dirs)


def _prefix_has_objects(directory_url: str) -> bool:
    directory_url = normalize_directory_path(directory_url)
    try:
        return next(iter(list_directory(directory_url, max_files=1)), None) is not None
    except Exception:
        return False


def verify_cosmos_run_files(
    output_directory: str,
    run_id: str,
    video_key: str,
    aug_idx: int,
) -> list[str]:
    """Return missing required Cosmos artifact paths for one run directory."""
    run_dir = str(URL(output_directory.rstrip("/")) / run_id / "cosmos" / video_key / str(aug_idx))
    missing = []
    for fname in REQUIRED_COSMOS_FILES:
        file_path = f"{run_dir.rstrip('/')}/{fname}"
        if not is_file(file_path):
            missing.append(file_path)
    return missing


def verify_extra_output_paths(output_directory: str, run_id: str) -> list[str]:
    """Check auto-labeling output prefixes."""
    missing = []
    base = _run_base_url(output_directory, run_id)

    if not _prefix_has_objects(f"{base}/auto_labeling_input"):
        missing.append(f"{base}/auto_labeling_input/ (no objects)")
    if not _prefix_has_objects(f"{base}/auto_labeling_output"):
        missing.append(f"{base}/auto_labeling_output/ (no objects)")
    if not _prefix_has_objects(f"{base}/training_export/auto_labeling_input"):
        missing.append(f"{base}/training_export/auto_labeling_input/ (no objects)")
    if not _prefix_has_objects(f"{base}/training_export/auto_labeling_output"):
        missing.append(f"{base}/training_export/auto_labeling_output/ (no objects)")
    return missing


def get_expected_video_count(input_path: str, max_videos: int | None) -> int:
    """Return expected processed video count for directory inputs."""
    input_dir = normalize_directory_path(input_path)
    video_count = sum(1 for _ in list_directory(input_dir, patterns=VIDEO_PATTERNS))
    if not video_count:
        raise PipelineOutputValidationError(
            [f"No video files found in input directory {input_path}"]
        )
    if max_videos and max_videos > 0:
        return min(max_videos, video_count)
    return video_count


def validate_pipeline_outputs(payload: dict[str, Any], run_id: str) -> None:
    """Validate artifacts for the successful Video Data Augmentation candidates."""
    output_directory = (payload.get("output_directory") or "").strip()
    if not output_directory:
        raise PipelineOutputValidationError(["Payload has no output_directory"])

    input_path = payload.get("input_path") or ""
    max_videos = payload.get("max_videos")
    cosmos_cfg = payload.get("cosmos") or {}
    num_augmentation = max(1, int(cosmos_cfg.get("num_augmentation", 1)))

    run_entries = discover_cosmos_run_entries(output_directory, run_id)
    if not run_entries:
        raise PipelineOutputValidationError(["No cosmos output directories found for any video"])

    if extract_format_from_path(input_path) == "file":
        num_expected_videos = 1
    else:
        num_expected_videos = get_expected_video_count(input_path, max_videos)

    expected_run_dirs = num_expected_videos * num_augmentation
    if len(run_entries) != expected_run_dirs:
        raise PipelineOutputValidationError(
            [
                "Expected "
                f"{expected_run_dirs} cosmos run directory(ies) "
                f"({num_expected_videos} video(s) x {num_augmentation} augmentation(s)), "
                f"found {len(run_entries)}"
            ]
        )

    successful_run_entries: list[tuple[str, int]] = []
    failed_run_entries: list[tuple[str, int]] = []
    for video_key, aug_idx in run_entries:
        if verify_cosmos_run_files(output_directory, run_id, video_key, aug_idx):
            failed_run_entries.append((video_key, aug_idx))
        else:
            successful_run_entries.append((video_key, aug_idx))

    if failed_run_entries:
        logging.warning(
            "Skipping %d incomplete Cosmos augmentation output(s) during Video Data "
            "Augmentation final validation. Failed entries: %s",
            len(failed_run_entries),
            failed_run_entries,
        )

    if not successful_run_entries:
        raise PipelineOutputValidationError(
            ["No successful cosmos outputs found for any video/augmentation index"]
        )

    errors = verify_extra_output_paths(output_directory, run_id)

    if errors:
        raise PipelineOutputValidationError(errors)


def validate_video_data_augmentation_pipeline_outputs(
    payload: str | dict[str, Any] | None = None,
    run_id: str = "",
    **context,
) -> None:
    """Validate Video Data Augmentation storage outputs for the current run."""
    del context
    try:
        if isinstance(payload, dict):
            config = VideoDataAugmentationDagConfig.model_validate(payload)
        else:
            config = VideoDataAugmentationDagConfig.model_validate(
                ast.literal_eval(payload or "{}")
            )
        validate_pipeline_outputs(config.model_dump(), run_id)
    except PipelineOutputValidationError as e:
        raise AirflowFailException(
            "Pipeline output validation failed: " + "; ".join(e.errors)
        ) from e
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(f"Pipeline output validation failed: {e}") from e
