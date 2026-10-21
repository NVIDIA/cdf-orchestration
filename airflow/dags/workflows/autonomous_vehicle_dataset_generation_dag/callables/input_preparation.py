# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Preparee the list of videos Curator will process."""

from __future__ import annotations

import logging
from typing import Any

from airflow.exceptions import AirflowFailException

from dags.shared.utils.msc_utils import (
    VIDEO_PATTERNS,
    convert_msc_to_storage_url,
    is_file,
    list_directory,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.config_generation import (
    to_dict,
)

logger = logging.getLogger(__name__)


def validate_avdg_input_video(path: str, corpus_root: str) -> bool:
    """Only direct front-camera videos belong to the AV Dataset Generation corpus."""
    prefix = corpus_root.rstrip("/") + "/"
    if not path.startswith(prefix):
        return False
    parts = path[len(prefix) :].split("/")
    return (
        len(parts) == 2
        and parts[0] not in {"", ".", ".."}
        and parts[1] == "camera_front_wide_120fov.mp4"
    )


def prepare_avdg_input(
    payload: Any = None,
    **_context: Any,
) -> dict[str, Any]:
    """Build the list of videos Curator will process."""
    payload = to_dict(payload)
    corpus_root = str(payload.get("input_video_path") or "").rstrip("/")
    if not corpus_root:
        raise AirflowFailException("payload.input_video_path is required")

    if is_file(corpus_root):
        raise AirflowFailException("input_video_path must be a dataset directory")
    storage_root = convert_msc_to_storage_url(corpus_root)
    paths = sorted(
        convert_msc_to_storage_url(obj.key)
        for obj in list_directory(corpus_root, patterns=VIDEO_PATTERNS)
    )
    paths = [path for path in paths if validate_avdg_input_video(path, storage_root)]
    if not paths:
        raise AirflowFailException(
            "Input directory structure requires <scene-id>/camera_front_wide_120fov.mp4 "
            f"directly beneath input_video_path: {corpus_root}"
        )
    query = payload.get("query_video")
    if (
        payload.get("search_mode", "embedding") == "embedding"
        and query
        and ("/" in query or "." in query)
        and not is_file(query)
    ):
        raise AirflowFailException(f"Embedding query video does not exist: {query}")
    limit = payload.get("limit")
    if limit:
        paths = paths[: int(limit)]
    missing = [path for path in paths if not is_file(path)]
    if missing:
        raise AirflowFailException(f"Input videos are missing or unreadable: {missing}")
    videos = [
        {
            "video_key": path[len(storage_root.rstrip("/")) + 1 :].split("/")[0],
            "video_path": path,
        }
        for path in paths
    ]

    distinct = len({video["video_key"] for video in videos})
    logger.info(
        "Prepared %d video file(s) in %d clip(s) from %s",
        len(videos),
        distinct,
        corpus_root,
    )
    return {"length": len(videos), "videos": videos}
