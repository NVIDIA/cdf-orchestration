# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Input preparation callable for Video Data Augmentation DAGs."""

import ast
import logging
from typing import Any

from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import InputPreparationResult
from dags.shared.utils.msc_utils import (
    VIDEO_PATTERNS,
    convert_msc_to_storage_url,
    is_file,
    list_directory,
)
from dags.shared.utils.video_input_utils import (
    derive_video_key,
    normalize_directory_path,
)
from dags.workflows.video_data_augmentation_dag.models import VideoDataAugmentationDagConfig


def prepare_video_data_augmentation_input(
    payload: str = "",
    run_id: str = "",
    **context,
) -> dict[str, Any]:
    """
    Prepare video input by inspecting ``input_path`` in storage.

    1. If ``input_path`` is a file (MSC ``is_file``): one video.
    2. Else: treat ``input_path`` as a directory prefix and list videos with
       ``VIDEO_PATTERNS``. Fails if no matching files are found.

    A positive ``max_videos`` bounds listing and truncates ``videos`` (default 1).
    ``None`` and nonpositive values process all matching videos.
    """
    try:
        input_data = VideoDataAugmentationDagConfig.model_validate(ast.literal_eval(payload))
        max_files = input_data.max_videos
        raw_path = input_data.input_path

        if is_file(raw_path):
            video_url = URL(raw_path)
            input_dir = normalize_directory_path(str(video_url.parent))
            paths = [convert_msc_to_storage_url(raw_path)]
            logging.info("prepare_input: single file at %s", raw_path)
        else:
            folder_path = normalize_directory_path(raw_path)
            input_dir = folder_path
            objs = list(
                list_directory(
                    directory_url=folder_path,
                    patterns=VIDEO_PATTERNS,
                    max_files=max_files,
                )
            )
            paths = sorted([convert_msc_to_storage_url(obj.key) for obj in objs])
            if not paths:
                raise AirflowFailException(
                    f"No video files found under directory {folder_path!r} "
                    f"(patterns={list(VIDEO_PATTERNS)})."
                )

        paths = paths[: input_data.max_videos]
        video_keys = [derive_video_key(video_path=p, input_path=input_dir) for p in paths]
        video_count = len(paths)

        logging.info(
            "Input preparation (run_id=%s): count=%s video file(s)",
            run_id,
            video_count,
        )
        return InputPreparationResult(
            length=video_count,
            videos=[
                {"video_key": video_key, "video_path": path}
                for video_key, path in zip(video_keys, paths)
            ],
        ).model_dump()

    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(f"Input preparation failed: {e}") from e
