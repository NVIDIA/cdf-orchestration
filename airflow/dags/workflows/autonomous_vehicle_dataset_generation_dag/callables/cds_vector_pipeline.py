# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prepare the CDS 1.2 pipeline for Curator vectors, then launch its stock server.

Executed inside the CDS image; no Airflow imports or additional dependencies.
"""

import copy
import os
import sys
from pathlib import Path

import yaml


def vector_only_pipeline(source: dict) -> dict:
    """Keep CDS's vector routes, storage, and ranking; remove remote embedders."""
    result = copy.deepcopy(source)
    for section, removed, router in (
        ("index", {"embedder"}, "EmbeddingDocuments"),
        (
            "query",
            {"text_embedder", "video_embedder", "session_segment_embedder"},
            "EmbeddingQueries",
        ),
    ):
        pipeline = result[section]["pipeline"]
        components = pipeline["components"]
        for name in removed:
            # Fail visibly if the pinned image's template contract changes.
            del components[name]
        components["router"] = {"type": f"avdg_cds_vectors.{router}"}
        pipeline["connections"] = [
            connection
            for connection in pipeline["connections"]
            if not any(connection[end].split(".")[0] in removed for end in ("sender", "receiver"))
        ]
    if "COSMOS_EMBED" in yaml.safe_dump(result):
        raise ValueError("Vector-only CDS pipeline still references an embedding endpoint")
    return result


def main() -> None:
    source = Path("/app/src/visual_search/pipelines/cosmos_video_search_milvus.mustache")
    destination = Path(os.environ["PIPELINES_DIR"])
    destination.mkdir(parents=True, exist_ok=True)
    pipeline = vector_only_pipeline(yaml.safe_load(source.read_text()))
    (destination / source.name).write_text(yaml.safe_dump(pipeline, sort_keys=False))
    os.execv(
        sys.executable,
        [
            sys.executable,
            "/app/src/wrappers/gunicorn_wrapper.py",
            "src/visual_search/main.py",
            "app",
        ],
    )


if __name__ == "__main__":
    main()
