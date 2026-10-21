# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Collect and validate the workflow’s Cosmos Dataset Search results."""

from __future__ import annotations

import json
import logging
from typing import Any

from airflow.exceptions import AirflowFailException

from dags.shared.utils.msc_utils import (
    copy_file,
    is_file,
    list_directory,
    read_file_as_dict,
    write_file_to_directory,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.cds_ingest import (
    retrievals_of,
    video_identity,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.config_generation import (
    to_dict,
)

logger = logging.getLogger(__name__)

RAW_RESPONSE_FILENAME = "cds_search_response.json"
RESULT_FILENAME = "search_result.json"
RESULTS_DIRNAME = "results"


def resolve_clip_location(location: str) -> tuple[str, list[str]]:
    """Resolve a curated clip file."""
    return location, [location] if location and is_file(location) else []


def is_query_hit(hit: dict, query: dict) -> bool:
    """Check whether a search result belongs to the input query video"""
    return bool(
        hit.get("is_query")
        or (query.get("query_span_uuid") and hit.get("span_uuid") == query["query_span_uuid"])
        or any(
            hit.get("source_video")
            and query.get(key)
            and video_identity(hit["source_video"]) == video_identity(query[key])
            for key in ("query_source_video", "query_video")
        )
    )


def summarize_retrievals(
    response: Any,
    query_span_uuid: str,
    top_k: int,
    *,
    query_source_video: str = "",
    query_video: str = "",
) -> list[dict[str, Any]]:
    """Turn the raw CDS response into a compact, ranked list of search hits."""
    query = {
        "query_span_uuid": query_span_uuid,
        "query_source_video": query_source_video,
        "query_video": query_video,
    }
    ranked: list[dict[str, Any]] = []
    for rank, hit in enumerate(retrievals_of(response), start=1):
        meta = hit.get("metadata") or {}
        span = str(meta.get("span_uuid") or hit.get("id") or "")
        if is_query_hit(
            {
                "span_uuid": span,
                "source_video": meta.get("source_video"),
                "is_query": hit.get("is_query") or meta.get("is_query"),
            },
            query,
        ):
            continue
        start_ns, end_ns = meta.get("start_ns"), meta.get("end_ns")
        duration_seconds = (
            (end_ns - start_ns) / 1e9
            if isinstance(start_ns, (int, float)) and isinstance(end_ns, (int, float))
            else None
        )
        resolution = f"{meta.get('width')}x{meta.get('height')}" if meta.get("width") else None
        ranked.append(
            {
                "rank": rank,
                "score": hit.get("score"),
                "span_uuid": span,
                "is_query": bool(query_span_uuid) and span == query_span_uuid,
                "source_video": meta.get("source_video"),
                "clip_location": meta.get("clip_location"),
                "start_ns": start_ns,
                "end_ns": end_ns,
                "duration_seconds": duration_seconds,
                "num_frames": meta.get("num_frames"),
                "resolution": resolution,
            }
        )
    return ranked[:top_k] if top_k > 0 else ranked


def collect_autonomous_vehicle_dataset_generation_results(
    payload: Any = None,
    configs: Any = None,
    run_id: str = "",
    **_context: Any,
) -> dict[str, Any]:
    """Turn the raw search response into the retrieval output used by the rest of the workflow."""
    payload = to_dict(payload)
    configs = to_dict(configs)

    result_prefix = configs["run_output_dir"].rstrip("/")

    raw_uri = f"{result_prefix}/{RAW_RESPONSE_FILENAME}"
    if not is_file(raw_uri):
        raise AirflowFailException(
            f"Ingest did not write {raw_uri}; there is no search response to collect"
        )
    envelope = read_file_as_dict(raw_uri)
    if not isinstance(envelope, dict):
        raise AirflowFailException(f"{raw_uri} did not contain a JSON object")

    top_k = int(payload.get("top_k") or 0)
    query_span = str(envelope.get("query_span_uuid") or "")
    ranked = summarize_retrievals(
        envelope.get("search"),
        query_span,
        top_k,
        query_source_video=envelope.get("query_source_video"),
        query_video=envelope.get("query_video"),
    )
    if not ranked and payload.get("search_mode") != "none":
        raise AirflowFailException(
            f"{raw_uri} contained no retrievals after excluding the query video"
        )

    for hit in ranked:
        reported = str(hit.get("clip_location") or "")
        resolved, files = resolve_clip_location(reported)
        hit["clip_location"] = resolved
        hit["clip_files"] = files
        if not files:
            logger.warning("No objects found for clip location %s", reported)

    document = {
        "search_mode": envelope.get("search_mode", "embedding"),
        "query_video": envelope.get("query_video"),
        "query_span_uuid": query_span or None,
        "query_source_video": envelope.get("query_source_video"),
        "top_k": top_k,
        "collection_name": envelope.get("collection_name"),
        "collection_id": envelope.get("collection_id"),
        "embedding_family": envelope.get("embedding_family", "ce1"),
        "ingest_mode": envelope.get("ingest_mode"),
        "indexed_spans": envelope.get("span_count"),
        "hits": ranked,
    }
    result_uri = f"{result_prefix}/{RESULT_FILENAME}"
    write_file_to_directory(result_uri, json.dumps(document, indent=2, default=str).encode())

    collected: list[dict[str, Any]] = []
    if payload.get("collect_result_clips", True):
        collected = _copy_result_clips(ranked, f"{result_prefix}/{RESULTS_DIRNAME}")

    summary = {
        "collected": True,
        "result_uri": result_uri,
        "indexed_spans": envelope.get("span_count"),
        "hits": len(ranked),
        "clips_copied": sum(entry["file_count"] for entry in collected),
        "unresolved_hits": [h["rank"] for h in ranked if not h["clip_files"]],
    }
    logger.info("Collected retrieval result: %s", summary)
    return summary


def _copy_result_clips(ranked: list[dict[str, Any]], results_prefix: str) -> list[dict[str, Any]]:
    """Copy each retrieved video into results/."""
    collected: list[dict[str, Any]] = []
    failures = 0
    for hit in ranked:
        files = hit.get("clip_files") or []
        if not files:
            continue
        target = f"{results_prefix}/rank-{int(hit['rank']):02d}-{hit['span_uuid']}"
        try:
            copy_file(files[0], f"{target}/{files[0].rsplit('/', 1)[-1]}")
        except Exception as exc:
            logger.warning("Could not copy clip for rank %s: %s", hit.get("rank"), exc)
            failures += 1
            continue
        collected.append({"rank": hit["rank"], "destination": target, "file_count": len(files)})
    if failures:
        raise AirflowFailException(f"{failures} / {len(ranked)} clips could not be copied.")
    return collected


def _has_objects(prefix: str) -> bool:
    return next(iter(list_directory(prefix, max_files=1)), None) is not None


def _clip_exists(location: str) -> bool:
    """Whether a curated clip file exists."""
    if not location:
        return False
    return is_file(location)


def validate_avdg_pipeline_outputs(
    payload: Any = None,
    configs: Any = None,
    run_id: str = "",
    **_context: Any,
) -> dict[str, Any]:
    """Check the ranked result exists and that every hit is retrievable."""
    payload = to_dict(payload)
    configs = to_dict(configs)

    result_prefix = configs["run_output_dir"].rstrip("/")

    result_uri = f"{result_prefix}/{RESULT_FILENAME}"
    if not is_file(result_uri):
        raise AirflowFailException(
            f"Ingest reported success but {result_uri} is missing. The run produced "
            "no retrieval result."
        )

    document = read_file_as_dict(result_uri)
    if not isinstance(document, dict):
        raise AirflowFailException(f"{result_uri} did not contain a JSON object")

    if payload.get("search_mode") == "none":
        if document.get("search_mode") != "none" or not document.get("indexed_spans"):
            raise AirflowFailException("Ingest-only run has no indexed spans")
        return {"validated": True, "result_uri": result_uri, "hits": 0}

    hits = document.get("hits") or []
    if not hits:
        raise AirflowFailException(f"{result_uri} contains no hits; the query returned nothing")

    top_k = int(payload.get("top_k") or 0)
    if top_k and len(hits) > top_k:
        raise AirflowFailException(
            f"{result_uri} has {len(hits)} hits, more than the requested top_k={top_k}"
        )

    unresolved = [
        hit.get("rank") for hit in hits if not _clip_exists(str(hit.get("clip_location") or ""))
    ]
    if unresolved:
        raise AirflowFailException(
            f"{len(unresolved)} of {len(hits)} hits have no retrievable clip "
            f"(ranks {unresolved}); check clip file paths and storage access."
        )

    if payload.get("collect_result_clips", True):
        collected = f"{result_prefix}/{RESULTS_DIRNAME}"
        if not _has_objects(collected):
            raise AirflowFailException(f"collect_result_clips is enabled but {collected} is empty")

        missing = [
            hit.get("rank")
            for hit in hits
            if hit.get("clip_files")
            and not _has_objects(f"{collected}/rank-{int(hit['rank']):02d}-{hit.get('span_uuid')}")
        ]
        if missing:
            raise AirflowFailException(
                f"{len(missing)} of {len(hits)} hits have no clips under {collected} "
                f"(ranks {missing}). The copy step reported success but produced "
                "nothing for them."
            )

    summary = {
        "validated": True,
        "result_uri": result_uri,
        "hits": len(hits),
        "query_span_uuid": document.get("query_span_uuid"),
        "self_match_present": any(hit.get("is_query") for hit in hits),
    }
    logger.info("Validated retrieval result: %s", summary)
    return summary
