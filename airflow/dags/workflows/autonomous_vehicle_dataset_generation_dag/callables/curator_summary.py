# SPDX-License-Identifier: Apache-2.0

"""Verify Curator’s output files and update its summary using measured media properties."""

import json

from airflow.exceptions import AirflowFailException

from dags.shared.utils.msc_utils import (
    convert_msc_to_storage_url,
    ensure_msc_configured,
    is_file,
    list_directory,
    read_file_as_dict,
    write_file_to_directory,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.collect_results import (
    resolve_clip_location,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.config_generation import (
    to_dict,
)


@ensure_msc_configured
def prepare_curator_probe(configs, **_context):
    """Prepare a request to check Curator's output."""
    root = to_dict(configs)["curator_output_dir"].rstrip("/")

    uris = set()
    for obj in list_directory(root + "/metas/v0"):
        uri = convert_msc_to_storage_url(obj.key)
        if not uri.endswith(".json"):
            continue
        meta = read_file_as_dict(uri)
        _, files = resolve_clip_location(meta.get("clip_location") or "")
        files = [f for f in files if f.lower().endswith(".mp4")]
        if not files:
            raise AirflowFailException(f"Curator metadata has no output media: {uri}")
        uris.update(files)
        if meta.get("source_video"):
            uris.add(meta["source_video"])
    if not uris:
        raise AirflowFailException("Curator wrote no clip metadata")
    request_uri = root + "/curator_probe_request.json"
    result_uri = root + "/curator_probe_result.json"
    request = {
        "items": [{"id": uri, "uri": uri, "expected": {}} for uri in sorted(uris)],
        "embedding_uris": sorted(
            uri
            for obj in list_directory(root)
            if any(
                f"/{name}/" in (uri := convert_msc_to_storage_url(obj.key))
                for name in ("ce1_embd_224p_parquet", "ce1_embd_parquet")
            )
            and uri.endswith(".parquet")
        ),
    }
    write_file_to_directory(request_uri, json.dumps(request, indent=2).encode())
    return {"request_uri": request_uri, "result_uri": result_uri}


def media_properties(uri, results):
    """Retrieve a video’s probe result and check that decoding passed,
    the frame count is nonzero, and FPS is positive."""
    media = results.get(uri) or {}
    if not media.get("passed") or not media.get("frames") or (media.get("fps") or 0) <= 0:
        raise AirflowFailException(
            f"Curator media failed decoding: {uri}: {media.get('error', 'missing probe result')}"
        )
    return {
        "duration": media["frames"] / media["fps"],
        "frames": media["frames"],
        "bytes": media["bytes"],
    }


@ensure_msc_configured
def reconcile_curator_outputs(configs, **_context):
    """Update Curator’s metadata and summary using the probe’s measured results."""
    configs = to_dict(configs)
    root = configs["curator_output_dir"].rstrip("/")
    result_uri = root + "/curator_probe_result.json"

    probe_result = read_file_as_dict(result_uri) or {}
    embedded = probe_result.get("embedding_count")
    if type(embedded) is not int or embedded < 0:
        raise AirflowFailException("Curator probe result is missing a valid embedding count")

    results = {item["id"]: item for item in probe_result.get("items", [])}
    summary_uri = root + "/summary.json"
    summary = read_file_as_dict(summary_uri) if is_file(summary_uri) else {}
    sources, durations, count = {}, [], 0
    for obj in list_directory(root + "/metas/v0"):
        uri = convert_msc_to_storage_url(obj.key)
        if not uri.endswith(".json"):
            continue
        meta = read_file_as_dict(uri)
        location, files = resolve_clip_location(meta.get("clip_location") or "")
        files = [f for f in files if f.lower().endswith(".mp4")]
        if not files:
            raise AirflowFailException(f"Curator metadata has no output media: {uri}")
        measured = [media_properties(f, results) for f in files]
        meta["clip_location"] = location
        meta["clip_files"] = files
        meta["valid"] = True
        write_file_to_directory(uri, json.dumps(meta, indent=2).encode())
        durations.append(max(m["duration"] for m in measured))
        source = meta.get("source_video")
        if source and source not in sources:
            sources[source] = media_properties(source, results)
        count += 1
    if not count:
        raise AirflowFailException("Curator wrote no clip metadata")

    original = root + "/summary.curator.json"
    if not is_file(original):
        write_file_to_directory(original, json.dumps(summary, indent=2).encode())
    if sources:
        summary.update(
            num_input_videos=max(summary.get("num_input_videos") or 0, len(sources)),
            num_processed_videos=len(sources),
            total_video_duration=sum(m["duration"] for m in sources.values()),
            total_video_bytes=sum(m["bytes"] for m in sources.values()),
        )
    summary.update(
        total_clip_duration=sum(durations),
        max_clip_duration=max(durations),
        total_num_clips_passed=count,
        total_num_clips_transcoded=count,
        reconciliation="decoded_media",
    )
    timing_uri = root + "/pipeline_timing.json"
    if is_file(timing_uri):
        summary["pipeline_run_time"] = read_file_as_dict(timing_uri)["seconds"] / 60

    summary["total_num_clips_with_embeddings"] = embedded
    write_file_to_directory(summary_uri, json.dumps(summary, indent=2).encode())
    return {"summary_uri": summary_uri, "clips": count, "embedded": embedded}
