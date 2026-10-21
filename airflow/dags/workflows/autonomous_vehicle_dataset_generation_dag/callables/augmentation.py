# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prepare Cosmos3-Nano RGB or WSM video transfer."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml
from airflow.exceptions import AirflowFailException

from dags.shared.task_groups.cosmos import load_cosmos_base_config
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.msc_utils import (
    copy_file,
    delete_path,
    ensure_msc_configured,
    is_file,
    read_file_as_dict,
    write_file_to_directory,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.collect_results import (
    validate_avdg_pipeline_outputs,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.config_generation import (
    to_dict,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.evaluator_lifecycle import (
    configure_physical_plausibility,
    physical_plausibility_passed,
    validate_physical_plausibility_result,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.wsm import (
    apply_wsm_probe_results,
    prepare_wsm_controls,
    wsm_probe_items,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.models.payload import (
    COSMOS_MODEL,
    Cosmos3NanoTaskConfig,
)

logger = logging.getLogger(__name__)

FINALIZE_OUTPUTS_TASK_ID = "finalize_augmentation_outputs"
WSM_PROBE_TASK_ID = "probe_wsm_controls"


def select_augmentation_tasks(task_ids: list[str], **context: Any) -> list[str]:
    """Determine the next augmentation tasks to run."""
    payload = context["ti"].xcom_pull(task_ids="validate_payload") or {}
    if payload.get("cosmos", {}).get("enabled"):
        return task_ids
    return ["skip_augmentation"]


def select_clips(document: dict, config: Cosmos3NanoTaskConfig) -> list[dict]:
    """Select highest-ranked distinct retrieved clips, limited by ``cosmos.num_clips``."""
    selected, seen = [], set()
    for hit in sorted(document.get("hits") or [], key=lambda item: item["rank"]):
        span = hit.get("span_uuid")
        if not span or span in seen:
            continue

        files = [
            uri for uri in hit.get("clip_files", []) if urlsplit(uri).path.lower().endswith(".mp4")
        ]

        if not files:
            continue

        duration = hit.get("duration_seconds")
        minimum_duration = (config.num_frames - 1) / config.fps
        if duration is None or duration + 1e-6 < minimum_duration:
            continue

        selected.append(
            {
                key: hit.get(key)
                for key in (
                    "rank",
                    "span_uuid",
                    "source_video",
                    "start_ns",
                    "end_ns",
                    "duration_seconds",
                )
            }
            | {
                "rgb": files[0],
                "camera": config.camera,
            }
        )
        seen.add(span)
        if len(selected) == config.num_clips:
            break

    if config.num_clips is None:
        if not selected:
            raise AirflowFailException("Augmentation found no eligible retrievals")
        return selected

    if len(selected) != config.num_clips:
        raise AirflowFailException(
            f"Requested {config.num_clips} clips but found {len(selected)} eligible retrievals"
        )
    return selected


def variant_seed(config: Cosmos3NanoTaskConfig, index: int, variant: int) -> int | None:
    """Give each generated clip a reproducible random seed."""
    if config.seed is None:
        return None
    return config.seed + index * config.num_augmentation + variant


def transfer_config(
    source: dict,
    config: Cosmos3NanoTaskConfig,
    endpoint: str,
    output_base: str,
    seed: int | None,
    *,
    ti: Any = None,
) -> dict:
    """Build RGB transfer or transfer with an aligned WSM control video."""
    generation_config = load_cosmos_base_config(
        base_config_path=None,
        default_path=Path(__file__).resolve().parents[1] / "configs/cosmos3_nano_config.yaml",
        description="Cosmos3-Nano generation configuration",
    )
    endpoints = generation_config["endpoints"]
    transfer_endpoint = next(item for item in endpoints if item["role"] == "video_transfer")
    transfer_endpoint.update(
        url=endpoint.rstrip("/") + "/videos",
        timeout=config.timeout_seconds,
    )
    generation_config["data"] = [
        {
            "inputs": {"rgb": source["rgb"]},
            "output": {
                "video": f"{output_base}/output.mp4",
                "metadata": f"{output_base}/metadata.json",
                "caption": f"{output_base}/caption.txt",
            },
        }
    ]
    generation_config["captioning"] = {"llm": {"text": config.prompt}}
    parameters = generation_config["augmentation"]["parameters"]
    parameters.update(
        size=config.size,
        num_frames=config.num_frames,
        fps=config.fps,
        num_inference_steps=config.num_inference_steps,
        guidance_scale=config.guidance_scale,
        seed=seed,
    )
    extra = parameters["extra_params"]
    extra.update(num_video_frames_per_chunk=config.num_frames, max_frames=config.num_frames)
    if config.control_mode == "wsm":
        generation_config["data"][0]["inputs"]["controls"] = {"wsm": source["wsm"]["uri"]}
    else:
        extra.pop("wsm", None)
        extra["edge"] = {"preset_edge_threshold": "high"}
        # Permit appearance changes from frame zero while retaining edge guidance.
        extra["num_first_chunk_conditional_frames"] = 0
    evaluator_config = generation_config["evaluators"][0]["cosmos_evaluator"]["config"]
    evaluator_config["plausibility_threshold"] = config.plausibility_threshold
    evaluator_config["bad_threshold"] = 1.0 - evaluator_config.pop("plausibility_threshold")
    configure_physical_plausibility(generation_config, ti)
    return generation_config


def _runtime(context: dict) -> str:
    configs = context["ti"].xcom_pull(task_ids="generate_configs")
    return configs["run_output_dir"].rstrip("/") + "/cosmos3-nano"


def _retrieval_result_uri(context: dict) -> str:
    collected = context["ti"].xcom_pull(task_ids="collect_results")
    return collected["result_uri"]


@ensure_msc_configured
def prepare_wsm_probe(cosmos_config=None, **context) -> list[str]:
    """Validate retrieved RGB videos or prepare aligned WSM control windows.

    Both modes share the existing probe task IDs; RGB needs no WSM manifest.
    """
    root = _runtime(context)
    config = Cosmos3NanoTaskConfig.model_validate(to_dict(cosmos_config))
    document = read_file_as_dict(_retrieval_result_uri(context)) or {}
    selected = select_clips(document, config)
    if config.control_mode == "rgb":
        matched = [
            source
            | {
                "rgb": f"{root}/controls/clip-{index:03d}-rgb-{config.fps}fps.mp4",
                "rgb_metadata": {"source_video": source["rgb"]},
            }
            for index, source in enumerate(selected)
        ]
        items = [
            {
                "id": source["rgb"],
                "kind": "rgb_video",
                "uri": source["rgb_metadata"]["source_video"],
                "output_uri": source["rgb"],
                "expected": {"fps": config.fps},
                "minimum_frames": config.num_frames,
            }
            for source in matched
        ]
    else:
        matched = prepare_wsm_controls(selected, config)
        items = wsm_probe_items(matched, config, root)
    write_file_to_directory(root + "/wsm_selection.json", json.dumps(matched, indent=2).encode())
    write_file_to_directory(
        f"{root}/wsm_probe_request.json", json.dumps({"items": items}, indent=2).encode()
    )
    context["ti"].xcom_push(key="request_uri", value=f"{root}/wsm_probe_request.json")
    context["ti"].xcom_push(key="result_uri", value=f"{root}/wsm_probe_result.json")
    return [WSM_PROBE_TASK_ID]


def _selected_sources(root: str) -> list[dict]:
    matched = read_file_as_dict(root + "/wsm_selection.json")
    if not matched:
        raise AirflowFailException("prepare_wsm_probe did not record the control selection")
    result_uri = f"{root}/wsm_probe_result.json"
    results = read_file_as_dict(result_uri) or {}
    if matched[0].get("rgb_metadata"):
        videos = {item.get("id"): item for item in results.get("items", [])}
        for source in matched:
            video = videos.get(source["rgb"], {})
            if not video.get("passed") or video.get("uri") != source["rgb"]:
                raise AirflowFailException(
                    f"RGB input validation failed: {video.get('error', 'missing result')}"
                )
            source["rgb_metadata"]["media"] = video
        return matched
    return apply_wsm_probe_results(matched, results)


@ensure_msc_configured
def generate_augmentation_configs(cosmos_config=None, run_id="", **context) -> list[str]:
    root = _runtime(context)
    config = Cosmos3NanoTaskConfig.model_validate(to_dict(cosmos_config))
    result_uri = _retrieval_result_uri(context)
    selected = _selected_sources(root)
    endpoint = require_service_endpoint_from_xcom(context["ti"], "cosmos3_service")
    endpoint = endpoint.rstrip("/")
    if endpoint.endswith("/v1"):
        endpoint = endpoint[:-3]
    jobs, paths = [], []
    for index, source in enumerate(selected):
        if not is_file(source["rgb"]):
            raise AirflowFailException(f"Selected input is missing: {source['rgb']}")
        for variant in range(config.num_augmentation):
            # Integer rank/index avoids treating an upstream span ID as a path.
            base = f"{root}/clip-{index:03d}/variant-{variant:03d}"
            seed = variant_seed(config, index, variant)
            generated = transfer_config(
                source, config, endpoint + "/v1", base, seed, ti=context["ti"]
            )
            path = base + "/config.yaml"
            write_file_to_directory(path, yaml.safe_dump(generated, sort_keys=False).encode())
            jobs.append(
                {
                    "source": source,
                    "variant": variant,
                    "seed": seed,
                    "config": path,
                    "validation": base + "/validation.json",
                    **generated["data"][0]["output"],
                }
            )
            paths.append(path)
    write_file_to_directory(
        root + "/selection.json",
        json.dumps(
            {
                "run_id": run_id,
                "retrieval_result_uri": result_uri,
                "prompt": config.prompt,
                "jobs": jobs,
            },
            indent=2,
        ).encode(),
    )
    return paths


@ensure_msc_configured
def validate_augmentation_outputs(cosmos_config=None, run_id="", **context) -> dict:
    root = _runtime(context)
    config = Cosmos3NanoTaskConfig.model_validate(to_dict(cosmos_config))
    selection_uri = root + "/selection.json"
    selection = read_file_as_dict(selection_uri)
    if selection is None:
        raise AirflowFailException(
            f"Augmentation selection is missing or empty at {selection_uri}. "
            "Check cosmos_augmentation.generate_cosmos_configs and its upstream tasks; "
            "if config generation succeeded, check storage access and the run output path."
        )
    jobs = selection.get("jobs") or []
    # Without a requested count the clip total is only known from the selection,
    # so check every selected clip still got each of its variants.
    expected_clips = (
        config.num_clips
        if config.num_clips is not None
        else len({job["source"]["span_uuid"] for job in jobs})
    )
    if not jobs or len(jobs) != expected_clips * config.num_augmentation:
        raise AirflowFailException("Augmentation selection does not contain every expected job")
    errors = []
    for job in jobs:
        try:
            for key in ("config", "metadata"):
                if not is_file(job[key]):
                    raise AirflowFailException(f"Missing augmentation {key}: {job[key]}")
            metadata = read_file_as_dict(job["metadata"]) or {}
            if (
                metadata.get("input_media_path") != job["source"]["rgb"]
                or metadata.get("prompt") != config.prompt
            ):
                raise AirflowFailException(
                    f"Augmentation source/prompt mismatch for {job['video']}"
                )
            if config.control_mode == "wsm":
                control = job["source"]["wsm"]
                if (metadata.get("control_media") or {}).get("wsm") != control["uri"]:
                    raise AirflowFailException(
                        f"Augmentation did not record the supplied WSM for {job['video']}"
                    )
            generated = load_cosmos_base_config(
                base_config_path=job["config"],
                default_path="",
                description="Generated CDF augmentation configuration",
            )
            job["evaluation"] = validate_physical_plausibility_result(metadata, generated)
            job["accepted"] = physical_plausibility_passed(job["evaluation"])
            if not job["accepted"]:
                job["rejection_reason"] = "Physical plausibility threshold did not pass"
                write_file_to_directory(
                    job["validation"],
                    json.dumps(
                        {"passed": False, "rejected": True, "reason": job["rejection_reason"]}
                    ).encode(),
                )
                continue
            if not is_file(job["video"]):
                raise AirflowFailException(f"Missing augmentation video: {job['video']}")
        except Exception as exc:
            errors.append(str(exc))
            _record_validation_failure(job, config, str(exc))
    if errors:
        manifest_uri = _publish_manifest(selection, config, root, run_id)
        raise AirflowFailException("; ".join(errors) + f"; details: {manifest_uri}")
    request_uri = f"{root}/output_probe_request.json"
    result_uri = f"{root}/output_probe_result.json"
    expected = {"size": config.size, "num_frames": config.num_frames, "fps": config.fps}
    items = [
        {"id": job["video"], "uri": job["video"], "expected": expected}
        for job in jobs
        if job["accepted"]
    ]
    write_file_to_directory(request_uri, json.dumps({"items": items}, indent=2).encode())
    write_file_to_directory(
        root + "/checked_outputs.json", json.dumps(selection, indent=2).encode()
    )
    return {"probe_request_uri": request_uri, "probe_result_uri": result_uri}


def _record_validation_failure(job, config, error):
    job["error"] = error
    try:
        if is_file(job.get("metadata", "")):
            job["evaluation"] = (read_file_as_dict(job["metadata"]) or {}).get("cosmos_evaluator")
    except Exception:
        logger.warning("Could not read metadata while recording validation failure", exc_info=True)
        pass
    failure = {
        "passed": False,
        "video": job.get("video"),
        "error": error,
        "requested_media": {
            "fps": config.fps,
            "num_frames": config.num_frames,
            "size": config.size,
        },
    }
    write_file_to_directory(job["validation"], json.dumps(failure, indent=2).encode())


def _publish_manifest(selection, config, root, run_id):
    jobs = selection["jobs"]
    outputs = [
        job for job in jobs if job.get("accepted") and (job.get("media") or {}).get("passed")
    ]
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "model": COSMOS_MODEL,
        "retrieval_result_uri": selection["retrieval_result_uri"],
        "prompt": config.prompt,
        "requested": len(jobs),
        "generated": sum(is_file(job["video"]) for job in jobs),
        "validated": sum(bool((job.get("media") or {}).get("passed")) for job in jobs),
        "outputs": outputs,
        "rejected": [job for job in jobs if job.get("accepted") is False],
        "failed": [job for job in jobs if job.get("error")],
        "videos_directory": root + "/videos/",
    }
    uri = root + "/result_manifest.json"
    write_file_to_directory(uri, json.dumps(manifest, indent=2).encode())
    return uri


def _publish_accepted_videos(jobs: list[dict], root: str) -> str:
    """Keep the deliverable directory separate from candidates and diagnostics."""
    directory = root + "/videos/"
    if urlsplit(directory).scheme:
        # Object stores represent an empty directory with a zero-byte folder marker.
        write_file_to_directory(directory, b"")
    else:
        Path(directory).mkdir(parents=True, exist_ok=True)
    for index, job in enumerate(jobs):
        destination = f"{directory}video-{index:04d}.mp4"
        if job.get("accepted") and (job.get("media") or {}).get("passed"):
            copy_file(job["video"], destination)
            job["published_video"] = destination
        elif is_file(destination):
            # A cleared task may be rerun after a candidate's verdict changes.
            delete_path(destination)
    return directory


@ensure_msc_configured
def finalize_augmentation_outputs(cosmos_config=None, run_id="", **context) -> dict:
    """Publish success or failure evidence for the media probe's full decode."""
    root = _runtime(context)
    config = Cosmos3NanoTaskConfig.model_validate(to_dict(cosmos_config))
    checked = read_file_as_dict(root + "/checked_outputs.json") or {}
    jobs = checked.get("jobs") or []
    if not jobs:
        raise AirflowFailException("validate_outputs did not record the checked outputs")
    if any(not isinstance(job.get("accepted"), bool) for job in jobs):
        raise AirflowFailException(
            "Checked outputs have no physical plausibility acceptance decision"
        )
    result_uri = f"{root}/output_probe_result.json"
    results = {
        item.get("id"): item for item in (read_file_as_dict(result_uri) or {}).get("items") or []
    }
    errors = []
    for job in jobs:
        if job.get("accepted") is False:
            continue
        media = results.get(job["video"])
        if media is None or not media.get("passed"):
            reason = (media or {}).get("error") or "the media probe returned no valid result"
            error = f"Invalid generated video {job['video']}: {reason}"
            _record_validation_failure(job, config, error)
            errors.append(error)
            continue
        validation = {key: value for key, value in media.items() if key != "id"}
        validation.update(config_uri=job["config"], video=job["video"])
        write_file_to_directory(job["validation"], json.dumps(validation, indent=2).encode())
        job["media"] = validation
    if not errors:
        videos_directory = _publish_accepted_videos(jobs, root)
    manifest_uri = _publish_manifest(checked, config, root, run_id)
    if errors:
        raise AirflowFailException("; ".join(errors) + f"; details: {manifest_uri}")
    return {
        "completed": True,
        "generated": sum(is_file(job["video"]) for job in jobs),
        "validated": sum(bool((job.get("media") or {}).get("passed")) for job in jobs),
        "rejected": sum(job.get("accepted") is False for job in jobs),
        "videos_directory": videos_directory,
        "clips": len({job["source"]["span_uuid"] for job in jobs}),
        "variants_per_clip": config.num_augmentation,
        "wsm_guided_videos": sum(bool(job["source"].get("wsm")) for job in jobs),
        "rgb_videos": sum(bool(job["source"].get("rgb_metadata")) for job in jobs),
        "manifest_uri": manifest_uri,
    }


def validate_combined_outputs(payload=None, configs=None, run_id="", **context) -> dict:
    result = validate_avdg_pipeline_outputs(payload, configs, run_id, **context)
    if to_dict(payload).get("cosmos", {}).get("enabled"):
        augmentation = context["ti"].xcom_pull(task_ids=FINALIZE_OUTPUTS_TASK_ID) or {}
        if augmentation.get("completed") is not True or not augmentation.get("manifest_uri"):
            raise AirflowFailException("Augmentation did not publish validated outputs")
        result["augmentation"] = augmentation
    return result
