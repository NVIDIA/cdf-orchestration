# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Connect retrieved clips to their precomputed WSM control videos for augmentation."""

from airflow.exceptions import AirflowFailException

from dags.shared.utils.msc_utils import is_file, read_file_as_dict
from dags.workflows.autonomous_vehicle_dataset_generation_dag.models.payload import (
    Cosmos3NanoTaskConfig,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.models.wsm import WSMManifest


def match_controls(selected: list[dict], manifest: WSMManifest, config: Cosmos3NanoTaskConfig):
    """Find exactly one WSM control for each selected retrieval clip and ensure
    it matches the generation configuration."""
    matched = []
    for source in selected:
        evaluated = []
        for entry in manifest.controls:
            reasons = []
            if not (
                (entry.span_uuid is not None and entry.span_uuid == source.get("span_uuid"))
                or (
                    entry.source_video is not None
                    and entry.source_video == source.get("source_video")
                )
            ):
                reasons.append("identity")
            if entry.camera != source["camera"]:
                reasons.append("camera")
            if not all(isinstance(source.get(key), int) for key in ("start_ns", "end_ns")):
                reasons.append("time range must contain integer nanoseconds")
            else:
                if source["start_ns"] < entry.start_ns:
                    reasons.append("start_ns coverage")
                if (
                    source["end_ns"] - source["start_ns"]
                ) * entry.fps / 1e9 < config.num_frames - 1.01:  # one frame tolerance
                    reasons.append("retrieved RGB generation coverage")
                offset = round((source["start_ns"] - entry.start_ns) * entry.fps / 1e9)
                if offset < 0 or offset + config.num_frames > entry.num_frames:
                    reasons.append("generation frame coverage")
                if entry.end_ns < source["end_ns"] - 1.01e9 / entry.fps:
                    reasons.append("end_ns coverage")
            evaluated.append((entry, reasons))
        candidates = [entry for entry, reasons in evaluated if not reasons]
        if len(candidates) != 1:
            identity = {
                key: source.get(key)
                for key in ("span_uuid", "source_video", "camera", "start_ns", "end_ns")
            }
            details = [
                {
                    **entry.model_dump(
                        include={"span_uuid", "source_video", "camera", "start_ns", "end_ns"},
                        exclude_none=True,
                    ),
                    "mismatches": reasons,
                }
                for entry, reasons in sorted(evaluated, key=lambda item: len(item[1]))[:5]
            ]
            raise AirflowFailException(
                f"Rank {source['rank']} needs exactly one WSM for its clip, camera and time range; "
                f"found {len(candidates)}. Selected clip: {identity!r}. "
                f"Closest manifest entries (up to 5 of {len(evaluated)}): {details!r}"
            )
        entry = candidates[0]
        if (
            entry.fps != config.fps
            or entry.size != config.size
            or entry.num_frames < config.num_frames
        ):
            raise AirflowFailException(
                f"Rank {source['rank']} WSM must match generation FPS/size and cover num_frames; "
                "prepare aligned controls before submission"
            )
        matched.append(dict(source, wsm=entry.model_dump(exclude_none=True)))
    return matched


def prepare_wsm_controls(selected: list[dict], config: Cosmos3NanoTaskConfig) -> list[dict]:
    """Loads and match the WSM controls needed for the selected clips."""
    if not config.enabled:
        return selected
    manifest_uris = set()
    for source in selected:
        source_uri = source.get("source_video") or ""
        if source_uri.rsplit("/", 1)[-1] != "camera_front_wide_120fov.mp4":
            raise AirflowFailException(
                "Per-scene WSM discovery needs a AV Dataset Generation source video"
            )
        manifest_uris.add(source_uri.rsplit("/", 1)[0] + "/wsm/wsm-manifest.json")
    controls = []
    for uri in sorted(manifest_uris):
        scene_manifest = WSMManifest.model_validate(read_file_as_dict(uri))
        controls.extend(scene_manifest.controls)
    manifest = WSMManifest(schema_version=1, kind="cdf-av-wsm", controls=controls)
    try:
        matched = match_controls(selected, manifest, config)
    except AirflowFailException as exc:
        raise AirflowFailException(f"{exc}. WSM manifests: {sorted(manifest_uris)!r}") from exc
    for source in matched:
        if not is_file(source["wsm"]["uri"]):
            raise AirflowFailException(f"Missing WSM video for rank {source['rank']}")
    return matched


def wsm_probe_items(
    matched: list[dict],
    config: Cosmos3NanoTaskConfig,
    output_directory: str,
) -> list[dict]:
    """Build the requests sent to the WSM probe pod."""
    items = [
        {
            "id": str(source["rank"]),
            "uri": source["wsm"]["uri"],
            "expected": {key: source["wsm"][key] for key in ("size", "num_frames", "fps")},
        }
        for source in matched
    ]

    for index, (source, item) in enumerate(zip(matched, items, strict=True)):
        control = source["wsm"]
        start_frame = round((source["start_ns"] - control["start_ns"]) * config.fps / 1e9)
        item.update(
            sha256=control.get("sha256"),
            start_frame=start_frame,
            source_start_ns=source["start_ns"],
            control_start_ns=control["start_ns"],
            output_uri=f"{output_directory.rstrip('/')}/controls/clip-{index:03d}/wsm.mp4",
            window_expected={
                "num_frames": config.num_frames,
                "fps": config.fps,
                "size": config.size,
            },
        )
    return items


def apply_wsm_probe_results(matched: list[dict], results: dict | None) -> list[dict]:
    """Fail on any control the probe could not decode, then attach its measurements."""
    by_id = {item.get("id"): item for item in (results or {}).get("items") or []}
    for source in matched:
        control = source["wsm"]
        media = by_id.get(str(source["rank"]))
        try:
            if media is None:
                raise ValueError("the media probe returned no result")
            if not media.get("passed"):
                raise ValueError(media.get("error") or "the media probe rejected it")
            original_sha256 = (media.get("control") or {}).get(
                "full_scene_sha256", media.get("sha256")
            )
            if control.get("sha256") and control["sha256"] != original_sha256:
                raise ValueError("WSM checksum differs from the manifest")
        except ValueError as exc:
            raise AirflowFailException(f"Invalid WSM for rank {source['rank']}: {exc}") from exc
        control.update(media.get("control") or {})
        control["media"] = {
            key: value for key, value in media.items() if key not in ("id", "control")
        }
    return matched
