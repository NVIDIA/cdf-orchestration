# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Plan chunks for aoi_anomalygen_dag and collect their results.

Layout under ``<output_root>/<campaign_id>/round_<n>/`` (nothing is overwritten; a rerun is a new
round):

    plan.json                                  the validated conf and every chunk's images
    chunk_<k>/anomaly_generation/              written by the chunk's AnomalyGen pod
        <type>_<nnnnn>.png                     defect images (AnomalyGen publish)
        export_manifest.json                   image -> internal name, image -> clean input
        masks/<type>_<nnnnn>.png               defect mask for each image (= the label)
        kpi.json                               AnomalyGen evaluate: per-type and per-sample scores
        stage_timing.json, logs/, amg/, generation/, pipeline_config.resolved.yaml
    results/manifest.json                      one row per defect image across all chunks
    results/labels.json                        image, mask, defect label
    results/kpi.json                           per-sample scores from every chunk, by image
    results/run_report.json                    chunk status, coverage, failures to rerun, timing
"""

import base64
import json
import logging
import math
import zlib
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from airflow.exceptions import AirflowFailException

from dags.shared.utils.msc_utils import (
    convert_to_msc_url,
    ensure_msc_configured,
    is_file,
    list_directory,
    read_file_as_dict,
    write_file_to_directory,
)
from dags.workflows.aoi_anomalygen_dag.models import AoiAnomalyGenPayload

logger = logging.getLogger(__name__)

IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff")
CONTAINER_DATASET = "/work/dataset/"


def _msc(url: str) -> str:
    resolved = convert_to_msc_url(url)
    if not resolved.startswith("msc://"):
        raise AirflowFailException(f"No MSC path_mapping covers {url}")
    return resolved


def _join(prefix: str, *parts: str) -> str:
    return prefix.rstrip("/") + "/" + "/".join(p.strip("/") for p in parts)


def _relative_keys(prefix: str) -> list[str]:
    """Every object under ``prefix`` as a path relative to it."""
    prefix = _msc(prefix).rstrip("/") + "/"
    bucket_path = prefix.split("://", 1)[1].split("/", 1)[1] if prefix.count("/") > 3 else ""
    keys = []
    for obj in list_directory(prefix):
        key = obj.key
        if key.startswith(prefix):
            keys.append(key[len(prefix) :])
        elif bucket_path and key.lstrip("/").startswith(bucket_path):
            keys.append(key.lstrip("/")[len(bucket_path) :])
        else:
            raise AirflowFailException(f"Listing {prefix} returned an unexpected key {key!r}")
    return sorted(k for k in keys if k and not k.endswith("/"))


def _run_root(payload: AoiAnomalyGenPayload) -> str:
    return _join(_msc(payload.output_root), payload.campaign_id, f"round_{payload.round}")


def _write_json(url: str, data: Any) -> None:
    write_file_to_directory(url, (json.dumps(data, indent=1, sort_keys=False) + "\n").encode())


def _balanced(items: list[str], chunk_size: int) -> list[list[str]]:
    """Split into ceil(n / chunk_size) chunks whose sizes differ by at most one."""
    n_chunks = math.ceil(len(items) / chunk_size)
    base, extra = divmod(len(items), n_chunks)
    chunks, start = [], 0
    for k in range(n_chunks):
        size = base + (1 if k < extra else 0)
        chunks.append(items[start : start + size])
        start += size
    return chunks


@ensure_msc_configured
def plan_chunks(payload: str, run_id: str) -> list[dict[str, Any]]:
    """Validate inputs on storage, assign a defect type to every image, split into chunks.

    Chunks hold one defect type each and ask for one output per image. amg launches its beams on
    the least-used clean image first, but it does NOT promise one output per clean image (one beam
    can return several accepted masks; run_pipeline refuses amp_per_clean_pairing under amg for
    this reason), so collect_results reports coverage and lists uncovered images for the next
    round. Two types in one chunk would make it worse: both would start over the same images.
    """
    p = AoiAnomalyGenPayload.model_validate(json.loads(payload))
    run_root = _run_root(p)
    if is_file(_join(run_root, "plan.json")):
        raise AirflowFailException(
            f"{run_root} already has a plan.json; use a new round or campaign_id (nothing is overwritten)"
        )

    clean_root = _msc(p.clean_images)
    clean_keys = set(_relative_keys(clean_root))
    available = sorted(
        k
        for k in clean_keys
        if len(k.split("/")) == 3
        and k.split("/")[1] == "clean_image"
        and k.lower().endswith(IMAGE_SUFFIXES)
    )
    if p.images is not None:
        missing = [rel for rel in p.images if rel not in clean_keys]
        if missing:
            raise AirflowFailException(f"{len(missing)} image(s) not under {clean_root}: {missing[:10]}")
        selected = list(p.images)
    else:
        selected = available
    if not selected:
        raise AirflowFailException(f"No clean images under {clean_root}<class>/clean_image/")

    def cad_of(rel: str) -> str:
        cls_name, _, name = rel.split("/")
        return f"{cls_name}/cad_mask/{PurePosixPath(name).stem}.png"

    no_cad = [rel for rel in selected if cad_of(rel) not in clean_keys]
    if no_cad:
        raise AirflowFailException(
            f"{len(no_cad)} image(s) have no <class>/cad_mask/<stem>.png: {no_cad[:10]}"
        )

    # Defect type per image: explicit first, then round-robin over the class's types.
    assigned: dict[str, str] = {}
    skipped: list[str] = []
    by_class: dict[str, list[str]] = defaultdict(list)
    for rel in selected:
        by_class[rel.split("/", 1)[0]].append(rel)
    for cls_name, rels in sorted(by_class.items()):
        types = p.defects_by_class.get(cls_name)
        rr = 0
        for rel in sorted(rels):
            if rel in p.image_defects:
                assigned[rel] = p.image_defects[rel]
            elif types:
                assigned[rel] = types[rr % len(types)]
                rr += 1
            elif p.images is not None:
                raise AirflowFailException(
                    f"{rel}: class {cls_name!r} has no entry in defects_by_class or image_defects"
                )
            else:
                skipped.append(rel)
    if not assigned:
        raise AirflowFailException("No image has a defect type; check defects_by_class")

    # The reference bank must hold every requested type (amg reads <class>/mask/<defect>/).
    dataset = _msc(p.dataset)
    dataset_keys = _relative_keys(dataset)
    if "defect_spec.jsonl" not in dataset_keys:
        raise AirflowFailException(f"{dataset} has no defect_spec.jsonl")
    for defect_type in sorted(set(assigned.values())):
        cls_name, defect = defect_type.split("+", 1)
        for kind in ("mask", "anomaly_image"):
            if not any(k.startswith(f"{cls_name}/{kind}/{defect}/") for k in dataset_keys):
                raise AirflowFailException(f"{dataset} has no {cls_name}/{kind}/{defect}/ for {defect_type}")
        for kind in ("clean_image", "cad_mask"):
            # input_override replaces these paths, which must exist in the fetched dataset.
            if not any(k.startswith(f"{cls_name}/{kind}/") for k in dataset_keys):
                raise AirflowFailException(f"{dataset} has no {cls_name}/{kind}/ to override")

    groups: dict[str, list[str]] = defaultdict(list)
    for rel, defect_type in assigned.items():
        groups[defect_type].append(rel)

    options = dict(p.anomaly_generation)
    # evaluate is run by the pod wrapper AFTER publish, so a scoring failure cannot throw away the
    # finished amg + generate work (run_pipeline stops at the first failing stage).
    stages = ["prepare", "amg", "generate", "export", "publish"]
    chunks: list[dict[str, Any]] = []
    for defect_type in sorted(groups):
        cls_name = defect_type.split("+", 1)[0]
        for images in _balanced(sorted(groups[defect_type]), p.chunk_size):
            k = len(chunks)
            chunk_id = f"chunk_{k:03d}"
            output = _join(run_root, chunk_id, "anomaly_generation") + "/"
            seed = zlib.crc32(f"{run_id}:{chunk_id}".encode()) % 2**31
            config = {
                **options,
                "input": "/work/dataset",
                "output": "/work/output",
                "recipe": "/work/recipe.yaml",
                "checkpoint": "/work/model.pt",
                "mode": "inference",
                "num_sdg": len(images) * p.outputs_per_image,
                "per_defect_counts": {defect_type: len(images) * p.outputs_per_image},
                "output_naming": "indexed",
                "base_seed": seed,
                "num_gpus": 1,
                "stages": stages,
                # Cloud VLM: the pod wrapper writes this registry from AOI_AMG_ENDPOINTS_B64 plus
                # the VLM_API_KEY secret; the key never enters the config or the outputs.
                **({} if p.local_vlm else {"amg_model_registry": "/work/endpoints.yaml"}),
                "staging": {
                    "fetch": {
                        "input": [dataset],
                        "input_override": {
                            f"{cls_name}/clean_image": [_join(clean_root, rel) for rel in images],
                            f"{cls_name}/cad_mask": [_join(clean_root, cad_of(rel)) for rel in images],
                        },
                        "recipe": _msc(p.recipe) if p.recipe.startswith("s3://") else p.recipe,
                        "checkpoint": _msc(p.checkpoint) if p.checkpoint.startswith("s3://") else p.checkpoint,
                    },
                    "publish": {"output": output},
                },
            }
            chunks.append(
                {
                    "chunk_id": chunk_id,
                    "map_index": k,
                    "defect_type": defect_type,
                    "images": images,
                    "output": output,
                    "base_seed": seed,
                    "evaluate": p.evaluate,
                    "vlm": "local" if p.local_vlm else "cloud",
                    # local Gemma takes 2 GPUs (TP=2) during amg; with a cloud VLM only generate
                    # and evaluate use a GPU.
                    "gpu_count": 2 if p.local_vlm else 1,
                    "pipeline_config": config,
                }
            )

    plan = {
        "run_id": run_id,
        "planned_at": datetime.now(timezone.utc).isoformat(),
        "run_root": run_root,
        "payload": p.model_dump(),
        "images_total": len(assigned),
        "images_skipped_no_defect_type": skipped,
        "chunks": chunks,
    }
    _write_json(_join(run_root, "plan.json"), plan)
    logger.info(
        "Planned %d image(s) in %d chunk(s) under %s (%s)",
        len(assigned),
        len(chunks),
        run_root,
        ", ".join(f"{t}: {len(v)}" for t, v in sorted(groups.items())),
    )
    endpoints_b64 = base64.b64encode(
        json.dumps([e.model_dump() for e in p.amg_endpoints]).encode()
    ).decode()
    mapped = [
        {k: v for k, v in c.items() if k != "pipeline_config"}
        | {
            "config_b64": base64.b64encode(json.dumps(c["pipeline_config"]).encode()).decode(),
            "endpoints_b64": None if p.local_vlm else endpoints_b64,
        }
        for c in chunks
    ]
    return mapped


def select_pod_kwargs(chunk: dict[str, Any]) -> dict[str, Any]:
    """Mapped AnomalyGen task arguments for one chunk."""
    env = [
        f"AOI_RUN_CONFIG_B64:{chunk['config_b64']}",
        f"AOI_EVALUATE:{'1' if chunk['evaluate'] else '0'}",
    ]
    if chunk.get("endpoints_b64"):
        env.append(f"AOI_AMG_ENDPOINTS_B64:{chunk['endpoints_b64']}")
    return {"runtime_environment_variables": env, "gpu_count": chunk["gpu_count"]}


def select_chunks(vlm: str, **context) -> list[dict[str, Any]]:
    """Pod arguments for the chunks of one VLM kind ("cloud" or "local").

    Two mapped AnomalyGen tasks exist because pool_slots cannot be mapped: cloud-VLM chunks take
    1 GPU slot, local-Gemma chunks 2. One of the two lists is always empty (task skipped).
    """
    chunks = context["ti"].xcom_pull(task_ids="plan_chunks", key="return_value") or []
    return [select_pod_kwargs(c) for c in chunks if c["vlm"] == vlm]


def _clean_rel(container_path: str) -> str:
    return container_path[len(CONTAINER_DATASET) :] if container_path.startswith(CONTAINER_DATASET) else container_path


def _finite(value: Any) -> Any:
    """JSON-safe copy: NaN/inf (AnomalyGen writes NaN for unscorable samples) become None."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _finite(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_finite(v) for v in value]
    return value


@ensure_msc_configured
def collect_results(**context) -> dict[str, Any]:
    """Merge every chunk's outputs into results/ and fail the run if any chunk failed.

    Runs whatever happened upstream (trigger rule ALL_DONE). Per chunk:
      failed      pod crashed, timed out, never ran, or exited non-zero, or no export_manifest
      incomplete  pod exited 0 but some published image lacks its mask or score (kpi.json)
      ok          every published image has image + mask (+ scores when evaluate is on)
    A clean image counts as covered when at least one complete defect image came from it. Images
    of failed chunks and uncovered images go to images_to_rerun with their defect type
    (rerun_payload_patch gives the conf fields for the next round). Failed or incomplete chunks
    fail the run; uncovered images fail it only when require_full_coverage is set.
    """
    ti = context["ti"]
    chunks = ti.xcom_pull(task_ids="plan_chunks", key="return_value")
    payload = ti.xcom_pull(task_ids="validate_payload", key="return_value")
    if not chunks or not payload:
        raise AirflowFailException("plan_chunks did not produce a plan; nothing to collect")
    p = AoiAnomalyGenPayload.model_validate(payload)
    run_root = _run_root(p)
    clean_root = _msc(p.clean_images)
    plan = read_file_as_dict(_join(run_root, "plan.json")) or {}

    rows, kpi_rows, chunk_reports = [], {}, []
    rerun: dict[str, str] = {}
    for chunk in chunks:
        out = chunk["output"]
        defect_type = chunk["defect_type"]
        keys = set(_relative_keys(out))
        timing = read_file_as_dict(_join(out, "stage_timing.json")) if "stage_timing.json" in keys else None
        manifest = (
            read_file_as_dict(_join(out, "export_manifest.json")) if "export_manifest.json" in keys else None
        )
        kpi = read_file_as_dict(_join(out, "kpi.json")) if "kpi.json" in keys else None
        failed_reasons = []
        if timing is None:
            failed_reasons.append("no stage_timing.json (pod did not finish: crash, timeout or never started)")
        elif timing.get("exit_code") != 0:
            failed_reasons.append(f"AnomalyGen exited with code {timing.get('exit_code')}")
        if manifest is None:
            failed_reasons.append("no export_manifest.json")
        problems = list(failed_reasons)
        if not failed_reasons and chunk.get("evaluate", p.evaluate) and kpi is None:
            problems.append(
                f"no kpi.json (evaluate exit code {(timing or {}).get('evaluate_exit_code')}; see logs/evaluate.log)"
            )
        if timing and timing.get("upload_errors"):
            problems.append(f"upload errors: {timing['upload_errors'][:5]}")

        per_sample = {}
        for block in (kpi or {}).values():
            if isinstance(block, dict):
                for sample in block.get("per_sample", []):
                    per_sample[PurePosixPath(sample.get("path", "")).name] = _finite(
                        {k: v for k, v in sample.items() if k != "path"}
                    )

        used_inputs, complete_inputs = set(), set()
        images = (manifest or {}).get("images", {})
        inputs = (manifest or {}).get("input_images", {})
        for published in sorted(images):
            internal = images[published]
            clean_rel = _clean_rel(inputs.get(published, ""))
            used_inputs.add(clean_rel)
            scores = per_sample.get(internal)
            missing = [
                name
                for name, present in (
                    ("image", published in keys),
                    ("mask", f"masks/{published}" in keys),
                    ("scores", not chunk.get("evaluate", p.evaluate) or scores is not None),
                )
                if not present
            ]
            if not missing:
                complete_inputs.add(clean_rel)
            row = {
                "chunk_id": chunk["chunk_id"],
                "defect_image": _join(out, published),
                "mask": _join(out, "masks", published),
                "label": defect_type,
                "class": defect_type.split("+", 1)[0],
                "defect": defect_type.split("+", 1)[1],
                "clean_image": _join(clean_root, clean_rel) if clean_rel else None,
                "clean_image_rel": clean_rel or None,
                "scores": scores,
                "missing": missing,
            }
            rows.append(row)
            if scores is not None:
                kpi_rows[row["defect_image"]] = {"label": defect_type, "clean_image": row["clean_image"], **scores}
            if missing:
                problems.append(f"{published}: missing {', '.join(missing)}")

        failed = bool(failed_reasons)
        uncovered = list(chunk["images"]) if failed else [r for r in chunk["images"] if r not in complete_inputs]
        for rel in uncovered:
            rerun[rel] = defect_type
        stages = (timing or {}).get("stages_seconds", {})
        per_image_stages = sum(stages.get(s, 0.0) for s in ("amg", "generate", "evaluate"))
        n = len(chunk["images"])
        chunk_reports.append(
            {
                "chunk_id": chunk["chunk_id"],
                "map_index": chunk["map_index"],
                "defect_type": defect_type,
                "images_requested": n,
                "images_generated": len(images),
                "distinct_inputs_used": len(used_inputs),
                "inputs_covered": len(complete_inputs),
                "images_uncovered": uncovered,
                "status": "failed" if failed else ("incomplete" if problems else "ok"),
                "problems": problems,
                "pod_seconds": (timing or {}).get("total_seconds"),
                "stages_seconds": stages,
                "seconds_per_image_pod": round(timing["total_seconds"] / n, 1) if timing else None,
                "seconds_per_image_amg_generate_evaluate": round(per_image_stages / n, 1) if timing else None,
                "started_at": (timing or {}).get("started_at"),
                "finished_at": (timing or {}).get("finished_at"),
            }
        )

    dag_run = context.get("dag_run")
    started = getattr(dag_run, "start_date", None)
    now = datetime.now(timezone.utc)
    timed = [c for c in chunk_reports if c["pod_seconds"]]
    timed_images = sum(c["images_requested"] for c in timed)
    requested = sum(c["images_requested"] for c in chunk_reports)
    covered = sum(c["inputs_covered"] for c in chunk_reports)
    by_type: dict[str, dict[str, Any]] = {}
    for defect_type in sorted({c["defect_type"] for c in chunk_reports}):
        type_rows = [r for r in rows if r["label"] == defect_type]
        means = {}
        for metric in ("nn_score", "precision_score", "boundary_iou_score", "completeness_score"):
            vals = [r["scores"][metric] for r in type_rows if r["scores"] and isinstance(r["scores"].get(metric), (int, float))]
            if vals:
                means[f"mean_{metric}"] = round(sum(vals) / len(vals), 4)
                means[f"n_{metric}"] = len(vals)
        by_type[defect_type] = {
            "images_requested": sum(c["images_requested"] for c in chunk_reports if c["defect_type"] == defect_type),
            "images_generated": len(type_rows),
            "inputs_covered": sum(c["inputs_covered"] for c in chunk_reports if c["defect_type"] == defect_type),
            **means,
        }
    rerun_sorted = dict(sorted(rerun.items()))
    report = {
        "run_id": context.get("run_id"),
        "run_root": run_root,
        "campaign_id": p.campaign_id,
        "round": p.round,
        "chunk_size": p.chunk_size,
        "outputs_per_image": p.outputs_per_image,
        "chunks_total": len(chunk_reports),
        "chunks_ok": sum(c["status"] == "ok" for c in chunk_reports),
        "chunks_failed": [c["chunk_id"] for c in chunk_reports if c["status"] == "failed"],
        "chunks_incomplete": [c["chunk_id"] for c in chunk_reports if c["status"] == "incomplete"],
        "images_requested": requested,
        "images_generated": len(rows),
        "images_complete": sum(not r["missing"] for r in rows),
        "clean_images_covered": covered,
        "clean_images_uncovered": len(rerun_sorted),
        "images_skipped_no_defect_type": plan.get("images_skipped_no_defect_type", []),
        "images_to_rerun": list(rerun_sorted),
        "rerun_payload_patch": {
            "round": p.round + 1,
            "images": list(rerun_sorted),
            "image_defects": rerun_sorted,
        } if rerun_sorted else None,
        "by_defect_type": by_type,
        "timing": {
            "dag_run_started_at": started.isoformat() if started else None,
            "collected_at": now.isoformat(),
            "dag_run_seconds": round((now - started).total_seconds(), 1) if started else None,
            "pod_seconds_total": round(sum(c["pod_seconds"] for c in timed), 1),
            "pod_seconds_per_image": round(sum(c["pod_seconds"] for c in timed) / timed_images, 1) if timed_images else None,
            "dag_run_seconds_per_image": round((now - started).total_seconds() / requested, 1) if started and requested else None,
        },
        "chunks": chunk_reports,
    }
    results = _join(run_root, "results")
    _write_json(_join(results, "manifest.json"), {"run_root": run_root, "images": rows})
    _write_json(
        _join(results, "labels.json"),
        [
            {"image": r["defect_image"], "mask": r["mask"], "label": r["label"], "clean_image": r["clean_image"]}
            for r in rows
            if not r["missing"]
        ],
    )
    _write_json(_join(results, "kpi.json"), {"per_image": kpi_rows, "by_defect_type": by_type})
    _write_json(_join(results, "run_report.json"), report)
    logger.info(
        "Collected %d image(s) from %d chunk(s); covered %d/%d clean images; failed chunks: %s",
        len(rows),
        len(chunk_reports),
        covered,
        requested,
        report["chunks_failed"] or "none",
    )
    if report["chunks_failed"] or report["chunks_incomplete"]:
        raise AirflowFailException(
            f"Chunks failed {report['chunks_failed']} / incomplete {report['chunks_incomplete']}; "
            f"see {_join(results, 'run_report.json')}"
        )
    if p.require_full_coverage and rerun_sorted:
        raise AirflowFailException(
            f"{len(rerun_sorted)} clean image(s) got no defect image and require_full_coverage is set; "
            f"see {_join(results, 'run_report.json')}"
        )
    return {k: report[k] for k in ("run_root", "chunks_total", "images_generated", "clean_images_covered")}
