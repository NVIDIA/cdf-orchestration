# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""anomaly_package and validate_anomalygen_outputs for one defect configuration.

Adapted from the DIG DAG's defect path (defect_image_generation_dag callables/smt/anomalygen.py):
package_anomalygen_inputs with no augmentation manifest, _pod_configuration unchanged, and
validate_anomalygen_outputs keeping every export. Additions: input_override sources are converted
to msc:// like the other fetch sources, the pod is told whether to run evaluate, and validation
reports the requested and delivered counts, the distinct inputs used and the masks, which the
agent needs to tell a shortfall (F2) from a success.
"""

import base64
import copy
import json
from pathlib import PurePosixPath
from typing import Any

from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.utils.msc_utils import (
    convert_to_msc_url,
    copy_file,
    ensure_msc_configured,
    is_file,
    read_file_as_dict,
    write_file_to_directory,
)
from dags.workflows.aoi_anomalygen_dag.callables.config_generation import requested_count


def select_anomaly_generation_kwargs(package: dict) -> dict:
    return {
        "runtime_environment_variables": package["anomaly_generation_environment"],
        "gpu_count": package["gpu_count"],
    }


def select_defect_validation_kwargs(package: dict) -> dict:
    return package["defect_validation_kwargs"]


def _pod_configuration(runner_config: dict[str, Any]) -> tuple[list[str], int]:
    """Encode public settings; the manifest adds credentials inside the pod."""
    endpoints = runner_config.pop("amg_endpoints", [])
    if endpoints:
        runner_config["amg_model_registry"] = "/work/endpoints.yaml"
        runner_config["staging"]["fetch"].pop("amg_model_registry", None)
    gpu_count = runner_config.get("num_gpus", 1)
    if "amg" in runner_config["stages"]:
        gpu_count = max(2, gpu_count)
    environment = [
        f"{name}:" + base64.b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()
        for name, value in (
            ("ANOMALYGEN_CONFIG_B64", runner_config),
            ("ANOMALYGEN_ENDPOINTS_B64", endpoints),
        )
    ]
    return environment, gpu_count


def package_anomalygen_inputs(
    output_directory: str,
    anomaly_generation_config: dict[str, Any],
    defect_id: str,
    evaluate: bool = False,
) -> dict:
    """Pass the configuration's prepared inputs straight to container staging (defect mode)."""
    config = copy.deepcopy(anomaly_generation_config)
    config.update(input="/work/dataset", output="/work/output")
    fetch = config["staging"]["fetch"]
    fetch["input"] = [convert_to_msc_url(path) for path in fetch["input"]]
    for field in ("recipe", "checkpoint", "base_checkpoint", "amg_model_registry"):
        if fetch.get(field):
            fetch[field] = convert_to_msc_url(fetch[field])
    override = fetch.get("input_override")
    if override:
        # {path under the dataset: source or [sources]}; the path is replaced, not merged.
        fetch["input_override"] = {
            path: (
                convert_to_msc_url(sources)
                if isinstance(sources, str)
                else [convert_to_msc_url(source) for source in sources]
            )
            for path, sources in override.items()
        }
    config["staging"]["publish"]["output"] = convert_to_msc_url(
        str(URL(output_directory) / "anomaly_generation")
    )
    requested = requested_count(config)
    environment, gpu_count = _pod_configuration(config)
    environment.append(f"AOI_EVALUATE:{'1' if evaluate else '0'}")
    return {
        "anomaly_generation_environment": environment,
        "gpu_count": gpu_count,
        "defect_validation_kwargs": {
            "output_directory": output_directory,
            "defect_id": defect_id,
            "requested": requested,
            "evaluate": evaluate,
        },
    }


@ensure_msc_configured
def validate_anomalygen_outputs(
    output_directory: str,
    defect_id: str,
    requested: int | None = None,
    evaluate: bool = False,
) -> dict:
    """Keep every export, copy it to artifact/, and report delivered against requested."""
    exports = URL(output_directory) / "anomaly_generation"
    manifest_path = str(exports / "export_manifest.json")
    if not is_file(manifest_path):
        raise AirflowFailException(f"Missing AnomalyGen export manifest: {manifest_path}")
    manifest = read_file_as_dict(manifest_path)
    images = manifest.get("images") if isinstance(manifest, dict) else None
    sources = manifest.get("input_images") if isinstance(manifest, dict) else None
    if not isinstance(images, dict) or not images:
        raise AirflowFailException("AnomalyGen must export at least one image")
    if not isinstance(sources, dict) or set(sources) != set(images):
        raise AirflowFailException("AnomalyGen input_images must identify every exported image")
    selected = {}
    for name in sorted(sources):
        source = sources[name]
        path = PurePosixPath(name)
        if not path.parts or path.is_absolute() or ".." in path.parts or "\\" in name:
            raise AirflowFailException("Exported images must stay inside the AnomalyGen output directory")
        if not isinstance(source, str) or not source.strip():
            raise AirflowFailException("AnomalyGen input_images must identify every exported image")
        if not is_file(str(exports / name)):
            raise AirflowFailException(f"Missing AnomalyGen exported image: {name}")
        selected.setdefault(name, {"input_image": source, "exported_image": str(exports / name)})

    # Every check runs before artifact/ is written, as in DIG, so a configuration that fails here
    # leaves no artifact/manifest.json that looks validated.
    timing_path = str(exports / "stage_timing.json")
    timing = read_file_as_dict(timing_path) if is_file(timing_path) else None
    if timing is not None and timing.get("exit_code") != 0:
        raise AirflowFailException(
            f"AnomalyGen exited with code {timing.get('exit_code')} (see {exports / 'logs'}); "
            "its exports are not validated"
        )
    # Masks are uploaded by the pod wrapper under the same names; kpi.json only when evaluate ran.
    masks_missing = [name for name in sorted(selected) if not is_file(str(exports / "masks" / name))]
    if masks_missing:
        raise AirflowFailException(
            f"{len(masks_missing)} exported image(s) have no mask under {exports / 'masks'}: {masks_missing[:5]}"
        )
    kpi_path = str(exports / "kpi.json")
    has_kpi = evaluate and is_file(kpi_path)
    if evaluate and not has_kpi:
        raise AirflowFailException(
            f"evaluate is on but {kpi_path} is missing (evaluate exit code "
            f"{(timing or {}).get('evaluate_exit_code')}; see logs/evaluate.log)"
        )

    artifact = URL(output_directory) / "artifact"
    files, provenance = [], {}
    for filename, origin in selected.items():
        copy_file(origin["exported_image"], str(artifact / filename))
        files.append(filename)
        provenance[filename] = {**origin, "mask": str(exports / "masks" / filename)}
    delivered = len(files)
    inputs_used = sorted({origin["input_image"] for origin in selected.values()})
    counts = {
        "requested": requested,
        "delivered": delivered,
        "shortfall": max(0, requested - delivered) if requested is not None else None,
        "distinct_inputs_used": len(inputs_used),
    }
    final_manifest = {
        "artifact_type": "image",
        "item_count": delivered,
        "files": files,
        "source_metadata": {
            "anomalygen": {
                "images": provenance,
                "export_manifest": manifest_path,
                "masks": str(exports / "masks"),
                "kpi": kpi_path if has_kpi else None,
                "stage_timing": timing_path if timing else None,
                **counts,
            }
        },
    }
    write_file_to_directory(
        str(artifact / "manifest.json"), (json.dumps(final_manifest, indent=2) + "\n").encode()
    )
    return {
        "defect_id": defect_id,
        "images": [str(artifact / name) for name in files],
        "export_manifest": manifest_path,
        "item_count": delivered,
        **counts,
        "inputs_used": inputs_used,
        "stages_seconds": (timing or {}).get("stages_seconds"),
        "pod_seconds": (timing or {}).get("total_seconds"),
    }
