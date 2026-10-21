# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prepare SMT AnomalyGen batches from augmented or user-staged inputs."""

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
from dags.workflows.defect_image_generation_dag.callables.smt.images import load_augmented_images
from dags.workflows.defect_image_generation_dag.callables.smt.models import AnomalyInputConfig


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


@ensure_msc_configured
def package_anomalygen_inputs(
    augmentation_manifest: str | None,
    class_by_component_type: dict[str, str] | None,
    output_directory: str,
    anomaly_generation_config: dict[str, Any],
    golden_id: str | None = None,
    defect_id: str | None = None,
) -> dict:
    """Package augmented images, or pass prepared inputs straight to container staging."""
    config = copy.deepcopy(anomaly_generation_config)
    config.update(input="/work/dataset", output="/work/output")
    fetch = config["staging"]["fetch"]
    input_images = None
    if augmentation_manifest is not None:
        class_by_component_type = AnomalyInputConfig(
            class_by_component_type=class_by_component_type
        ).class_by_component_type
        images = load_augmented_images(augmentation_manifest)
        for item in images:
            component_type = item.get("component_type")
            if not isinstance(component_type, str) or not component_type.strip():
                raise AirflowFailException(f"Missing component_type for SMT image: {item['file']}")
            if component_type not in class_by_component_type:
                raise AirflowFailException(
                    f"No anomaly_input.class_by_component_type mapping for "
                    f"{component_type!r} (SMT image: {item['file']})"
                )
        package_root = URL(output_directory) / "anomaly_package" / "artifact"
        input_images = {}
        for item in images:
            input_image = item["augmented_image"]
            if not is_file(input_image):
                raise AirflowFailException(f"Missing SMT clean image: {input_image}")
            class_name = class_by_component_type[item["component_type"]]
            source_name = f"{class_name}/clean_image/{item['file']}"
            copy_file(input_image, str(package_root / source_name))
            input_images[source_name] = input_image
        fetch["input"].append(str(package_root))
    fetch["input"] = [convert_to_msc_url(path) for path in fetch["input"]]
    for field in ("recipe", "checkpoint", "base_checkpoint", "amg_model_registry"):
        if fetch.get(field):
            fetch[field] = convert_to_msc_url(fetch[field])
    config["staging"]["publish"]["output"] = convert_to_msc_url(
        str(URL(output_directory) / "anomaly_generation")
    )
    environment, gpu_count = _pod_configuration(config)
    return {
        "anomaly_generation_environment": environment,
        "gpu_count": gpu_count,
        "defect_validation_kwargs": {
            "input_images": input_images,
            "output_directory": output_directory,
            "golden_id": golden_id,
            "defect_id": defect_id,
        },
    }


@ensure_msc_configured
def validate_anomalygen_outputs(
    input_images: dict[str, str] | None,
    output_directory: str,
    golden_id: str | None = None,
    defect_id: str | None = None,
) -> dict:
    """Keep all direct-input exports, or one per augmented input for the full route."""
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
            raise AirflowFailException(
                "Exported images must stay inside the AnomalyGen output directory"
            )
        if not isinstance(source, str) or not source.strip():
            raise AirflowFailException("AnomalyGen input_images must identify every exported image")
        filename, input_image = name, source
        if input_images is not None:
            packaged_image = next(
                (item for item in input_images if source == item or source.endswith("/" + item)),
                None,
            )
            if packaged_image is None:
                raise AirflowFailException(
                    f"AnomalyGen export refers to an unexpected input: {source}"
                )
            filename = PurePosixPath(packaged_image).name
            input_image = input_images[packaged_image]
        if not is_file(str(exports / name)):
            raise AirflowFailException(f"Missing AnomalyGen exported image: {name}")
        selected.setdefault(
            filename, {"input_image": input_image, "exported_image": str(exports / name)}
        )
    artifact = URL(output_directory) / "artifact"
    files, provenance = [], {}
    for filename, origin in selected.items():
        copy_file(origin["exported_image"], str(artifact / filename))
        files.append(filename)
        provenance[filename] = origin
    final_manifest = {
        "artifact_type": "image",
        "item_count": len(files),
        "files": files,
        "source_metadata": {
            "anomalygen": {
                "images": provenance,
                "export_manifest": manifest_path,
            }
        },
    }
    write_file_to_directory(
        str(artifact / "manifest.json"), (json.dumps(final_manifest, indent=2) + "\n").encode()
    )
    return {
        "golden_id": golden_id,
        "defect_id": defect_id,
        "images": [str(artifact / name) for name in files],
        "export_manifest": manifest_path,
        "item_count": len(files),
    }
