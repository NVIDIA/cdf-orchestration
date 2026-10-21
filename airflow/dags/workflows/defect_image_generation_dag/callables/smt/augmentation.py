# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Map one CPU augmentation task per simulation component image."""

import copy
import json
from pathlib import PurePosixPath

import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import CosmosOutputResult
from dags.shared.task_groups.cosmos import load_cosmos_base_config
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.msc_utils import is_file, write_file_to_directory
from dags.workflows.defect_image_generation_dag.callables.config_overrides import (
    apply_component_overrides,
)
from dags.workflows.defect_image_generation_dag.callables.smt.config_generation import (
    CONFIG_DIR,
    build_work_items,
    validate_payload,
)
from dags.workflows.defect_image_generation_dag.callables.smt.images import (
    load_augmented_images,
    load_simulation_images,
)


def generate_smt_augmentation_configs(
    cosmos_config=None, run_id="", *, image_edit_model: str, **context
) -> list[str]:
    payload = validate_payload(cosmos_config)
    goldens = build_work_items(payload.model_dump(), run_id)["golden_work_items"]
    endpoint_url = (
        payload.cosmos.image_edit_service_url
        if payload.external_services
        else require_service_endpoint_from_xcom(context["ti"], "image_edit_service")
    )
    base = load_cosmos_base_config(
        base_config_path=payload.cosmos.base_config_path,
        default_path=CONFIG_DIR / "cosmos_config.yaml",
        description="SMT augmentation config",
    )
    paths = []
    for variant, golden in zip(payload.golden_generation.configurations, goldens, strict=True):
        config = apply_component_overrides(
            base,
            defaults=payload.golden_generation.defaults,
            configuration=variant,
            component_names=("augmentation",),
        )
        output = URL(golden["output_directory"]) / "augmentation"
        for endpoint in config["endpoints"]:
            if endpoint.get("id") == "image_edit" or endpoint.get("role") == "image_edit":
                endpoint.update(
                    url=endpoint_url, model=payload.cosmos.image_edit_model or image_edit_model
                )
        images = load_simulation_images(golden)
        for item in images:
            if not is_file(item["simulation_image"]):
                raise AirflowFailException(
                    f"Missing SMT simulation image: {item['simulation_image']}"
                )
            name = PurePosixPath(item["file"]).stem
            item["augmented_image"] = str(output / "artifact" / item["file"])
            image_config = copy.deepcopy(config)
            image_config["data"] = [
                {
                    "inputs": {"rgb": item["simulation_image"]},
                    "output": {
                        "video": item["augmented_image"],
                        "caption": str(output / "captions" / f"{name}.txt"),
                        "metadata": str(output / "metadata" / f"{name}.json"),
                    },
                }
            ]
            path = str(output / "configs" / f"{name}.yaml")
            write_file_to_directory(path, yaml.safe_dump(image_config, sort_keys=False).encode())
            paths.append(path)
        write_file_to_directory(
            golden["augmentation_manifest"],
            (json.dumps({"images": images}, indent=2) + "\n").encode(),
        )
    return paths


def validate_smt_augmentation_outputs(cosmos_config=None, run_id="", **context) -> dict:
    goldens = build_work_items(cosmos_config, run_id)["golden_work_items"]
    videos, missing = [], []
    for golden in goldens:
        for item in load_augmented_images(golden["augmentation_manifest"]):
            if not is_file(item["augmented_image"]):
                missing.append(item["augmented_image"])
            videos.append(
                {
                    "video_key": f"{golden['golden_id']}/{item['file']}",
                    "video_path": item["augmented_image"],
                }
            )
    if missing:
        raise AirflowFailException(f"Missing SMT augmentation outputs: {missing}")
    return CosmosOutputResult(
        length=len(videos),
        videos=videos,
    ).model_dump()
