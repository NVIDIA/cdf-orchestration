# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generate independent SMT simulation and whole-image work items."""

from __future__ import annotations

import ast
import base64
import copy
import json
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException
from pydantic import ValidationError
from yarl import URL

from dags.workflows.defect_image_generation_dag.callables.config_overrides import (
    apply_component_overrides,
)
from dags.workflows.defect_image_generation_dag.callables.smt.models import (
    AnomalyInputConfig,
    ConfigurationCollection,
    SimulationOutputConfig,
    SmtDefectImageGenerationPayload,
    VariantConfiguration,
)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "configs" / "smt"


def default_payload() -> SmtDefectImageGenerationPayload:
    return SmtDefectImageGenerationPayload(
        api_version="1.0",
        input_scene="s3://<your-input-bucket>/<your-workflow>",
        output_directory="s3://<your-output-bucket>/<your-workflow>",
        golden_generation={"configurations": [{"id": "golden-default"}]},
        defect_generation={"configurations": [{"id": "defect-default"}]},
    )


def validate_payload(payload: str | dict[str, Any]) -> SmtDefectImageGenerationPayload:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            try:
                payload = ast.literal_eval(payload)
            except (SyntaxError, ValueError) as exc:
                raise AirflowFailException("Invalid SMT payload") from exc
    try:
        return SmtDefectImageGenerationPayload.model_validate(payload)
    except ValidationError as exc:
        raise AirflowFailException(str(exc)) from exc


def load_config(name: str) -> dict[str, Any]:
    return yaml.safe_load((CONFIG_DIR / name).read_text())


def _anomaly_config(
    collection: ConfigurationCollection, defect: VariantConfiguration
) -> dict[str, Any]:
    runner = apply_component_overrides(
        load_config("anomaly_gen_config.yaml"),
        defaults=collection.defaults,
        configuration=defect,
        component_names=("anomaly_generation",),
    )
    # A defect selection replaces the previous selection across workflow overrides.
    for source in (collection.defaults, defect.model_extra or {}):
        overrides = source.get("anomaly_generation", {})
        if "per_defect_counts" in overrides:
            runner["per_defect_counts"] = copy.deepcopy(overrides["per_defect_counts"])
    return runner


def build_work_items(payload: str | dict[str, Any], run_id: str = "") -> dict[str, Any]:
    """Resolve paths from the payload; no image contents or tile metadata are required."""
    validated = validate_payload(payload)
    if not validated.output_directory:
        raise AirflowFailException("Runtime payload must supply output_directory")
    if validated.pipeline_mode != "defect" and not validated.input_scene:
        raise AirflowFailException("Runtime payload must supply input_scene and output_directory")
    run_output = URL(validated.output_directory.rstrip("/"))
    if run_id:
        run_output /= run_id
    if validated.pipeline_mode == "defect":
        collection = validated.defect_generation
        return {
            "golden_work_items": [],
            "image_generation_environments": [],
            "anomaly_package_kwargs": [
                {
                    "augmentation_manifest": None,
                    "class_by_component_type": None,
                    "output_directory": str(run_output / "defects" / defect.id),
                    "anomaly_generation_config": _anomaly_config(collection, defect),
                    "defect_id": defect.id,
                }
                for defect in collection.configurations
            ],
            "output_directory": str(run_output),
        }
    goldens, defects = [], []
    for variant in validated.golden_generation.configurations:
        golden_output = run_output / "goldens" / variant.id
        simulation_output = golden_output / "simulation"
        config = apply_component_overrides(
            load_config("image_generation_config.yaml"),
            defaults=validated.golden_generation.defaults,
            configuration=variant,
            component_names=("image_generation",),
        )
        config["scene"]["usd"] = validated.input_scene
        config["output_dir"] = str(simulation_output)
        output_config = SimulationOutputConfig()
        if validated.pipeline_mode != "simulation":
            output_config = apply_component_overrides(
                {},
                defaults=validated.golden_generation.defaults,
                configuration=variant,
                component_names=("simulation_output",),
            )
            try:
                output_config = SimulationOutputConfig.model_validate(output_config)
            except ValidationError as exc:
                raise AirflowFailException(f"Golden {variant.id}: {exc}") from exc
        golden = {
            "golden_id": variant.id,
            "simulation_directory": str(simulation_output),
            "simulation_pass": output_config.pass_name,
            "simulation_image": (
                str(simulation_output / output_config.image) if output_config.image else None
            ),
            "augmentation_manifest": str(golden_output / "augmentation" / "manifest.json"),
            "output_directory": str(golden_output),
            "image_generation_environment": [
                "PCB_AOI_CONFIG_B64:" + base64.b64encode(yaml.safe_dump(config).encode()).decode()
            ],
        }
        goldens.append(golden)
        if validated.pipeline_mode != "full":
            continue
        collection = validated.defect_generation
        for defect in collection.configurations:
            runner = _anomaly_config(collection, defect)
            assignment = apply_component_overrides(
                {},
                defaults=collection.defaults,
                configuration=defect,
                component_names=("anomaly_input",),
            )
            try:
                input_config = AnomalyInputConfig.model_validate(assignment)
            except ValidationError as exc:
                raise AirflowFailException(f"Defect {defect.id}: {exc}") from exc
            defects.append(
                {
                    "augmentation_manifest": golden["augmentation_manifest"],
                    "class_by_component_type": input_config.class_by_component_type,
                    "output_directory": str(run_output / "defects" / variant.id / defect.id),
                    "anomaly_generation_config": runner,
                    "golden_id": variant.id,
                    "defect_id": defect.id,
                }
            )
    return {
        "golden_work_items": goldens,
        "image_generation_environments": [item["image_generation_environment"] for item in goldens],
        "anomaly_package_kwargs": defects,
        "output_directory": str(run_output),
    }


def generate_smt_configs(payload: str | dict[str, Any], run_id: str = "", **context) -> dict:
    result = build_work_items(payload, run_id)
    if context.get("ti") is not None:
        for key, value in result.items():
            context["ti"].xcom_push(key=key, value=value)
    return result


def select_generated_config_list(config_key: str, **context) -> list:
    return context["ti"].xcom_pull(task_ids="generate_configs", key=config_key)
