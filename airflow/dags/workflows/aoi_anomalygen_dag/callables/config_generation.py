# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""generate_configs: one AnomalyGen work item per defect configuration.

Adapted from the DIG DAG's defect-mode branch (defect_image_generation_dag
callables/smt/config_generation.py build_work_items, and callables/config_overrides.py).
"""

from __future__ import annotations

import ast
import copy
import json
import re
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException
from pydantic import ValidationError
from yarl import URL

from dags.workflows.aoi_anomalygen_dag.models.payload import (
    AoiAnomalyGenPayload,
    ConfigurationCollection,
    VariantConfiguration,
)

CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs"


def validate_payload(payload: str | dict[str, Any]) -> AoiAnomalyGenPayload:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            try:
                payload = ast.literal_eval(payload)
            except (SyntaxError, ValueError) as exc:
                raise AirflowFailException("Invalid AOI AnomalyGen payload") from exc
    try:
        return AoiAnomalyGenPayload.model_validate(payload)
    except ValidationError as exc:
        raise AirflowFailException(str(exc)) from exc


def load_config(name: str) -> dict[str, Any]:
    return yaml.safe_load((CONFIG_DIR / name).read_text())


def _deep_merge_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge dictionaries while replacing lists and scalar values."""
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _deep_merge_config(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _anomaly_config(collection: ConfigurationCollection, defect: VariantConfiguration) -> dict[str, Any]:
    """Template -> collection defaults -> configuration, as DIG merges its component overrides."""
    runner = load_config("anomaly_gen_config.yaml")
    for source in (collection.defaults, defect.model_extra or {}):
        override = source.get("anomaly_generation")
        if override:
            runner = _deep_merge_config(runner, override)
    # A defect selection replaces the previous selection across workflow overrides.
    for source in (collection.defaults, defect.model_extra or {}):
        overrides = source.get("anomaly_generation", {})
        if "per_defect_counts" in overrides:
            runner["per_defect_counts"] = copy.deepcopy(overrides["per_defect_counts"])
    return runner


def requested_count(runner_config: dict[str, Any]) -> int:
    """What run_pipeline will deliver on success: the per_defect_counts total, else num_sdg."""
    counts = runner_config.get("per_defect_counts")
    if counts:
        return sum(int(v) for v in counts.values())
    return int(runner_config.get("num_sdg") or 0)


def generate_configs(payload: str | dict[str, Any], run_id: str = "", **_context) -> list[dict[str, Any]]:
    """Resolve each defect configuration into anomaly_package arguments; reads no storage."""
    validated = validate_payload(payload)
    run_output = URL(validated.output_directory.rstrip("/"))
    if run_id:
        run_output /= run_id
    collection = validated.defect_generation
    items = []
    for defect in collection.configurations:
        runner = _anomaly_config(collection, defect)
        fetch = (runner.get("staging") or {}).get("fetch") or {}
        if not fetch.get("input"):
            raise AirflowFailException(
                f"Defect configuration {defect.id}: anomaly_generation.staging.fetch.input is empty; "
                "list the shared reference assets and the prepared clean-image dataset"
            )
        # The template's and the UI example's values are <placeholders> (locations, VLM model and
        # endpoint); a run must replace every one of them.
        placeholders = sorted(set(re.findall(r"<[^<>\"]+>", json.dumps(runner))))
        if placeholders:
            raise AirflowFailException(
                f"Defect configuration {defect.id}: replace the placeholder(s) {placeholders} in "
                "anomaly_generation (the template and the UI example only have example values)"
            )
        items.append(
            {
                "output_directory": str(run_output / "defects" / defect.id),
                "anomaly_generation_config": runner,
                "defect_id": defect.id,
                "evaluate": validated.evaluate,
            }
        )
    return items
