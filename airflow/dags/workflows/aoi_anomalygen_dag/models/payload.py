# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Payload for aoi_anomalygen_dag: the DIG DAG's defect-mode payload, AnomalyGen only.

Same contract as defect_image_generation_smt_dag with ``pipeline_mode: defect``
(skills/cdf-defect-image-generation/references/payload-contract.md, "Defect-only input"): a DIG
defect-only payload is accepted as is. One entry of ``defect_generation.configurations`` is one
AnomalyGen pod; its ``anomaly_generation`` dictionary overrides the run_pipeline template
configs/anomaly_gen_config.yaml (merge order: template -> defaults -> configuration). The checks
below are the DIG payload checker's (skills/cdf-defect-image-generation/scripts/payload.py) for
the fields defect mode reads, so a bad payload fails in validate_payload, before any GPU is used.
"""

from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Override names a configuration may carry. DIG also accepts anomaly_input, which only full mode
# reads (it maps simulated component types to classes); defect mode has no use for it.
COMPONENTS = ("anomaly_generation",)

# run_pipeline keys the workflow sets (payload-contract.md, "Component overrides").
WORKFLOW_KEYS = ("input", "output", "recipe", "checkpoint")

# Stages a run must keep so it publishes (DIG checker REQUIRED_STAGES).
REQUIRED_STAGES = {"prepare", "generate", "export", "publish"}

STORAGE_SCHEMES = {"s3", "msc"}


def check_location(value: Any, field: str) -> None:
    """An explicit storage URL: no placeholders, credentials, query or fragment (DIG location())."""
    if not isinstance(value, str) or not value.strip() or any(c in value for c in "<>\n\r"):
        raise ValueError(f"{field} requires an explicit location without placeholders")
    parsed = urlsplit(value)
    if parsed.scheme not in STORAGE_SCHEMES or not parsed.netloc:
        raise ValueError(f"{field} requires a URL using {', '.join(sorted(STORAGE_SCHEMES))}")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(f"{field} must not embed credentials, query parameters or fragments")


def check_anomaly_generation(override: dict[str, Any], where: str) -> None:
    """The parts of one anomaly_generation override that defect mode depends on."""
    managed = [key for key in WORKFLOW_KEYS if key in override]
    staging = override.get("staging")
    if staging is not None and not isinstance(staging, dict):
        raise ValueError(f"{where}: staging must be a dictionary")
    staging = staging or {}
    if "publish" in staging:
        managed.append("staging.publish")
    if managed:
        raise ValueError(f"{where}: anomaly_generation may not set {managed}; the workflow sets them")

    counts = override.get("per_defect_counts")
    if counts is not None and (
        not isinstance(counts, dict)
        or not counts
        or any(
            "+" not in str(key) or isinstance(value, bool) or not isinstance(value, int) or value < 1
            for key, value in counts.items()
        )
    ):
        raise ValueError(f"{where}: per_defect_counts must map '<class>+<defect>' to a positive integer count")

    if "stages" in override:
        stages = override["stages"]
        if not isinstance(stages, list) or not all(isinstance(s, str) for s in stages):
            raise ValueError(f"{where}: stages must be a list of stage names")
        missing = sorted(REQUIRED_STAGES - set(stages))
        if missing:
            raise ValueError(f"{where}: stages must keep {missing}, or nothing is published")

    num_gpus = override.get("num_gpus")
    if num_gpus is not None and (isinstance(num_gpus, bool) or not isinstance(num_gpus, int) or num_gpus < 1):
        raise ValueError(f"{where}: num_gpus must be a positive integer")

    fetch = staging.get("fetch")
    if fetch is None:
        return
    if not isinstance(fetch, dict):
        raise ValueError(f"{where}: staging.fetch must be a dictionary")
    if "input" in fetch:
        if not isinstance(fetch["input"], list) or not fetch["input"]:
            raise ValueError(f"{where}: staging.fetch.input must be a nonempty list of locations")
        for i, item in enumerate(fetch["input"]):
            check_location(item, f"{where}: staging.fetch.input[{i}]")
    for key in ("recipe", "checkpoint", "base_checkpoint"):
        if fetch.get(key) is not None:
            check_location(fetch[key], f"{where}: staging.fetch.{key}")
    override_map = fetch.get("input_override")
    if override_map is not None:
        if not isinstance(override_map, dict) or not override_map:
            raise ValueError(f"{where}: staging.fetch.input_override must map a dataset path to sources")
        for path, sources in override_map.items():
            items = [sources] if isinstance(sources, str) else sources
            if not isinstance(items, list) or not items:
                raise ValueError(f"{where}: staging.fetch.input_override[{path!r}] must be a location or a list")
            for item in items:
                check_location(item, f"{where}: staging.fetch.input_override[{path!r}]")


class VariantConfiguration(BaseModel):
    """One defect configuration: an id plus component override dictionaries."""

    model_config = ConfigDict(extra="allow")

    id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

    @model_validator(mode="after")
    def check_component_overrides(self):
        if any(not isinstance(value, dict) for value in (self.model_extra or {}).values()):
            raise ValueError("configuration component overrides must be dictionaries")
        return self


class ConfigurationCollection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    defaults: dict[str, dict[str, Any]] = Field(default_factory=dict)
    configurations: list[VariantConfiguration] = Field(min_length=1)

    @model_validator(mode="after")
    def check_unique_ids(self):
        ids = [item.id for item in self.configurations]
        if len(ids) != len(set(ids)):
            raise ValueError("configuration ids must be unique")
        return self

    @model_validator(mode="after")
    def check_overrides(self):
        sources = [("defaults", self.defaults)] + [
            (f"configuration {item.id!r}", item.model_extra or {}) for item in self.configurations
        ]
        for where, overrides in sources:
            unknown = sorted(set(overrides) - set(COMPONENTS))
            if unknown:
                raise ValueError(f"{where}: unknown override(s) {unknown}; allowed: {list(COMPONENTS)}")
            check_anomaly_generation(overrides.get("anomaly_generation") or {}, where)
        return self


class AoiAnomalyGenPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_version: Literal["1.0"]
    output_directory: str = Field(
        description="Run outputs go to <output_directory>/<run_id>/defects/<configuration id>/."
    )
    defect_generation: ConfigurationCollection = Field(description="anomaly_generation overrides.")
    evaluate: bool = Field(
        default=False,
        description=(
            "Also score every generated image with AnomalyGen evaluate after publish and upload "
            "kpi.json. Off by default: per-image scores belong to the Quality Gate."
        ),
    )
    # Accepted so a DIG defect-only payload runs unchanged; this DAG only runs defect mode.
    pipeline_mode: Literal["defect"] = Field(default="defect", description="Always defect (AnomalyGen only).")
    external_services: bool = Field(default=False, description="No effect in defect mode; leave false.")
    enable_performance_reporting: bool = Field(
        default=False, description="Not part of this DAG (SRD Figure 4); must stay false."
    )

    @field_validator("output_directory")
    @classmethod
    def check_output_directory(cls, value: str) -> str:
        check_location(value, "output_directory")
        return value

    @model_validator(mode="after")
    def check_dig_fields(self):
        if self.external_services:
            raise ValueError("external_services only affects image edit, which defect mode does not run; leave it false")
        if self.enable_performance_reporting:
            raise ValueError("enable_performance_reporting is not supported by aoi_anomalygen_dag")
        return self


def default_payload() -> dict[str, Any]:
    """Example conf shown in the Airflow UI; replace every <...> before a run."""
    vlm = "<vlm-model>"
    return {
        "api_version": "1.0",
        "output_directory": "s3://example-bucket/aoi-anomalygen",
        "defect_generation": {
            "defaults": {
                "anomaly_generation": {
                    "staging": {
                        "fetch": {
                            "input": [
                                "s3://example-bucket/assets/UC1/",
                                "s3://example-bucket/prepared-components/",
                            ],
                            "recipe": "s3://example-bucket/recipes/exp_texture_ft_pcb.yaml",
                            "checkpoint": "s3://example-bucket/models/iter_000015000.pt",
                        }
                    },
                    "amg_gen_model": vlm,
                    "amg_judge_model": vlm,
                    "amg_endpoints": [{"model": vlm, "base_url": "https://<vlm-host>/v1"}],
                }
            },
            "configurations": [
                {"id": "ic-bridge", "anomaly_generation": {"per_defect_counts": {"IC+bridge": 3}}},
            ],
        },
        "evaluate": False,
    }
