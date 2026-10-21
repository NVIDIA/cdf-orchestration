# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""SMT payloads for simulation component images, without tile or ROI settings."""

from pathlib import PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dags.shared.models import (
    CosmosTaskConfig,
    ServiceLifecycleServiceConfig,
    ServiceLifecycleTaskConfig,
)


class VariantConfiguration(BaseModel):
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


class SimulationOutputConfig(BaseModel):
    """Select a simulation capture pass, or optionally one image for a small run."""

    model_config = ConfigDict(extra="forbid")

    pass_name: str = Field(default="component_focus", pattern=r"^[A-Za-z0-9_-]+$")
    image: str | None = Field(
        default=None,
        min_length=1,
        description="Path relative to simulation output, listed in the selected pass's manifest.",
    )

    @model_validator(mode="after")
    def check_relative_image(self):
        if self.image is None:
            return self
        path = PurePosixPath(self.image)
        if path.is_absolute() or ".." in path.parts or ":" in self.image or "\\" in self.image:
            raise ValueError("simulation_output.image must be relative to the simulation output")
        if not path.parts:
            raise ValueError("simulation_output.image must name a file")
        return self


class AnomalyInputConfig(BaseModel):
    """Map simulation component types to AnomalyGen dataset class directories."""

    model_config = ConfigDict(extra="forbid")

    class_by_component_type: dict[str, str] = Field(min_length=1)

    @model_validator(mode="after")
    def check_class_mapping(self):
        for component_type, class_name in self.class_by_component_type.items():
            if not component_type.strip():
                raise ValueError("anomaly_input.class_by_component_type keys must not be blank")
            if (
                not class_name.strip()
                or class_name in {".", ".."}
                or any(character in class_name for character in "/\\\x00")
            ):
                raise ValueError(
                    "anomaly_input.class_by_component_type values must name a subdirectory"
                )
        return self


class SmtServiceLifecycleConfig(ServiceLifecycleTaskConfig):
    image_edit_service: ServiceLifecycleServiceConfig = Field(
        default_factory=ServiceLifecycleServiceConfig
    )


class SmtCosmosTaskConfig(CosmosTaskConfig):
    # The shared placeholder is truthy and would bypass the external-service requirement.
    image_edit_service_url: str | None = Field(
        default=None,
        description="External image-edit endpoint; required for external full or sim2real runs.",
    )
    image_edit_model: str | None = Field(
        default=None, description="Request model override; defaults to the SMT deployment manifest."
    )


class SmtDefectImageGenerationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_version: Literal["1.0"]
    input_scene: str | None = None
    output_directory: str
    golden_generation: ConfigurationCollection | None = Field(
        default=None, description="image_generation, simulation_output, and augmentation overrides."
    )
    defect_generation: ConfigurationCollection | None = Field(
        default=None, description="anomaly_input and anomaly_generation overrides."
    )
    pipeline_mode: Literal["simulation", "sim2real", "full", "defect"] = "full"
    enable_performance_reporting: bool = Field(
        default=False,
        description=(
            "Write YAML orchestration statistics and a self-contained graphical HTML "
            "dashboard after the workflow finishes."
        ),
    )
    external_services: bool = False
    cosmos: SmtCosmosTaskConfig = Field(default_factory=SmtCosmosTaskConfig)
    service_lifecycle: SmtServiceLifecycleConfig = Field(default_factory=SmtServiceLifecycleConfig)

    @model_validator(mode="before")
    @classmethod
    def populate_service_settings(cls, data):
        if not isinstance(data, dict):
            return data
        data = dict(data)
        external = data.get("external_services", False)
        cosmos = dict(data.get("cosmos") or {})
        for key, expected in (
            ("external_services", external),
            ("output_directory", data.get("output_directory")),
        ):
            if key in cosmos and cosmos[key] is not None and cosmos[key] != expected:
                raise ValueError(f"cosmos.{key} must match top-level {key}")
            if expected is not None:
                cosmos[key] = expected
        data["cosmos"] = cosmos
        lifecycle = dict(data.get("service_lifecycle") or {})
        service = dict(lifecycle.get("image_edit_service") or {})
        service["enabled"] = not external and data.get("pipeline_mode", "full") in {
            "full",
            "sim2real",
        }
        lifecycle["image_edit_service"] = service
        data["service_lifecycle"] = lifecycle
        return data

    @model_validator(mode="after")
    def check_workflow_requirements(self):
        if self.pipeline_mode in {"full", "defect"} and self.defect_generation is None:
            raise ValueError("defect_generation is required for full or defect mode")
        if self.pipeline_mode == "defect":
            return self
        if self.input_scene is None or self.golden_generation is None:
            raise ValueError("input_scene and golden_generation are required outside defect mode")
        if (
            self.pipeline_mode in {"full", "sim2real"}
            and self.external_services
            and not self.cosmos.image_edit_service_url
        ):
            raise ValueError("cosmos.image_edit_service_url is required for external services")
        return self
