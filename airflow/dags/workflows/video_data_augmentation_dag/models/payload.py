# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Runtime payload schema for the Video Data Augmentation workflow."""

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from dags.shared.models import (
    CaptioningTaskConfig,
    CosmosTaskConfig,
    DetectionAndTrackingTaskConfig,
    ReasoningTaskConfig,
    ServiceLifecycleServiceConfig,
    ServiceLifecycleTaskConfig,
    SuperResolutionTaskConfig,
    TrainingExportTaskConfig,
    VisualQATaskConfig,
)

DEFAULT_INPUT_PATH = "s3://<your-input-bucket>/<your-workflow>"
DEFAULT_OUTPUT_DIRECTORY = "s3://<your-output-bucket>/<your-workflow>"


def _default_variable_distribution() -> dict:
    return {
        "variables": {
            "weather": {"cloudy": 0.5, "sunny": 0.5},
            "time_of_day": {"morning": 0.5, "afternoon": 0.5},
        }
    }


class VideoDataAugmentationServiceLifecycleConfig(ServiceLifecycleTaskConfig):
    """Lifecycle settings for services used by Video Data Augmentation."""

    cosmos_transfer_service: ServiceLifecycleServiceConfig = Field(
        default_factory=ServiceLifecycleServiceConfig,
        description="Cosmos Transfer service lifecycle settings.",
    )


def _default_service_lifecycle() -> VideoDataAugmentationServiceLifecycleConfig:
    return VideoDataAugmentationServiceLifecycleConfig.model_validate(
        {
            "vlm_service": {"enabled": True, "replicas": 1},
            "llm_service": {"enabled": True, "replicas": 1},
            "cosmos_transfer_service": {"enabled": True, "replicas": 1},
        }
    )


def _default_cosmos_config() -> CosmosTaskConfig:
    return CosmosTaskConfig.model_validate(
        {
            "num_augmentation": 1,
            "external_services": False,
            "output_directory": DEFAULT_OUTPUT_DIRECTORY,
            "variable_distribution": _default_variable_distribution(),
        }
    )


def _default_task_config(config_type):
    return config_type(
        external_services=False,
        output_directory=DEFAULT_OUTPUT_DIRECTORY,
    )


def _default_output_task_config(config_type):
    return config_type(output_directory=DEFAULT_OUTPUT_DIRECTORY)


class VideoDataAugmentationDagConfig(BaseModel):
    """The input schema for the Video Data Augmentation DAG."""

    model_config = ConfigDict(extra="ignore")

    input_path: str = Field(
        default=DEFAULT_INPUT_PATH,
        description="Storage path to a single video file or a directory of video files.",
    )
    max_videos: int | None = Field(
        default=1,
        description="Maximum number of videos to process when input_path is a directory. When set to 0 or negative, all videos in the directory will be processed.",
    )
    output_directory: str = Field(
        default=DEFAULT_OUTPUT_DIRECTORY,
        description="The output directory to store the processed video.",
    )
    external_services: bool = Field(
        default=False,
        description=(
            "Top-level service mode switch. When true, provided service URLs are used for "
            "cosmos/captioning. When false, internal services are deployed and nested "
            "service URLs are ignored."
        ),
    )
    service_lifecycle: VideoDataAugmentationServiceLifecycleConfig = Field(
        default_factory=_default_service_lifecycle,
        description=(
            "VLM/LLM/Cosmos Transfer internal deployment flags. Each must be false when "
            "external_services is true, and true when external_services is false."
        ),
    )
    cosmos: CosmosTaskConfig = Field(
        default_factory=_default_cosmos_config,
        description="The input schema for the Cosmos augmentation services.",
    )
    captioning: CaptioningTaskConfig = Field(
        default_factory=lambda: _default_task_config(CaptioningTaskConfig),
        description="Configuration for video captioning.",
    )
    visual_qa: VisualQATaskConfig = Field(
        default_factory=lambda: _default_task_config(VisualQATaskConfig),
        description="Configuration for visual question-answering.",
    )
    reasoning: ReasoningTaskConfig = Field(
        default_factory=lambda: _default_task_config(ReasoningTaskConfig),
        description="Configuration for reasoning label generation.",
    )
    training_export: TrainingExportTaskConfig = Field(
        default_factory=lambda: _default_output_task_config(TrainingExportTaskConfig),
        description="Configuration for non-scaled training dataset export.",
    )
    super_resolution: SuperResolutionTaskConfig = Field(
        default_factory=lambda: _default_output_task_config(SuperResolutionTaskConfig),
        description="Configuration for video super-resolution.",
    )
    detection_and_tracking: DetectionAndTrackingTaskConfig = Field(
        default_factory=lambda: _default_output_task_config(DetectionAndTrackingTaskConfig),
        description="Configuration for object detection and tracking.",
    )

    @field_validator("max_videos")
    @classmethod
    def normalize_unlimited_max_videos(cls, value: int | None) -> int | None:
        """Use ``None`` as the canonical representation of an unlimited input count."""
        if value is not None and value <= 0:
            return None
        return value

    @model_validator(mode="before")
    @classmethod
    def populate_nested_payload(cls, data):
        if not isinstance(data, dict):
            return data

        output_directory = data.get("output_directory", DEFAULT_OUTPUT_DIRECTORY)
        external_services = bool(data.get("external_services", False))

        cosmos_cfg = data.get("cosmos")
        if cosmos_cfg is None:
            data["cosmos"] = {
                "output_directory": output_directory,
                "external_services": external_services,
                "variable_distribution": _default_variable_distribution(),
            }
        elif isinstance(cosmos_cfg, dict):
            if "output_directory" not in cosmos_cfg:
                cosmos_cfg["output_directory"] = output_directory
            if cosmos_cfg.get("external_services") is None:
                cosmos_cfg["external_services"] = external_services

        captioning_cfg = data.get("captioning")
        if captioning_cfg is None:
            data["captioning"] = {
                "output_directory": output_directory,
                "external_services": external_services,
            }
        elif isinstance(captioning_cfg, dict):
            if "output_directory" not in captioning_cfg:
                captioning_cfg["output_directory"] = output_directory
            if captioning_cfg.get("external_services") is None:
                captioning_cfg["external_services"] = external_services

        visual_qa_cfg = data.get("visual_qa")
        if visual_qa_cfg is None:
            data["visual_qa"] = {
                "output_directory": output_directory,
                "external_services": external_services,
            }
        elif isinstance(visual_qa_cfg, dict):
            if "output_directory" not in visual_qa_cfg:
                visual_qa_cfg["output_directory"] = output_directory
            if visual_qa_cfg.get("external_services") is None:
                visual_qa_cfg["external_services"] = external_services

        reasoning_cfg = data.get("reasoning")
        if reasoning_cfg is None:
            data["reasoning"] = {
                "output_directory": output_directory,
                "external_services": external_services,
            }
        elif isinstance(reasoning_cfg, dict):
            if "output_directory" not in reasoning_cfg:
                reasoning_cfg["output_directory"] = output_directory
            if reasoning_cfg.get("external_services") is None:
                reasoning_cfg["external_services"] = external_services

        training_export_cfg = data.get("training_export")
        if training_export_cfg is None:
            data["training_export"] = {
                "output_directory": output_directory,
            }
        elif isinstance(training_export_cfg, dict):
            if "output_directory" not in training_export_cfg:
                training_export_cfg["output_directory"] = output_directory

        super_resolution_cfg = data.get("super_resolution")
        if super_resolution_cfg is None:
            data["super_resolution"] = {
                "output_directory": output_directory,
            }
        elif isinstance(super_resolution_cfg, dict):
            if "output_directory" not in super_resolution_cfg:
                super_resolution_cfg["output_directory"] = output_directory

        detection_and_tracking_cfg = data.get("detection_and_tracking")
        if detection_and_tracking_cfg is None:
            data["detection_and_tracking"] = {
                "output_directory": output_directory,
            }
        elif isinstance(detection_and_tracking_cfg, dict):
            if "output_directory" not in detection_and_tracking_cfg:
                detection_and_tracking_cfg["output_directory"] = output_directory

        deploy_internal = not external_services
        sl_cfg = data.get("service_lifecycle")
        if sl_cfg is None:
            data["service_lifecycle"] = {
                "vlm_service": {"enabled": deploy_internal},
                "llm_service": {"enabled": deploy_internal},
                "cosmos_transfer_service": {"enabled": deploy_internal},
            }
        elif isinstance(sl_cfg, dict):
            for service_key in ("vlm_service", "llm_service", "cosmos_transfer_service"):
                entry = sl_cfg.get(service_key)
                if entry is None:
                    sl_cfg[service_key] = {"enabled": deploy_internal}
                elif isinstance(entry, dict) and entry.get("enabled") is None:
                    entry["enabled"] = deploy_internal

        return data

    @model_validator(mode="after")
    def check_service_lifecycle_matches_external_services(self):
        expected = not self.external_services
        if self.service_lifecycle.vlm_service.enabled != expected:
            raise ValueError(
                "service_lifecycle.vlm_service.enabled must be false when external_services "
                "is true, and true when external_services is false"
            )
        if self.service_lifecycle.llm_service.enabled != expected:
            raise ValueError(
                "service_lifecycle.llm_service.enabled must be false when external_services "
                "is true, and true when external_services is false"
            )
        if self.service_lifecycle.cosmos_transfer_service.enabled != expected:
            raise ValueError(
                "service_lifecycle.cosmos_transfer_service.enabled must be false when "
                "external_services is true, and true when external_services is false"
            )
        return self

    @model_validator(mode="after")
    def check_service_url_requirements(self):
        if self.cosmos.external_services:
            if not self.cosmos.vlm_service_url:
                raise ValueError(
                    "cosmos.vlm_service_url is required when cosmos.external_services is true"
                )
            if not self.cosmos.llm_service_url:
                raise ValueError(
                    "cosmos.llm_service_url is required when cosmos.external_services is true"
                )

        if self.captioning.external_services:
            if not self.captioning.vlm_service_url:
                raise ValueError(
                    "captioning.vlm_service_url is required when "
                    "captioning.external_services is true"
                )
            if self.captioning.enable_llm_summary and not self.captioning.llm_service_url:
                raise ValueError(
                    "captioning.llm_service_url is required when "
                    "captioning.external_services is true and enable_llm_summary is true"
                )
        if self.visual_qa.external_services:
            if not self.visual_qa.vlm_service_url:
                raise ValueError(
                    "visual_qa.vlm_service_url is required when visual_qa.external_services is true"
                )
            if not self.visual_qa.llm_service_url:
                raise ValueError(
                    "visual_qa.llm_service_url is required when visual_qa.external_services is true"
                )
        if self.reasoning.external_services and not self.reasoning.llm_service_url:
            raise ValueError(
                "reasoning.llm_service_url is required when reasoning.external_services is true"
            )
        return self

    @model_validator(mode="after")
    def check_nested_output_directory_matches_top_level(self):
        if self.cosmos.output_directory != self.output_directory:
            raise ValueError("cosmos.output_directory must match top-level output_directory")
        if self.captioning.output_directory != self.output_directory:
            raise ValueError("captioning.output_directory must match top-level output_directory")
        if self.visual_qa.output_directory != self.output_directory:
            raise ValueError("visual_qa.output_directory must match top-level output_directory")
        if self.reasoning.output_directory != self.output_directory:
            raise ValueError("reasoning.output_directory must match top-level output_directory")
        if self.training_export.output_directory != self.output_directory:
            raise ValueError(
                "training_export.output_directory must match top-level output_directory"
            )
        if self.super_resolution.output_directory != self.output_directory:
            raise ValueError(
                "super_resolution.output_directory must match top-level output_directory"
            )
        if self.detection_and_tracking.output_directory != self.output_directory:
            raise ValueError(
                "detection_and_tracking.output_directory must match top-level output_directory"
            )
        return self
