# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Runtime schema for AV retrieval and optional RGB or WSM Cosmos3-Nano generation."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

COSMOS_MODEL = "nvidia/Cosmos3-Nano"
DEFAULT_CDS_PIPELINE = "cosmos_video_search_milvus"


class Cosmos3NanoTaskConfig(BaseModel):
    """Configure Cosmos3-Nano generation with default RGB or optional WSM control.

    ``rgb`` uses edges derived from each selected retrieved video.
    ``wsm`` conditions on retrieved clips and their prepared WSM control videos.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    enabled: bool = False
    control_mode: Literal["wsm", "rgb"] = Field(
        default="rgb",
        description="Use RGB clips or prepared WSM controls.",
    )
    prompt: str | None = Field(default=None, min_length=1, max_length=8000)
    num_clips: int | None = Field(
        default=None,
        ge=1,
        le=100,
        description=("Maximum number of retrieved clips to augment, highest ranked first."),
    )
    num_augmentation: int = Field(default=1, ge=1, le=20)
    camera: Literal["camera_front_wide_120fov"] = "camera_front_wide_120fov"
    size: Literal["1280x720"] = "1280x720"
    num_frames: int = Field(default=93, ge=5, le=301)
    fps: int = Field(
        default=24,
        ge=1,
        le=60,
    )
    num_inference_steps: int = Field(
        default=35,
        ge=1,
        le=100,
    )
    guidance_scale: float = Field(default=5.0, ge=0, le=20)
    plausibility_threshold: float = Field(
        default=0.5,
        ge=0,
        le=1,
        description="Generated videos must have plausibility strictly above this threshold.",
    )
    seed: int | None = Field(
        default=None,
        ge=0,
        le=2147480000,
        description=("Base seed for generation. Null uses a random seed."),
    )
    timeout_seconds: int = Field(default=7200, ge=60, le=21600)

    @model_validator(mode="after")
    def _validate_transfer(self) -> "Cosmos3NanoTaskConfig":
        if self.enabled and not self.prompt:
            raise ValueError("cosmos.prompt is required when cosmos.enabled is true")
        return self


class AVDatasetGenerationPayload(BaseModel):
    """Configure AV retrieval and optional Cosmos3-Nano generation from RGB or WSM controls."""

    model_config = ConfigDict(extra="forbid")

    cosmos: Cosmos3NanoTaskConfig = Field(
        default_factory=Cosmos3NanoTaskConfig,
        description="Optional WSM or RGB Cosmos3-Nano generation.",
    )

    input_video_path: str = Field(
        description="S3 prefix or local directory containing AV Dataset Generation scene/front-camera videos.",
    )
    output_directory: str = Field(
        description="S3 prefix or local directory for run outputs.",
    )

    collection_name: str = Field(
        default="cdf-curation",
        min_length=1,
        description="CDS collection to create or reuse.",
    )
    pipeline: str = Field(
        default=DEFAULT_CDS_PIPELINE,
        min_length=1,
        description="CDS embedding pipeline for the collection.",
    )

    ingest_mode: Literal["documents"] = Field(
        default="documents",
        description="Post embedding documents to CDS over HTTP.",
    )

    search_mode: Literal["embedding", "none"] = "none"

    query_video: str | None = Field(
        default=None,
        description=(
            "MSC or local query video for embedding search; processed separately from the input "
            "limit. A corpus scene ID reuses corpus embeddings."
        ),
    )
    top_k: int = Field(
        default=10,
        ge=1,
        description="Maximum retrieved clips to return, excluding the query video.",
    )

    pixi_env: str = Field(
        default="default",
        description="Pixi environment used to run Curator.",
    )
    limit: int | None = Field(
        default=1,
        ge=1,
        description="Maximum input videos to process. Null processes all inputs.",
    )
    collect_result_clips: bool = Field(
        default=True,
        description="Copy retrieved clips into the run’s results directory.",
    )
    object_store_endpoint: str | None = Field(
        default=None,
        description="MSC endpoint override. Defaults to the MSC configuration.",
    )
    job_timeout_seconds: int = Field(
        default=900,
        ge=1,
        description="Timeout in seconds for CDS indexing and search readiness.",
    )

    @model_validator(mode="before")
    @classmethod
    def validate_payload_structure(cls, data):
        if not isinstance(data, dict):
            raise ValueError("payload must be a dictionary")
        if "augmentation" in data:
            raise ValueError(
                "Use the cosmos payload section instead of the draft augmentation section"
            )
        return data

    @model_validator(mode="after")
    def _check_service_wiring(self) -> "AVDatasetGenerationPayload":
        if self.search_mode == "embedding" and not (self.query_video or "").strip():
            raise ValueError("query_video is required for embedding search")
        if self.search_mode == "none" and self.cosmos.enabled:
            raise ValueError("search_mode 'none' cannot enable Cosmos generation")
        if self.cosmos.enabled:
            if self.cosmos.num_clips is not None and self.cosmos.num_clips > self.top_k:
                raise ValueError("cosmos.num_clips cannot exceed retrieval top_k")
        return self
