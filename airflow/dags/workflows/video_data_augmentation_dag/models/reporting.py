# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Models used by the Video Data Augmentation workflow reporting callable.
"""

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class CosmosMetadataHallucinationCheckDetails(BaseModel):
    """Details for the Cosmos hallucination check."""

    duration_seconds: float


class CosmosMetadataHallucinationCheck(BaseModel):
    """Cosmos hallucination check metadata."""

    passed: bool
    details: CosmosMetadataHallucinationCheckDetails


class CosmosMetadataAttributeCheckDetails(BaseModel):
    """Details for the Cosmos attribute check."""

    duration_seconds: float = Field(
        ..., description="The duration of the attribute check in seconds."
    )


class CosmosMetadataAttributeCheck(BaseModel):
    """Cosmos attribute check metadata."""

    passed: bool = Field(..., description="Whether the cosmos output passed the attribute check.")
    details: CosmosMetadataAttributeCheckDetails = Field(
        ..., description="The details of the attribute check for the cosmos output."
    )


class CosmosMetadataLoop(BaseModel):
    """Metadata for one Cosmos augmentation loop."""

    duration_seconds: float = Field(..., description="The duration of the loop in seconds.")
    augmentation_duration_seconds: float = Field(
        ..., description="The duration of the augmentation in seconds."
    )
    hallucination_check: CosmosMetadataHallucinationCheck = Field(
        ..., description="The hallucination check for the cosmos output."
    )
    attribute_verification_check: CosmosMetadataAttributeCheck | None = Field(
        None, description="The attribute check for the cosmos output."
    )


class CosmosMetadata(BaseModel):
    """Metadata emitted by Cosmos. Only the fields used by reporting are required."""

    passed: bool = Field(..., description="Whether the cosmos output passed the quality check.")
    selections: Dict[str, str] = Field(..., description="The selections of the cosmos output.")
    loops: List[CosmosMetadataLoop] = Field(..., description="The loops of the cosmos output.")


class AutoLabelingMCQGenerationVLMAnalysisResult(BaseModel):
    """Result for auto-labeling MCQ generation VLM analysis."""

    success: bool = Field(
        ...,
        description="Whether the auto-labeling MCQ generation VLM analysis passed the quality check.",
    )


class AutoLabelingMCQGenerationJSONExtractionResult(BaseModel):
    """Result for auto-labeling MCQ generation JSON extraction."""

    success: bool = Field(
        ...,
        description="Whether the auto-labeling MCQ generation JSON extraction passed the quality check.",
    )


class AutoLabelingMCQGenerationFrameExtractionResult(BaseModel):
    """Result for auto-labeling MCQ generation frame extraction."""

    success: bool = Field(
        ...,
        description="Whether the auto-labeling MCQ generation frame extraction passed the quality check.",
    )


class AutoLabelingMCQGenerationResult(BaseModel):
    """Result for auto-labeling MCQ generation."""

    vlm_analysis: AutoLabelingMCQGenerationVLMAnalysisResult = Field(
        ..., description="The result of the VLM analysis."
    )
    json_extraction: AutoLabelingMCQGenerationJSONExtractionResult = Field(
        ..., description="The result of the JSON extraction."
    )
    frame_extraction: AutoLabelingMCQGenerationFrameExtractionResult = Field(
        ..., description="The result of the frame extraction."
    )


class AutoLabelingSubcomponentMetadata(BaseModel):
    """Metadata for one auto-labeling subcomponent output."""

    total_videos: int = Field(
        ..., description="The number of videos in the auto-labeling MCQ output."
    )
    successful: int = Field(
        ..., description="The number of successful videos in the auto-labeling MCQ output."
    )
    failed: int = Field(
        ..., description="The number of failed videos in the auto-labeling MCQ output."
    )


class ReportOutput(BaseModel):
    """Output metadata for the SDG workflow report."""

    total: int = Field(..., description="The number of outputs.")
    successful: int = Field(..., description="The number of successful outputs.")
    failed: int = Field(..., description="The number of failed outputs.")
    success_percentage: float = Field(..., description="The success percentage of the outputs.")
    directory: str = Field(..., description="The directory of the output.")


class ReportAutoLabelingSubcomponent(BaseModel):
    """Auto-labeling subcomponent report summary."""

    total: int = Field(
        ...,
        description="The number of videos in the auto-labeling detection and tracking output.",
    )
    successful: int = Field(
        ...,
        description="The number of successful videos in the auto-labeling detection and tracking output.",
    )
    failed: int = Field(
        ...,
        description="The number of failed videos in the auto-labeling detection and tracking output.",
    )
    success_percentage: float = Field(
        ...,
        description="The success percentage of the auto-labeling detection and tracking output.",
    )


class ReportAutoLabeling(BaseModel):
    """Auto-labeling report summary."""

    total: int = Field(..., description="The number of videos in the auto-labeling input.")
    successful: int = Field(
        ..., description="The number of successful videos in the auto-labeling input."
    )
    failed: int = Field(..., description="The number of failed videos in the auto-labeling input.")
    success_percentage: float = Field(
        ..., description="The success percentage of the videos in the auto-labeling input."
    )


class ReportCosmosLoopChecker(BaseModel):
    """Checker summary for a Cosmos augmentation loop."""

    total: int = Field(0, description="The number of augmentations in the loop.")
    passed: int = Field(0, description="The number of augmentations that passed the checker.")
    failed: int = Field(0, description="The number of augmentations that failed the checker.")
    success_percentage: float = Field(
        0.0, description="The success percentage of the augmentations in the loop."
    )


class ReportCosmosLoop(BaseModel):
    """Cosmos augmentation loop report summary."""

    total: int = Field(0, description="The number of augmentations run in this loop.")
    passed: int = Field(
        0, description="The number of augmentations that produced successful output."
    )
    failed: int = Field(
        0, description="The number of augmentations that failed a check and must be retried."
    )
    success_percentage: float = Field(
        0.0, description="The success percentage of the augmentations in this loop."
    )
    checkers: Dict[str, ReportCosmosLoopChecker] = Field(
        {}, description="The checkers for the augmentations in this loop."
    )


class ReportCosmos(BaseModel):
    """Cosmos report summary."""

    total: int = Field(0, description="The number of successful augmentation loops.")
    successful: int = Field(0, description="The number of successful augmentation loops.")
    failed: int = Field(0, description="The number of failed augmentation loops.")
    success_percentage: float = Field(
        0.0, description="The success percentage of the augmentation loops."
    )
    average_number_of_loops: float = Field(
        0.0, description="The average number of loops per video."
    )
    augmentation_loops: List[ReportCosmosLoop] = Field(
        [], description="The augmentation loops for the cosmos output."
    )


class ReportTimingTask(BaseModel):
    """Timing metadata for an Airflow task."""

    start_time: str | None = Field(None, description="The start time of the task.")
    end_time: str | None = Field(None, description="The end time of the task.")
    duration_seconds: float | None = Field(None, description="The duration of the task in seconds.")


class ReportTimingCosmosSubcomponent(BaseModel):
    """Timing details for a Cosmos augmentation subcomponent."""

    count: int = Field(0, description="The number of subcomponents.")
    min_duration_seconds: float = Field(
        0.0, description="The minimum duration of the subcomponent in seconds."
    )
    max_duration_seconds: float = Field(
        ..., description="The maximum duration of the subcomponent in seconds."
    )
    average_duration_seconds: float = Field(
        ..., description="The average duration of the subcomponent in seconds."
    )


class ReportTimingAggregatedTask(BaseModel):
    """Aggregated timing details for a mapped task."""

    count: int = Field(0, description="The number of subcomponents.")
    min_duration_seconds: float = Field(
        0.0, description="The minimum duration of the subcomponent in seconds."
    )
    max_duration_seconds: float = Field(
        ..., description="The maximum duration of the subcomponent in seconds."
    )
    average_duration_seconds: float = Field(
        ..., description="The average duration of the subcomponent in seconds."
    )
    details: Dict[str, Any] | None = Field(None, description="The details of the subcomponent.")


class ReportTiming(BaseModel):
    """Timing metadata for the SDG workflow."""

    start_time: str | None = Field(None, description="The start time of the SDG Workflow.")
    end_time: str | None = Field(None, description="The end time of the SDG Workflow.")
    duration_seconds: float | None = Field(
        None, description="The duration of the SDG Workflow in seconds."
    )
    tasks: Dict[str, ReportTimingTask] = Field({}, description="The tasks for the SDG Workflow.")
    mapped_tasks: Dict[str, ReportTimingAggregatedTask] = Field(
        {}, description="The mapped tasks for the SDG Workflow."
    )


class Report(BaseModel):
    """Video Data Augmentation workflow report."""

    output: ReportOutput | None = Field(
        None, description="The output metadata for the SDG Workflow."
    )
    auto_labeling_detection_and_tracking: ReportAutoLabelingSubcomponent | None = Field(
        None,
        description="The auto-labeling detection and tracking metadata for the SDG Workflow.",
    )
    auto_labeling_mcq_generation: ReportAutoLabelingSubcomponent | None = Field(
        None, description="The auto-labeling MCQ generation metadata for the SDG Workflow."
    )
    cosmos: ReportCosmos | None = Field(
        None, description="The cosmos metadata for the SDG Workflow."
    )
    timing: ReportTiming | None = Field(
        None, description="The timing metadata for the SDG Workflow."
    )
