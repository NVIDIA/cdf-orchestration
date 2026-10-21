# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Autonomous Vehicle Dataset Generation workflow models."""

from dags.workflows.autonomous_vehicle_dataset_generation_dag.models.payload import (
    AVDatasetGenerationPayload,
)

__all__ = ["AVDatasetGenerationPayload"]
