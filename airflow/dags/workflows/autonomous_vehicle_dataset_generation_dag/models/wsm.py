# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Precomputed WSM inputs associated with retrieved RGB spans."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class WSMControl(BaseModel):
    """One camera's control video, using the source RGB video's time coordinates."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    span_uuid: str | None = Field(default=None, min_length=1)
    source_video: str | None = Field(default=None, min_length=1)
    camera: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    start_ns: int = Field(ge=0)
    end_ns: int = Field(gt=0)
    uri: str = Field(min_length=1)
    num_frames: int = Field(gt=0)
    fps: int = Field(gt=0, le=120)
    size: str = Field(pattern=r"^[1-9][0-9]*x[1-9][0-9]*$")
    sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def _identity_and_window(self) -> "WSMControl":
        if bool(self.span_uuid) == bool(self.source_video):
            raise ValueError("Identify a WSM entry by exactly one of span_uuid or source_video")
        if self.end_ns <= self.start_ns:
            raise ValueError("WSM end_ns must be after start_ns")
        duration_frames = (self.end_ns - self.start_ns) / 1e9 * self.fps
        if abs(duration_frames - self.num_frames) > 1.01:
            raise ValueError("WSM frame count does not cover its declared time range")
        return self


class WSMManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    kind: Literal["cdf-av-wsm"]
    controls: list[WSMControl] = Field(min_length=1)
