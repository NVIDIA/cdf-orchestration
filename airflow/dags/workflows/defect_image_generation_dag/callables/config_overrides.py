# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Merge opaque DIG component overrides into workflow config templates."""

from __future__ import annotations

import copy
from typing import Any

from pydantic import BaseModel


def _deep_merge_config(
    base: dict[str, Any],
    override: dict[str, Any],
) -> dict[str, Any]:
    """Recursively merge dictionaries while replacing lists and scalar values."""
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _deep_merge_config(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def apply_component_overrides(
    template: dict[str, Any],
    *,
    defaults: dict[str, Any],
    configuration: BaseModel,
    component_names: tuple[str, ...],
) -> dict[str, Any]:
    """Apply collection defaults, then variant overrides, to a component template."""
    merged = copy.deepcopy(template)
    configuration_values = configuration.model_extra or {}
    for source in (defaults, configuration_values):
        for component_name in component_names:
            component_override = source.get(component_name)
            if component_override:
                merged = _deep_merge_config(merged, component_override)
    return merged
