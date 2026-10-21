# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Config generation callable for Video Data Augmentation Cosmos augmentation."""

import ast
import copy
import logging
import random
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import CosmosTaskConfig, VariableDistribution
from dags.shared.task_groups.cosmos import load_cosmos_base_config
from dags.shared.task_groups.cosmos_evaluator import (
    ARBITRATOR_COMPONENT,
    COSMOS_EVALUATOR_STARTUP_GROUP_ID,
    apply_checker_overrides,
)
from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.task_groups.service_lifecycle import (
    require_service_base_url_from_xcom,
    require_service_endpoint_from_xcom,
)
from dags.shared.utils.msc_utils import write_file_to_directory

VIDEO_DATA_AUGMENTATION_COSMOS_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "configs" / "cosmos_config.yaml"
)


def _load_yaml_file(path: str, description: str) -> dict[str, Any]:
    return load_cosmos_base_config(
        base_config_path=None,
        default_path=path,
        description=description,
    )


def _normalize_distribution(distribution: dict[str, float]) -> dict[str, float]:
    if not distribution:
        raise AirflowFailException("Variable distributions must not be empty.")
    total = sum(distribution.values())
    if total <= 0 or any(value < 0 for value in distribution.values()):
        raise AirflowFailException(
            "Variable distributions must contain non-negative weights with a positive total."
        )
    if abs(total - 1.0) > 0.01:
        return {key: value / total for key, value in distribution.items()}
    return distribution


def _sample_distribution(distribution: dict[str, float]) -> str:
    values = list(distribution.keys())
    probabilities = list(distribution.values())
    return random.choices(values, weights=probabilities, k=1)[0]


def _sample_variables(
    variable_distribution: VariableDistribution,
    n_samples: int,
) -> list[dict[str, str]]:
    direct_config, lookup_config = variable_distribution.split_variables()
    conditional_variables = variable_distribution.conditional_variables

    if not direct_config and not lookup_config and not conditional_variables:
        return [{} for _ in range(n_samples)]

    random.seed(42)
    samples: list[dict[str, str]] = []
    for _ in range(n_samples):
        sample: dict[str, str] = {}

        for var_name, dist in direct_config.items():
            base_value = _sample_distribution(_normalize_distribution(dist))
            resolved_value = base_value
            for lookup in lookup_config.values():
                if base_value in lookup:
                    resolved_value = _sample_distribution(
                        _normalize_distribution(lookup[base_value])
                    )
                    break
            sample[var_name] = resolved_value

        for var_name, conditional in conditional_variables.items():
            parent = conditional.depends_on
            parent_value = sample[parent]
            dist = _normalize_distribution(conditional.distributions[parent_value].as_dict())
            sample[var_name] = _sample_distribution(dist)

        samples.append(sample)
    return samples


def _log_realized_distributions(samples: list[dict[str, str]]) -> None:
    realized_distributions = defaultdict(list)
    for sample in samples:
        for var_name, var_val in sample.items():
            realized_distributions[var_name].append(var_val)

    realized_counts = {var: dict(Counter(values)) for var, values in realized_distributions.items()}
    for var, counts in realized_counts.items():
        total = sum(counts.values())
        frequencies = {val: count / total for val, count in counts.items()}
        logging.info(
            "Realized distribution of variable '%s': counts=%s, frequencies=%s",
            var,
            counts,
            frequencies,
        )


def _single_value_variables(sampled_vars: dict[str, str]) -> dict[str, list[str]]:
    return {key: [value] for key, value in sampled_vars.items()}


def _set_sampled_variables(config: dict[str, Any], sampled_vars: dict[str, str]) -> None:
    """Set sampled variables in either the legacy or current Cosmos config schema."""
    variables = _single_value_variables(sampled_vars)
    set_variable_locations = []

    template_generation = config.get("template_generation")
    if isinstance(template_generation, dict):
        template_generation["variables"] = variables
        set_variable_locations.append("template_generation.variables")

    captioning = config.get("captioning")
    if isinstance(captioning, dict):
        captioning_llm = captioning.get("llm")
        if isinstance(captioning_llm, dict):
            captioning_llm["variables"] = variables
            captioning_llm["verification_values"] = variables
            set_variable_locations.append("captioning.llm.variables")
        elif "variables" in captioning:
            captioning["variables"] = variables
            set_variable_locations.append("captioning.variables")

    if set_variable_locations:
        logging.debug("Set sampled Cosmos variables at %s", ", ".join(set_variable_locations))
        return

    if not variables:
        return

    raise AirflowFailException(
        "Cosmos base config has sampled variable distributions in the payload, but no supported "
        "variable section was found. Expected one of: template_generation, captioning.llm, "
        "or captioning.variables."
    )


def _set_data_entry(
    config: dict[str, Any],
    input_video_path: str,
    output_base: str,
) -> None:
    data_template: dict[str, Any] = {}
    if isinstance(config.get("data"), list) and config["data"]:
        first_entry = config["data"][0]
        if isinstance(first_entry, dict):
            data_template = copy.deepcopy(first_entry)

    inputs = data_template.setdefault("inputs", {})
    if not isinstance(inputs, dict):
        inputs = {}
        data_template["inputs"] = inputs
    inputs["rgb"] = input_video_path

    output = data_template.setdefault("output", {})
    if not isinstance(output, dict):
        output = {}
        data_template["output"] = output
    output.update(
        {
            "video": f"{output_base}/output.mp4",
            "caption": f"{output_base}/caption.txt",
            "metadata": f"{output_base}/metadata.json",
        }
    )

    config["data"] = [data_template]


def _enabled_cosmos_evaluator(config: dict[str, Any]) -> dict[str, Any] | None:
    """Return the enabled external evaluator entry, if the selected config has one."""
    evaluators = config.get("evaluators")
    if not isinstance(evaluators, list):
        return None

    for entry in evaluators:
        if not isinstance(entry, dict):
            continue
        cosmos_evaluator = entry.get("cosmos_evaluator")
        if isinstance(cosmos_evaluator, dict) and cosmos_evaluator.get("enabled") is True:
            return cosmos_evaluator
    return None


def _enable_external_cosmos_evaluator(config: dict[str, Any]) -> None:
    """Enable the configured evaluator when a caller provides its external URL."""
    evaluators = config.get("evaluators")
    cosmos_evaluator = (
        next(
            (
                entry["cosmos_evaluator"]
                for entry in evaluators
                if isinstance(entry, dict) and isinstance(entry.get("cosmos_evaluator"), dict)
            ),
            None,
        )
        if isinstance(evaluators, list)
        else None
    )
    if cosmos_evaluator is None:
        raise AirflowFailException(
            "cosmos_evaluator_service_url was provided, but the Cosmos base config has no "
            "cosmos_evaluator configuration."
        )

    endpoint_id = cosmos_evaluator.get("endpoint_id")
    if not isinstance(endpoint_id, str) or not endpoint_id.strip():
        raise AirflowFailException(
            "cosmos_evaluator_service_url was provided, but the Cosmos base config "
            "cosmos_evaluator must define a non-empty string endpoint_id."
        )

    endpoints = config.get("endpoints")
    if not isinstance(endpoints, list) or not any(
        isinstance(endpoint, dict)
        and isinstance(endpoint.get("id"), str)
        and endpoint["id"] == endpoint_id
        for endpoint in endpoints
    ):
        raise AirflowFailException(
            "cosmos_evaluator_service_url was provided, but the Cosmos base config has no "
            "matching evaluator endpoint."
        )
    cosmos_evaluator["enabled"] = True


def _requires_cosmos_evaluator_service_url(config: dict[str, Any]) -> bool:
    """Return whether an enabled evaluator references the Airflow URL placeholder."""
    cosmos_evaluator = _enabled_cosmos_evaluator(config)
    if cosmos_evaluator is None:
        return False
    endpoint_id = cosmos_evaluator.get("endpoint_id")
    endpoints = config.get("endpoints")
    if not isinstance(endpoints, list):
        return False
    return any(
        isinstance(endpoint, dict)
        and endpoint.get("id") == endpoint_id
        and endpoint.get("url") == "<>"
        for endpoint in endpoints
    )


def _video_create_url(base_url: str) -> str:
    """Build the async Videos API create route from a service base URL.

    The ``openai.video.async`` adapter posts to its configured URL verbatim, so
    the route belongs here. Payload and XCom URLs arrive with or without a
    trailing ``/v1``, so normalize before appending.
    """
    normalized = base_url.rstrip("/")
    if normalized.endswith("/v1"):
        normalized = normalized[: -len("/v1")]
    return f"{normalized}/v1/videos"


def _set_endpoints(
    config: dict[str, Any],
    cosmos_task_config: CosmosTaskConfig,
    cosmos_evaluator_service_url: str | None = None,
) -> None:
    """Overwrite endpoint fields from the payload CosmosTaskConfig.

    Expects the list-based endpoints schema used by cosmos_config.yaml
    (entries with ``id``, ``url``, and ``model``).
    """
    _ENDPOINT_SERVICE_FIELDS: dict[str, tuple[str, str | None]] = {
        "vlm": ("vlm_service_url", "vlm_model"),
        "llm": ("llm_service_url", "llm_model"),
        "cosmos_transfer": ("cosmos_transfer_service_url", None),
    }
    endpoints = config.get("endpoints")
    if not isinstance(endpoints, list):
        return
    cosmos_evaluator = _enabled_cosmos_evaluator(config)
    cosmos_evaluator_endpoint_id = (
        cosmos_evaluator.get("endpoint_id") if cosmos_evaluator is not None else None
    )

    for endpoint in endpoints:
        if not isinstance(endpoint, dict):
            continue
        endpoint_id = endpoint.get("id")
        if endpoint_id == cosmos_evaluator_endpoint_id and cosmos_evaluator_service_url is not None:
            endpoint["url"] = cosmos_evaluator_service_url
            if cosmos_task_config.cosmos_evaluator_service_url is not None:
                endpoint["api_key_env"] = "COSMOS_API_KEY"
            else:
                endpoint.pop("api_key_env", None)
            continue
        service_fields = _ENDPOINT_SERVICE_FIELDS.get(endpoint_id)
        if service_fields is None:
            continue
        url_field, model_field = service_fields
        url = getattr(cosmos_task_config, url_field)
        if endpoint_id == "cosmos_transfer" and url:
            url = _video_create_url(url)
        endpoint["url"] = url
        if model_field is not None:
            endpoint["model"] = getattr(cosmos_task_config, model_field)


def _set_cosmos_evaluator_runtime(
    config: dict[str, Any],
    output_base: str,
    sampled_vars: dict[str, str],
) -> None:
    """Fill per-candidate fields already present in an enabled Cosmos evaluator."""
    cosmos_evaluator = _enabled_cosmos_evaluator(config)
    if cosmos_evaluator is None:
        return

    data = config.get("data")
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return
    output = data[0].get("output")
    if not isinstance(output, dict):
        return

    output_prefix = URL(output_base).path.strip("/")
    if not output_prefix:
        raise AirflowFailException("Enabled Cosmos evaluator requires a candidate output prefix.")

    if "prefix" in output:
        output["prefix"] = output_prefix

    checker_config = cosmos_evaluator.get("config")
    if isinstance(checker_config, dict):
        selected_variables = checker_config.get("selected_variables")
        if isinstance(selected_variables, dict):
            selected_variables.clear()
            selected_variables.update(sampled_vars)


def generate_cosmos_configs(
    cosmos_config: str | None = None,
    run_id: str = "",
    share_vlm_for_llm: bool = False,
    **context,
) -> list[str]:
    """Generate Cosmos template configs and return their storage paths."""
    try:
        cosmos_task_config = CosmosTaskConfig.model_validate(ast.literal_eval(cosmos_config))
    except Exception as e:
        raise AirflowFailException(f"Payload validation failed: {e}") from e

    if not cosmos_task_config.external_services:
        cosmos_task_config.vlm_service_url = require_service_endpoint_from_xcom(
            context["ti"], "vlm_service"
        )
        if share_vlm_for_llm:
            cosmos_task_config.llm_service_url = cosmos_task_config.vlm_service_url
            cosmos_task_config.llm_model = cosmos_task_config.vlm_model
        else:
            cosmos_task_config.llm_service_url = require_service_endpoint_from_xcom(
                context["ti"], "llm_service"
            )
        cosmos_task_config.cosmos_transfer_service_url = require_service_endpoint_from_xcom(
            context["ti"], "cosmos_transfer_service"
        )

    prepared = require_prepared_input_from_xcom(context["ti"])
    if cosmos_task_config.base_config_path:
        base_config = load_cosmos_base_config(
            base_config_path=cosmos_task_config.base_config_path,
            default_path=VIDEO_DATA_AUGMENTATION_COSMOS_CONFIG_PATH,
            description="cosmos base configuration file",
        )
    else:
        base_config = _load_yaml_file(
            str(VIDEO_DATA_AUGMENTATION_COSMOS_CONFIG_PATH),
            "cosmos base configuration file",
        )

    cosmos_evaluator_service_url = cosmos_task_config.cosmos_evaluator_service_url
    if cosmos_evaluator_service_url is not None:
        cosmos_evaluator_service_url = cosmos_evaluator_service_url.strip()
        if not cosmos_evaluator_service_url:
            raise AirflowFailException("cosmos_evaluator_service_url must not be empty.")
        _enable_external_cosmos_evaluator(base_config)
    apply_checker_overrides(base_config, cosmos_task_config.checker_overrides)
    if cosmos_evaluator_service_url is None and _requires_cosmos_evaluator_service_url(base_config):
        cosmos_evaluator_service_url = require_service_base_url_from_xcom(
            context["ti"],
            ARBITRATOR_COMPONENT,
            task_group_id=COSMOS_EVALUATOR_STARTUP_GROUP_ID,
        )

    total_configs = prepared.length * cosmos_task_config.num_augmentation
    samples = _sample_variables(cosmos_task_config.variable_distribution, total_configs)
    _log_realized_distributions(samples)

    output_base_path = str(URL(cosmos_task_config.output_directory) / run_id / "cosmos")
    config_paths = []
    sample_idx = 0
    for prepared_video in prepared.videos:
        for aug_idx in range(cosmos_task_config.num_augmentation):
            sampled_vars = samples[sample_idx]
            sample_idx += 1
            output_base = f"{output_base_path}/{prepared_video.video_key}/{aug_idx}"
            generated_config = copy.deepcopy(base_config)
            _set_sampled_variables(generated_config, sampled_vars)
            _set_data_entry(
                generated_config,
                input_video_path=prepared_video.video_path,
                output_base=output_base,
            )
            _set_endpoints(
                generated_config,
                cosmos_task_config,
                cosmos_evaluator_service_url=cosmos_evaluator_service_url,
            )
            _set_cosmos_evaluator_runtime(
                generated_config,
                output_base=output_base,
                sampled_vars=sampled_vars,
            )

            file_path = f"{output_base}/config.yaml"
            write_file_to_directory(
                file_path, yaml.safe_dump(generated_config, sort_keys=False).encode("utf-8")
            )
            config_paths.append(file_path)

    return config_paths
