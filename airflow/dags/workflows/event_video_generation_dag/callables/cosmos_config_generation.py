# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generate per-image Cosmos3 augmentation configs for Event Video Generation."""

import ast
import copy
import random
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.task_groups.cosmos import load_cosmos_base_config
from dags.shared.task_groups.cosmos_evaluator import (
    ARBITRATOR_COMPONENT,
    ATTRIBUTE_VERIFICATION_CHECKER,
    COSMOS_EVALUATOR_STARTUP_GROUP_ID,
    PHYSICAL_PLAUSIBILITY_CHECKER,
    apply_checker_overrides,
)
from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.task_groups.service_lifecycle import (
    require_service_base_url_from_xcom,
    require_service_endpoint_from_xcom,
)
from dags.shared.utils.msc_utils import write_file_to_directory
from dags.workflows.event_video_generation_dag.models import EventVideoGenerationCosmosTaskConfig

EVENT_VIDEO_GENERATION_COSMOS_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "configs" / "cosmos_config.yaml"
)
NIM_TO_OPENAI_RESOLUTION = {
    "720_16_9": "1280x720",
}
ANOMALY_VARIABLE = "anomaly_type"
NO_ANOMALY_OPTION = "No anomaly: people are only doing ordinary activity for this setting"


def _parse_config(value: str | dict[str, Any] | None) -> EventVideoGenerationCosmosTaskConfig:
    raw = ast.literal_eval(value) if isinstance(value, str) else value
    return EventVideoGenerationCosmosTaskConfig.model_validate(raw or {})


def _load_base_config(
    base_config_path: str | None = None,
    default_config_path: str | Path = EVENT_VIDEO_GENERATION_COSMOS_CONFIG_PATH,
) -> dict[str, Any]:
    return load_cosmos_base_config(
        base_config_path=base_config_path,
        default_path=default_config_path,
        description="Event Video Generation Cosmos config",
    )


def _enabled_cosmos_evaluator(config: dict[str, Any]) -> dict[str, Any] | None:
    evaluators = config.get("evaluators")
    if not isinstance(evaluators, list):
        return None
    for entry in evaluators:
        if not isinstance(entry, dict):
            continue
        evaluator = entry.get("cosmos_evaluator")
        if isinstance(evaluator, dict) and evaluator.get("enabled") is True:
            return evaluator
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
            "cosmos_evaluator_service_url was provided, but the Event Video Generation "
            "Cosmos config has no cosmos_evaluator configuration."
        )

    endpoint_id = cosmos_evaluator.get("endpoint_id")
    if not isinstance(endpoint_id, str) or not endpoint_id.strip():
        raise AirflowFailException(
            "cosmos_evaluator_service_url was provided, but the Event Video Generation "
            "cosmos_evaluator must define a non-empty string endpoint_id."
        )

    endpoints = config.get("endpoints")
    if not isinstance(endpoints, list) or not any(
        isinstance(endpoint, dict) and endpoint.get("id") == endpoint_id for endpoint in endpoints
    ):
        raise AirflowFailException(
            "cosmos_evaluator_service_url was provided, but the Event Video Generation "
            "Cosmos config has no matching evaluator endpoint."
        )
    cosmos_evaluator["enabled"] = True


def _endpoint_by_role(config: dict[str, Any], role: str) -> dict[str, Any]:
    endpoints = config.get("endpoints")
    if not isinstance(endpoints, list):
        raise AirflowFailException("Event Video Generation Cosmos config endpoints must be a list")
    matches = [
        endpoint
        for endpoint in endpoints
        if isinstance(endpoint, dict) and endpoint.get("role") == role
    ]
    if len(matches) != 1:
        raise AirflowFailException(
            f"Event Video Generation Cosmos config must define exactly one endpoint with role {role!r}"
        )
    return matches[0]


def _sample_variables(
    task_config: EventVideoGenerationCosmosTaskConfig,
    sample_count: int,
) -> list[dict[str, str]]:
    rng = random.Random(42)
    distributions = task_config.variable_distribution.variables
    samples = []
    for _ in range(sample_count):
        samples.append(
            {
                variable: rng.choices(
                    list(distribution.root),
                    weights=list(distribution.root.values()),
                    k=1,
                )[0]
                for variable, distribution in distributions.items()
            }
        )
    return samples


def _set_data(
    config: dict[str, Any],
    *,
    input_path: str,
    output_base: str,
) -> None:
    output = {
        "video": f"{output_base}/output.mp4",
        "caption": f"{output_base}/caption.txt",
        "metadata": f"{output_base}/metadata.json",
    }
    if _enabled_cosmos_evaluator(config) is not None:
        output["prefix"] = URL(output_base).path.strip("/")
    config["data"] = [
        {
            "inputs": {"rgb": input_path},
            "output": output,
        }
    ]


def _set_variables(config: dict[str, Any], variables: dict[str, str]) -> None:
    captioning = config.get("captioning")
    llm = captioning.get("llm") if isinstance(captioning, dict) else None
    if not isinstance(llm, dict):
        raise AirflowFailException("Event Video Generation Cosmos config is missing captioning.llm")
    llm["variables"] = {name: [value] for name, value in variables.items()}

    evaluator = _enabled_cosmos_evaluator(config)
    checker_config = evaluator.get("config") if evaluator is not None else None
    selected_variables = (
        checker_config.get("selected_variables") if isinstance(checker_config, dict) else None
    )
    if isinstance(selected_variables, dict):
        selected_variables.clear()
        selected_variables.update(variables)
        if ANOMALY_VARIABLE in variables:
            _set_anomaly_question(checker_config, variables[ANOMALY_VARIABLE])


def _anomaly_question(value: str) -> dict[str, Any]:
    """Fixed multiple-choice question for the requested anomaly.

    The options are built here rather than by the evaluator's LLM so the verifier can always
    answer that the anomaly is absent or that a different one happened. An LLM-written option
    set can leave both out, and then the requested anomaly wins by elimination.
    """
    label = value.replace("_", " ")
    options = [label, NO_ANOMALY_OPTION, f"A different incident or unusual action (not {label})"]
    # Seeded by the value so the correct letter varies across anomalies but is reproducible.
    random.Random(f"{ANOMALY_VARIABLE}={value}").shuffle(options)
    letters = "ABC"
    return {
        "variable": ANOMALY_VARIABLE,
        "value": value,
        "question": (
            f"Which best describes the {ANOMALY_VARIABLE.replace('_', ' ')} shown in the video, "
            "i.e. what the people are doing?"
        ),
        "options": dict(zip(letters, options, strict=True)),
        "correct_answer": letters[options.index(label)],
    }


def _set_anomaly_question(checker_config: dict[str, Any], value: str) -> None:
    """Verify the anomaly with a fixed question instead of an LLM-generated one."""
    excluded = list(checker_config.get("exclude_variables") or [])
    if ANOMALY_VARIABLE not in excluded:
        excluded.append(ANOMALY_VARIABLE)
    checker_config["exclude_variables"] = excluded
    checker_config["extra_questions"] = [
        question
        for question in checker_config.get("extra_questions") or []
        if not (isinstance(question, dict) and question.get("variable") == ANOMALY_VARIABLE)
    ] + [_anomaly_question(value)]


def _set_endpoints(
    config: dict[str, Any],
    task_config: EventVideoGenerationCosmosTaskConfig,
    cosmos_evaluator_service_url: str | None = None,
    **context,
) -> None:
    vlm = _endpoint_by_role(config, "vlm")
    llm = _endpoint_by_role(config, "llm")
    image2video = _endpoint_by_role(config, "image2video")
    evaluator = _enabled_cosmos_evaluator(config)
    evaluator_endpoint = _endpoint_by_role(config, "evaluator") if evaluator is not None else None

    if task_config.external_services:
        vlm_url = task_config.vlm_service_url
        llm_url = task_config.llm_service_url
        image2video_url = task_config.image2video_service_url
        image2video["api_key_env"] = "COSMOS_API_KEY"
        image2video.pop("model", None)
        llm_model = task_config.llm_model
    else:
        ti = context["ti"]
        shared_endpoint = require_service_endpoint_from_xcom(ti, "vlm_service")
        vlm_url = shared_endpoint
        llm_url = shared_endpoint
        image2video_url = require_service_endpoint_from_xcom(ti, "image2video_service")
        image2video["model"] = task_config.image2video_model
        image2video.pop("api_key_env", None)
        llm_model = task_config.vlm_model

    vlm.update({"url": vlm_url, "model": task_config.vlm_model})
    llm.update({"url": llm_url, "model": llm_model})
    image2video["url"] = image2video_url
    if evaluator_endpoint is not None and cosmos_evaluator_service_url is not None:
        evaluator_endpoint["url"] = cosmos_evaluator_service_url
        if task_config.cosmos_evaluator_service_url is not None:
            evaluator_endpoint["api_key_env"] = "COSMOS_API_KEY"
        else:
            evaluator_endpoint.pop("api_key_env", None)


def _set_augmentation_contract(
    config: dict[str, Any],
    *,
    external_services: bool,
) -> None:
    """Translate NIM parameter names for the internal vLLM-Omni endpoint."""
    if external_services:
        return

    augmentation = config.get("augmentation")
    parameters = augmentation.get("parameters") if isinstance(augmentation, dict) else None
    if not isinstance(parameters, dict):
        raise AirflowFailException(
            "Event Video Generation Cosmos config is missing augmentation.parameters"
        )

    # The checked-in base config may already use the OpenAI-compatible contract.
    if {"size", "num_frames", "num_inference_steps"}.issubset(parameters):
        return

    resolution = parameters.pop("resolution", None)
    try:
        parameters["size"] = NIM_TO_OPENAI_RESOLUTION[resolution]
    except KeyError as e:
        raise AirflowFailException(
            f"Unsupported NIM resolution for internal vLLM-Omni: {resolution!r}"
        ) from e

    if "num_output_frames" not in parameters or "steps" not in parameters:
        raise AirflowFailException(
            "Internal vLLM-Omni requires num_output_frames and steps in the base config"
        )
    parameters["num_frames"] = parameters.pop("num_output_frames")
    parameters["num_inference_steps"] = parameters.pop("steps")
    parameters["extra_params"] = {
        "use_resolution_template": False,
        "use_duration_template": False,
        "guardrails": False,
    }


def generate_event_video_generation_cosmos_configs(
    cosmos_config: str | dict[str, Any] | None = None,
    run_id: str = "",
    default_config_path: str | Path = EVENT_VIDEO_GENERATION_COSMOS_CONFIG_PATH,
    **context,
) -> list[str]:
    """Generate one storage-backed config per input image and augmentation index."""
    try:
        task_config = _parse_config(cosmos_config)
        prepared = require_prepared_input_from_xcom(context["ti"])
        base_config = _load_base_config(task_config.base_config_path, default_config_path)
        cosmos_evaluator_service_url = task_config.cosmos_evaluator_service_url
        if cosmos_evaluator_service_url is not None:
            cosmos_evaluator_service_url = cosmos_evaluator_service_url.strip()
            if not cosmos_evaluator_service_url:
                raise AirflowFailException("cosmos_evaluator_service_url must not be empty.")
            _enable_external_cosmos_evaluator(base_config)
        apply_checker_overrides(
            base_config,
            task_config.checker_overrides,
            checker_names=(
                ATTRIBUTE_VERIFICATION_CHECKER,
                PHYSICAL_PLAUSIBILITY_CHECKER,
            ),
        )
        if (
            cosmos_evaluator_service_url is None
            and _enabled_cosmos_evaluator(base_config) is not None
        ):
            cosmos_evaluator_service_url = require_service_base_url_from_xcom(
                context["ti"],
                ARBITRATOR_COMPONENT,
                task_group_id=COSMOS_EVALUATOR_STARTUP_GROUP_ID,
            )
        output_root = str(URL(task_config.output_directory) / run_id / "cosmos")
        samples = _sample_variables(
            task_config,
            prepared.length * task_config.num_augmentation,
        )

        config_paths: list[str] = []
        sample_index = 0
        for prepared_image in prepared.videos:
            for augmentation_index in range(task_config.num_augmentation):
                generated = copy.deepcopy(base_config)
                output_base = f"{output_root}/{prepared_image.video_key}/{augmentation_index}"
                _set_data(
                    generated,
                    input_path=prepared_image.video_path,
                    output_base=output_base,
                )
                _set_variables(generated, samples[sample_index])
                _set_endpoints(
                    generated,
                    task_config,
                    cosmos_evaluator_service_url=cosmos_evaluator_service_url,
                    **context,
                )
                _set_augmentation_contract(
                    generated,
                    external_services=task_config.external_services,
                )
                sample_index += 1

                config_path = f"{output_base}/config.yaml"
                write_file_to_directory(
                    config_path,
                    yaml.safe_dump(generated, sort_keys=False).encode("utf-8"),
                )
                config_paths.append(config_path)
        return config_paths
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Event Video Generation Cosmos config generation failed: {e}"
        ) from e
