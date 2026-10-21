# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Configure AV physical-plausibility evaluation and validate its results."""

from typing import Any
from urllib.parse import urlsplit

from airflow.exceptions import AirflowFailException

from dags.shared.models import PHYSICAL_PLAUSIBILITY_CHECKER
from dags.shared.task_groups.cosmos_evaluator import (
    ARBITRATOR_COMPONENT,
    CHECKER_COMPONENTS,
    CORE_COMPONENTS,
    COSMOS_EVALUATOR_STARTUP_GROUP_ID,
    CosmosEvaluatorTaskGroup,
)
from dags.shared.task_groups.service_lifecycle import require_service_base_url_from_xcom
from dags.workflows.autonomous_vehicle_dataset_generation_dag.models.payload import (
    Cosmos3NanoTaskConfig,
)


class AVEvaluatorTaskGroup(CosmosEvaluatorTaskGroup):
    """Use the shared evaluator lifecycle when AV generation is enabled."""

    @staticmethod
    def resolve_service_config(
        *,
        payload_task_id: str,
        default_config_path: str,
        checker_names: tuple[str, ...] = (PHYSICAL_PLAUSIBILITY_CHECKER,),
        **context: Any,
    ) -> dict[str, dict[str, Any]]:
        ti = context["ti"]
        payload = ti.xcom_pull(task_ids=payload_task_id, key="return_value") or {}
        config = Cosmos3NanoTaskConfig.model_validate(payload.get("cosmos", {}))
        if not config.enabled:
            ti.xcom_push(key="enabled_checker_names", value=[])
            return {
                component: {"enabled": False}
                for component in (*CORE_COMPONENTS, *(CHECKER_COMPONENTS[n] for n in checker_names))
            }
        return CosmosEvaluatorTaskGroup.resolve_service_config(
            payload_task_id=payload_task_id,
            default_config_path=default_config_path,
            checker_names=checker_names,
            **context,
        )


def configure_physical_plausibility(config: dict, ti: Any) -> None:
    """Update the generation config in-place."""
    endpoint = next(e for e in config["endpoints"] if e["role"] == "evaluator")
    endpoint["url"] = require_service_base_url_from_xcom(
        ti, ARBITRATOR_COMPONENT, task_group_id=COSMOS_EVALUATOR_STARTUP_GROUP_ID
    )
    output = config["data"][0]["output"]
    output["prefix"] = urlsplit(output["video"]).path.rsplit("/", 1)[0].strip("/")


def validate_physical_plausibility_result(metadata: dict, config: dict) -> dict:
    """Read the cosmos_evaluator result from the generated video’s metadata and verifies the result."""
    evaluator = config["evaluators"][0]["cosmos_evaluator"]
    result = metadata.get("cosmos_evaluator")
    if not isinstance(result, dict):
        raise AirflowFailException("Physical Plausibility metadata is missing or invalid")

    if result.get("operational_success") is not True:
        raise AirflowFailException(
            f"Physical Plausibility failed: {result.get('error_code', 'unknown')} — "
            f"{result.get('error', 'no checker diagnostic')} "
            f"(request_id={result.get('request_id', 'unknown')})"
        )
    if not isinstance(result.get("quality_passed"), bool):
        raise AirflowFailException("Physical Plausibility quality verdict is missing or invalid")

    stream = result.get("stream_key")
    results = result.get("checks")
    if not isinstance(stream, str) or not stream or not isinstance(results, dict):
        raise AirflowFailException(
            "Physical Plausibility metadata has no stream or checker results"
        )
    if stream != evaluator.get("stream_key", "output"):
        raise AirflowFailException("Physical Plausibility result belongs to a different stream")

    for check in evaluator["checks"]:
        checker = results.get(check["name"])
        verdict = checker.get(stream) if isinstance(checker, dict) else None
        if not isinstance(verdict, dict) or not isinstance(verdict.get("passed"), bool):
            raise AirflowFailException(
                f"Physical Plausibility check {check['name']!r} has no verdict"
            )
    return result


def physical_plausibility_passed(result: dict) -> bool:
    """Use the checker verdict; aggregate quality_passed ignores non-gating checks."""
    return result["checks"][PHYSICAL_PLAUSIBILITY_CHECKER][result["stream_key"]]["passed"]
