# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared Cosmos Evaluator task-group composition for K8s."""

import json
from collections.abc import Collection
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule

from dags.shared.models import (
    ATTRIBUTE_VERIFICATION_CHECKER,
    HALLUCINATION_CHECKER,
    PHYSICAL_PLAUSIBILITY_CHECKER,
    CosmosEvaluatorCheckerOverrides,
    CosmosTaskConfig,
)
from dags.shared.task_groups.cosmos import load_cosmos_base_config
from dags.shared.task_groups.service_lifecycle import (
    ServiceLifecycleSpec,
    ServiceLifecycleTaskGroup,
    require_service_base_url_from_xcom,
)

COSMOS_EVALUATOR_STARTUP_GROUP_ID = "wait_for_cosmos_evaluator"
COSMOS_EVALUATOR_SHUTDOWN_GROUP_ID = "cosmos_evaluator_shutdown"

POSTGRES_COMPONENT = "cosmos_evaluator_postgres"
DATABASE_COMPONENT = "cosmos_evaluator_database"
HALLUCINATION_COMPONENT = "cosmos_evaluator_hallucination"
ATTRIBUTE_VERIFICATION_COMPONENT = "cosmos_evaluator_attribute_verification"
PHYSICAL_PLAUSIBILITY_COMPONENT = "cosmos_evaluator_physical_plausibility"
ARBITRATOR_COMPONENT = "cosmos_evaluator_arbitrator"

CHECKER_COMPONENTS = {
    HALLUCINATION_CHECKER: HALLUCINATION_COMPONENT,
    ATTRIBUTE_VERIFICATION_CHECKER: ATTRIBUTE_VERIFICATION_COMPONENT,
    PHYSICAL_PLAUSIBILITY_CHECKER: PHYSICAL_PLAUSIBILITY_COMPONENT,
}
CORE_COMPONENTS = {
    POSTGRES_COMPONENT,
    DATABASE_COMPONENT,
    ARBITRATOR_COMPONENT,
}


def _normalize_checker_names(checker_names: Collection[str] | None) -> tuple[str, ...]:
    if checker_names is None:
        return tuple(CHECKER_COMPONENTS)
    if isinstance(checker_names, str):
        raise ValueError("checker_names must be a collection of checker names, not a string")

    normalized = tuple(dict.fromkeys(checker_names))
    if not normalized:
        raise ValueError("checker_names must contain at least one checker")

    unsupported = sorted(set(normalized) - CHECKER_COMPONENTS.keys())
    if unsupported:
        raise ValueError(f"Unsupported Cosmos Evaluator checkers: {unsupported}")
    return normalized


def _enabled_cosmos_evaluator(config: dict[str, Any]) -> dict[str, Any] | None:
    evaluators = config.get("evaluators")
    if not isinstance(evaluators, list):
        return None
    for entry in evaluators:
        if not isinstance(entry, dict):
            continue
        evaluator = entry.get("cosmos_evaluator")
        if isinstance(evaluator, dict) and evaluator.get("enabled", True):
            return evaluator
    return None


def _cosmos_evaluator(config: dict[str, Any]) -> dict[str, Any] | None:
    evaluators = config.get("evaluators")
    if not isinstance(evaluators, list):
        return None
    for entry in evaluators:
        if not isinstance(entry, dict):
            continue
        evaluator = entry.get("cosmos_evaluator")
        if isinstance(evaluator, dict):
            return evaluator
    return None


def apply_checker_overrides(
    config: dict[str, Any],
    checker_overrides: CosmosEvaluatorCheckerOverrides,
    *,
    checker_names: Collection[str] | None = None,
) -> dict[str, Any]:
    """Apply per-run checker switches to a selected Cosmos configuration."""
    if not checker_overrides:
        return config

    configured_checker_names = set(_normalize_checker_names(checker_names))
    unsupported = sorted(set(checker_overrides) - configured_checker_names)
    if unsupported:
        raise AirflowFailException(
            f"Cosmos Evaluator checker overrides are not supported by this DAG: {unsupported}"
        )

    evaluator = _cosmos_evaluator(config)
    if evaluator is None:
        return config

    checks = evaluator.get("checks")
    configured_checks = checks if isinstance(checks, list) else []
    declared_names = {check.get("name") for check in configured_checks if isinstance(check, dict)}
    missing_enabled = sorted(
        name
        for name, enabled in checker_overrides.items()
        if enabled and name not in declared_names
    )
    if missing_enabled:
        raise AirflowFailException(
            "Cosmos Evaluator checker overrides enabled checks absent from the selected "
            f"configuration: {missing_enabled}"
        )

    effective_checks = [
        check
        for check in configured_checks
        if not isinstance(check, dict) or checker_overrides.get(check.get("name"), True)
    ]
    if not effective_checks:
        # Augmentation resolves every OmegaConf interpolation before it validates
        # or filters disabled evaluators.  Keeping a disabled provider here would
        # therefore still require all of its runtime-only interpolations to be
        # populated.  It would also be invalid because the augmentation schema
        # requires every cosmos_evaluator entry to contain at least one check.
        # Remove only this provider entry and preserve any unrelated evaluators.
        evaluators = config.get("evaluators")
        if isinstance(evaluators, list):
            evaluators[:] = [
                entry
                for entry in evaluators
                if not (isinstance(entry, dict) and entry.get("cosmos_evaluator") is evaluator)
            ]
        return config

    evaluator["checks"] = effective_checks
    return config


class CosmosEvaluatorTaskGroup:
    """Build a Cosmos Evaluator K8s lifecycle for a configured checker subset."""

    def __init__(
        self,
        *,
        builder: Any,
        default_config_path: str | Path,
        payload_task_id: str = "validate_payload",
        checker_names: Collection[str] | None = None,
    ) -> None:
        self.builder = builder
        self.default_config_path = str(default_config_path)
        self.payload_task_id = payload_task_id
        self.checker_names = _normalize_checker_names(checker_names)
        checker_components = tuple(CHECKER_COMPONENTS[name] for name in self.checker_names)
        self.service_lifecycle = ServiceLifecycleTaskGroup(
            builder=self.builder,
            service_lifecycle_config=(
                "{{ ti.xcom_pull(task_ids='"
                f"{COSMOS_EVALUATOR_STARTUP_GROUP_ID}.select_services"
                "', key='return_value') | tojson }}"
            ),
            service_specs=(
                ServiceLifecycleSpec(POSTGRES_COMPONENT),
                ServiceLifecycleSpec(DATABASE_COMPONENT),
                *(ServiceLifecycleSpec(component) for component in checker_components),
                ServiceLifecycleSpec(ARBITRATOR_COMPONENT),
            ),
            defer_until_shutdown=False,
            startup_group_id=COSMOS_EVALUATOR_STARTUP_GROUP_ID,
            shutdown_group_id=COSMOS_EVALUATOR_SHUTDOWN_GROUP_ID,
            reserve_pool_slots=False,
        )

    @classmethod
    def is_supported(
        cls,
        builder: Any,
        checker_names: Collection[str] | None = None,
    ) -> bool:
        manifest_components = set(builder.manifest.deployment.components.endpoints)
        configured_checker_names = _normalize_checker_names(checker_names)
        required_components = CORE_COMPONENTS | {
            CHECKER_COMPONENTS[name] for name in configured_checker_names
        }
        return required_components.issubset(manifest_components)

    @staticmethod
    def resolve_service_config(
        *,
        payload_task_id: str,
        default_config_path: str,
        checker_names: Collection[str] | None = None,
        **context: Any,
    ) -> dict[str, dict[str, Any]]:
        """Resolve enabled evaluator services from the chosen Cosmos configuration."""
        ti = context["ti"]
        payload = ti.xcom_pull(task_ids=payload_task_id, key="return_value") or {}
        cosmos_task_config = CosmosTaskConfig.model_validate(payload.get("cosmos", {}))
        configured_checker_names = _normalize_checker_names(checker_names)
        service_config = {
            component: {"enabled": False}
            for component in (
                POSTGRES_COMPONENT,
                DATABASE_COMPONENT,
                *(CHECKER_COMPONENTS[name] for name in configured_checker_names),
                ARBITRATOR_COMPONENT,
            )
        }
        if cosmos_task_config.cosmos_evaluator_service_url is not None:
            ti.xcom_push(key="enabled_checker_names", value=[])
            return service_config

        config = load_cosmos_base_config(
            base_config_path=cosmos_task_config.base_config_path,
            default_path=default_config_path,
            description="cosmos base configuration file",
        )
        apply_checker_overrides(
            config,
            cosmos_task_config.checker_overrides,
            checker_names=configured_checker_names,
        )
        evaluator = _enabled_cosmos_evaluator(config)
        if evaluator is None:
            ti.xcom_push(key="enabled_checker_names", value=[])
            return service_config

        checks = evaluator.get("checks")
        if not isinstance(checks, list) or not checks:
            raise AirflowFailException("Enabled cosmos_evaluator must configure at least one check")

        configured_checker_name_set = set(configured_checker_names)
        enabled_checker_names: list[str] = []
        for check in checks:
            checker_name = check.get("name") if isinstance(check, dict) else None
            if checker_name not in CHECKER_COMPONENTS:
                raise AirflowFailException(
                    f"Unsupported K8s Cosmos Evaluator checker: {checker_name!r}"
                )
            if checker_name not in configured_checker_name_set:
                raise AirflowFailException(
                    f"Cosmos Evaluator checker {checker_name!r} is not configured "
                    "for this lifecycle"
                )
            if checker_name not in enabled_checker_names:
                enabled_checker_names.append(checker_name)

        ti.xcom_push(key="enabled_checker_names", value=enabled_checker_names)
        for component in CORE_COMPONENTS:
            service_config[component]["enabled"] = True
        for name in enabled_checker_names:
            service_config[CHECKER_COMPONENTS[name]]["enabled"] = True
        return service_config

    @staticmethod
    def build_checker_endpoint_map(*, startup_group_id: str, **context: Any) -> str:
        """Build the Arbitrator checker map from the selected ready-service XComs."""
        ti = context["ti"]
        checker_names = ti.xcom_pull(
            task_ids=f"{startup_group_id}.select_services",
            key="enabled_checker_names",
        )
        # An empty selection is valid: the default Cosmos template leaves the
        # evaluator disabled, and an external Arbitrator also deploys nothing.
        # Augmentation still follows this task, so that result must not fail the run.
        if checker_names == []:
            return "{}"
        if not isinstance(checker_names, list):
            raise AirflowFailException("Cosmos Evaluator checker selection XCom is missing")

        endpoint_map = {
            name: require_service_base_url_from_xcom(
                ti,
                CHECKER_COMPONENTS[name],
                task_group_id=startup_group_id,
            )
            for name in checker_names
        }
        return json.dumps(endpoint_map, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def select_checker_task(
        *,
        checker_name: str,
        component_key: str,
        task_group_id: str,
        stage: str,
        **context: Any,
    ) -> str:
        """Choose the task for one checker after the shared lifecycle decision."""
        selected = context["ti"].xcom_pull(
            task_ids=f"{COSMOS_EVALUATOR_STARTUP_GROUP_ID}.select_services",
            key="enabled_checker_names",
        )
        if selected is None and stage == "shutdown":
            # Startup can fail before it writes the selection. No checker could
            # have been deployed in that case, but the cleanup path must finish.
            selected = []
        if not isinstance(selected, list):
            raise AirflowFailException("Cosmos Evaluator checker selection XCom is missing")
        task_name = (
            f"{stage}_{component_key}"
            if checker_name in selected
            else f"skip_{stage}_{component_key}"
        )
        return f"{task_group_id}.{task_name}"

    def get_startup_task_group(self) -> TaskGroup:
        task_group = self.service_lifecycle.get_service_startup_task_group()
        deploy_tasks = self.service_lifecycle.startup_deploy_tasks
        select_services = PythonOperator(
            task_id="select_services",
            python_callable=self.resolve_service_config,
            op_kwargs={
                "payload_task_id": self.payload_task_id,
                "default_config_path": self.default_config_path,
                "checker_names": self.checker_names,
            },
            task_group=task_group,
        )
        checker_tasks = [deploy_tasks[CHECKER_COMPONENTS[name]] for name in self.checker_names]
        build_checker_endpoint_map = PythonOperator(
            task_id="build_checker_endpoint_map",
            python_callable=self.build_checker_endpoint_map,
            op_kwargs={
                "startup_group_id": COSMOS_EVALUATOR_STARTUP_GROUP_ID,
            },
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            task_group=task_group,
        )

        select_services >> self.service_lifecycle.startup_decision_task
        deploy_tasks[POSTGRES_COMPONENT] >> deploy_tasks[DATABASE_COMPONENT]
        for name in self.checker_names:
            component = CHECKER_COMPONENTS[name]
            select_checker = BranchPythonOperator(
                task_id=f"select_startup_{component}",
                python_callable=self.select_checker_task,
                op_kwargs={
                    "checker_name": name,
                    "component_key": component,
                    "task_group_id": COSMOS_EVALUATOR_STARTUP_GROUP_ID,
                    "stage": "startup",
                },
                task_group=task_group,
            )
            skip_checker = EmptyOperator(task_id=f"skip_startup_{component}", task_group=task_group)
            (
                deploy_tasks[DATABASE_COMPONENT]
                >> select_checker
                >> [
                    deploy_tasks[component],
                    skip_checker,
                ]
            )
            skip_checker >> build_checker_endpoint_map
        checker_tasks >> build_checker_endpoint_map
        build_checker_endpoint_map >> deploy_tasks[ARBITRATOR_COMPONENT]
        ready = EmptyOperator(
            task_id="ready",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            task_group=task_group,
        )
        [deploy_tasks[ARBITRATOR_COMPONENT], self.service_lifecycle.startup_skip_task] >> ready
        return task_group

    def get_shutdown_task_group(self) -> TaskGroup:
        task_group = self.service_lifecycle.get_service_shutdown_task_group()
        cleanup_tasks = self.service_lifecycle.shutdown_tasks
        arbitrator = cleanup_tasks[ARBITRATOR_COMPONENT]
        database = cleanup_tasks[DATABASE_COMPONENT]
        postgres = cleanup_tasks[POSTGRES_COMPONENT]

        for name in self.checker_names:
            component = CHECKER_COMPONENTS[name]
            select_checker = BranchPythonOperator(
                task_id=f"select_shutdown_{component}",
                python_callable=self.select_checker_task,
                op_kwargs={
                    "checker_name": name,
                    "component_key": component,
                    "task_group_id": COSMOS_EVALUATOR_SHUTDOWN_GROUP_ID,
                    "stage": "shutdown",
                },
                trigger_rule=TriggerRule.ALL_DONE,
                task_group=task_group,
            )
            skip_checker = EmptyOperator(
                task_id=f"skip_shutdown_{component}",
                trigger_rule=TriggerRule.ALL_DONE,
                task_group=task_group,
            )
            arbitrator >> select_checker >> [cleanup_tasks[component], skip_checker]
            cleanup_tasks[component] >> database
            skip_checker >> database
        database >> postgres
        return task_group
