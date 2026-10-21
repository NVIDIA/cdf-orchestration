# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared DAG utilities for callbacks and timeout resolution."""

from datetime import timedelta
from typing import Any

import requests
from airflow.exceptions import AirflowConfigException
from airflow.sdk import Param
from pydantic import BaseModel

from dags.shared.utils.logging import setup_logger

logger = setup_logger(__name__)

DEFAULT_DAG_TIMEOUT_SECONDS = 12 * 60 * 60
K8S_RESOURCE_CLEANUP_DAG_ID = "k8s_resource_cleanup_dag"


def model_payload_param(
    model_type: type[BaseModel],
    default: dict[str, Any],
    *,
    description: str | None = None,
) -> Param:
    """Build a serializable Airflow Param from a Pydantic model schema.

    Airflow 3.3.2 passes ``Param.description`` separately while expanding the
    JSON schema during deserialization. A Pydantic model docstring produces a
    top-level ``description`` in that schema, which would otherwise supply the
    argument twice and prevent the DAG from being persisted. The docstring is
    dropped; pass ``description`` to set the Airflow param help text.
    """
    schema = model_type.model_json_schema()
    schema.pop("description", None)
    return Param(default=default, description=description, schema=schema)


def trigger_k8s_resource_cleanup_dag(context) -> None:
    """Enqueue the worker-backed K8s cleanup DAG for this DAG run.

    DAG callbacks run in the scheduler, which does not have the tbot kubeconfig.
    The cleanup DAG's PythonOperator runs on a KubernetesExecutor worker that does.
    """
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    source_dag_id = getattr(dag, "dag_id", None)
    if source_dag_id == K8S_RESOURCE_CLEANUP_DAG_ID:
        return

    source_run_id = getattr(dag_run, "run_id", None)
    if not source_run_id:
        logger.warning("Skipping K8s cleanup DAG trigger: missing run_id")
        return

    try:
        from airflow.api.common.trigger_dag import trigger_dag
        from airflow.exceptions import DagNotFound, DagRunAlreadyExists
        from airflow.utils.types import DagRunTriggeredByType
    except ImportError as e:
        logger.warning("Cannot trigger K8s cleanup DAG: %s", e)
        return

    cleanup_run_id = f"cleanup__{source_dag_id or 'unknown'}__{source_run_id}"[:250]
    try:
        trigger_dag(
            dag_id=K8S_RESOURCE_CLEANUP_DAG_ID,
            triggered_by=DagRunTriggeredByType.OPERATOR,
            run_id=cleanup_run_id,
            conf={
                "target_run_id": source_run_id,
                "source_dag_id": source_dag_id,
            },
            replace_microseconds=False,
        )
        logger.info(
            "Triggered %s run_id=%s for target_run_id=%s",
            K8S_RESOURCE_CLEANUP_DAG_ID,
            cleanup_run_id,
            source_run_id,
        )
    except DagRunAlreadyExists:
        logger.info("K8s cleanup DAG run already exists: %s", cleanup_run_id)
    except DagNotFound:
        logger.warning("K8s cleanup DAG %s not found", K8S_RESOURCE_CLEANUP_DAG_ID)
    except Exception as e:
        logger.warning("Failed to trigger K8s cleanup DAG: %s", e)


def resolve_dag_timeout(dag_timeout_seconds: int | None) -> timedelta:
    """Parse manifest dag timeout in seconds."""
    if dag_timeout_seconds is None:
        return timedelta(seconds=DEFAULT_DAG_TIMEOUT_SECONDS)

    if not isinstance(dag_timeout_seconds, int) or isinstance(dag_timeout_seconds, bool):
        raise AirflowConfigException(
            "Invalid deployment.dag_timeout in manifest. Expected an integer number of seconds."
        )
    if dag_timeout_seconds <= 0:
        raise AirflowConfigException(
            "Invalid deployment.dag_timeout in manifest. Value must be greater than zero."
        )

    return timedelta(seconds=dag_timeout_seconds)


def on_success_callback(context):
    """
    Callback executed when a DAG run completes successfully.
    Reads the callback URL from the DAG run configuration.
    """
    trigger_k8s_resource_cleanup_dag(context)
    dag_run = context.get("dag_run")
    dag = context.get("dag")

    # Get callback URL from DAG run configuration
    callback_url = None
    if dag_run and hasattr(dag_run, "conf") and dag_run.conf:
        callback_url = dag_run.conf.get("callback_success_url")

    if not callback_url:
        logger.warning("callback_success_url not found in DAG run configuration")
        return

    payload = {
        "dag_id": dag.dag_id if dag else "unknown",
        "run_id": dag_run.run_id,
        "logical_date": (str(dag_run.logical_date) if dag_run.logical_date else None),
        "state": "success",
    }

    try:
        response = requests.post(
            callback_url,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        logger.info("SUCCESS callback sent to %s", callback_url)
    except Exception as e:
        logger.error("Failed to send SUCCESS callback to %s: %s", callback_url, str(e))


def on_failure_callback(context):
    """
    Callback executed when a DAG run fails.
    Reads the callback URL from the DAG run configuration.
    """
    trigger_k8s_resource_cleanup_dag(context)
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    exception = context.get("exception")

    # Get callback URL from DAG run configuration
    callback_url = None
    if dag_run and hasattr(dag_run, "conf") and dag_run.conf:
        callback_url = dag_run.conf.get("callback_failure_url")

    if not callback_url:
        logger.warning("callback_failure_url not found in DAG run configuration")
        return

    payload = {
        "dag_id": dag.dag_id if dag else "unknown",
        "run_id": dag_run.run_id,
        "logical_date": (str(dag_run.logical_date) if dag_run.logical_date else None),
        "state": "failed",
        "error_message": str(exception) if exception else "Unknown error",
    }

    try:
        response = requests.post(
            callback_url,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        logger.info("FAILURE callback sent to %s", callback_url)
    except Exception as e:
        logger.error("Failed to send FAILURE callback to %s: %s", callback_url, str(e))
