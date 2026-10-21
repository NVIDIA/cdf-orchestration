# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Worker-backed cleanup of Kubernetes resources created by the k8s plugin.

Triggered by DAG success/failure callbacks with ``conf.target_run_id`` set to
the source run, or triggered manually with that field empty to delete every
resource labeled ``created-by=k8s-plugin``. The task sweeps the in-cluster API
and, if a ``kubernetes_remote`` connection is reachable, the remote cluster.
Unreachable backends are skipped. This DAG must not attach those callbacks, or
it would retrigger itself.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

from dags.shared.utils.dag_utils import K8S_RESOURCE_CLEANUP_DAG_ID


def cleanup_labeled_k8s_resources(**context: Any) -> dict[str, Any]:
    """Delete k8s-plugin Deployments, Services, and Pods.

    When ``target_run_id`` is set, only that source DAG run is cleaned. When it
    is empty, every resource labeled ``created-by=k8s-plugin`` is deleted.
    """
    from k8s_plugin.labels import cleanup_k8s_resources_best_effort

    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    params = context.get("params") or {}
    target_run_id = str(conf.get("target_run_id") or params.get("target_run_id") or "").strip()

    results = cleanup_k8s_resources_best_effort(
        context,
        run_id=target_run_id or None,
        all_plugin_resources=not target_run_id,
    )
    if results.get("skipped"):
        raise AirflowFailException(
            f"Kubernetes cleanup skipped: {results.get('reason', 'unknown')}"
        )
    return results


with DAG(
    dag_id=K8S_RESOURCE_CLEANUP_DAG_ID,
    default_args={"owner": "NVIDIA", "depends_on_past": False, "retries": 2},
    description=(
        "Delete Kubernetes Deployments, Services, and Pods created by the k8s "
        "plugin. Set conf.target_run_id to a source DAG run id to clean that run "
        "only (used by workflow callbacks). Leave target_run_id empty to delete "
        "every resource labeled created-by=k8s-plugin."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=32,
    dagrun_timeout=timedelta(minutes=15),
    tags=["sdg", "kubernetes", "cleanup"],
    params={
        "target_run_id": Param(
            default=None,
            # "null" keeps the trigger form field optional. With a bare "string"
            # type the UI requires a value, so a full plugin sweep cannot be
            # requested.
            type=["string", "null"],
            description=(
                "Source DAG run id to delete. Leave empty to delete all "
                "Kubernetes resources and reset the Kubernetes cluster to a clean state."
            ),
        ),
    },
) as dag:
    PythonOperator(
        task_id="cleanup_labeled_resources",
        python_callable=cleanup_labeled_k8s_resources,
        pool="default_pool",
    )
