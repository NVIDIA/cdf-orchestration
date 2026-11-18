# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""aoi_anomalygen_dag: AnomalyGen only (Day 1, defect mode) over a set of clean PCB images.

validate_payload -> plan_chunks -> anomaly_generation[chunk] (one AnomalyGen pod per chunk of ~10
images, mapped; 1 GPU with the default cloud VLM GPT-6 Astra, or anomaly_generation_local[chunk] with
2 GPUs when the conf asks for a local Gemma) -> collect_results (always runs; fails the run if any
chunk failed).

Everything that changes per run comes from the conf ``payload`` (see models.py). A rerun of failed
images is a new round with ``images`` (and optionally ``image_defects``) set to
results/run_report.json's ``images_to_rerun``.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowConfigException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule

from dags.shared.task_groups import ValidatePayloadTaskGroup
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.dag_utils import (
    model_payload_param,
    on_failure_callback,
    on_success_callback,
    resolve_dag_timeout,
)
from dags.workflows.aoi_anomalygen_dag.callables import (
    collect_results,
    plan_chunks,
    select_chunks,
)
from dags.workflows.aoi_anomalygen_dag.models import AoiAnomalyGenPayload, default_payload

logger = logging.getLogger(__name__)
DAG_ID = "aoi_anomalygen_dag"
MANIFEST_ENV = "AOI_ANOMALYGEN_K8S_MANIFEST_PATH"
DEFAULT_MANIFEST = Path(__file__).parent / "configs" / "aoi_anomalygen_k8s_manifest.yaml"


def build_dag(manifest_path: str) -> DAG:
    try:
        builder = ComponentBuilder(manifest_path=manifest_path)
    except Exception as exc:
        raise AirflowConfigException(f"Cannot load AOI AnomalyGen manifest: {manifest_path}") from exc

    with DAG(
        dag_id=DAG_ID,
        default_args={"owner": "NVIDIA", "depends_on_past": False},
        description="AnomalyGen only: clean PCB images -> defect images, masks, labels, kpi.json",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        dagrun_timeout=resolve_dag_timeout(builder.manifest.deployment.dag_timeout),
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        max_active_runs=2,
        tags=["sdg", "kubernetes", "defect-image-generation", "aoi", "anomalygen"],
        params={"payload": model_payload_param(AoiAnomalyGenPayload, default_payload())},
    ) as dag:
        validate_payload = ValidatePayloadTaskGroup(
            model_class=AoiAnomalyGenPayload
        ).get_validate_payload_task(task_id="validate_payload")
        plan = PythonOperator(
            task_id="plan_chunks",
            python_callable=plan_chunks,
            retries=0,
            op_kwargs={
                "payload": "{{ ti.xcom_pull(task_ids='validate_payload', key='return_value') | tojson }}",
                "run_id": "{{ run_id }}",
            },
        )
        selected = {
            vlm: PythonOperator(
                task_id=f"select_{vlm}_chunks",
                python_callable=select_chunks,
                op_kwargs={"vlm": vlm},
                retries=0,
            )
            for vlm in ("cloud", "local")
        }
        # Cloud VLM chunks (default): 1 GPU, 1 pool slot each, so chunks run in parallel.
        generation = builder.partial_task(
            "aoi_anomaly_generation", task_id="anomaly_generation"
        ).expand_kwargs(XComArg(selected["cloud"]))
        # Local Gemma chunks (anomaly_generation.amg_serve set): 2 GPUs, 2 pool slots each.
        generation_local = builder.partial_task(
            "aoi_anomaly_generation_local", task_id="anomaly_generation_local"
        ).expand_kwargs(XComArg(selected["local"]))
        collect = PythonOperator(
            task_id="collect_results",
            python_callable=collect_results,
            trigger_rule=TriggerRule.ALL_DONE,
            retries=0,
        )
        validate_payload >> plan >> list(selected.values())
        selected["cloud"] >> generation >> collect
        selected["local"] >> generation_local >> collect
    return dag


_manifest = os.environ.get(MANIFEST_ENV, str(DEFAULT_MANIFEST))
if os.path.isfile(_manifest):
    try:
        aoi_anomalygen_dag = build_dag(_manifest)
    except Exception:
        logger.exception("Failed to build %s from %s", DAG_ID, _manifest)
