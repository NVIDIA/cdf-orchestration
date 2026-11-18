# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""aoi_anomalygen_dag: the DIG DAG's AnomalyGen step in its own DAG (defect mode).

validate_payload -> generate_configs -> per defect configuration:
anomaly_package -> anomaly_generation (pod, >= 2 GPUs) -> validate_anomalygen_outputs
-> pipeline_success, with DIG's fail_pipeline (ONE_FAILED) deciding the run outcome.

The tasks, callables, manifest entry and payload are taken from defect_image_generation_smt_dag
(pipeline_mode: defect). Outputs per configuration under <output_directory>/<run_id>/defects/<id>/:
anomaly_generation/ (defect images, export_manifest.json, masks/, logs/, stage_timing.json, kpi.json
when evaluate is on) and artifact/ (the images plus artifact/manifest.json with requested and
delivered counts). A retry resubmits one configuration with a count and a new base_seed.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowConfigException, AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.empty import EmptyOperator
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
from dags.workflows.aoi_anomalygen_dag.callables.anomalygen import (
    package_anomalygen_inputs,
    select_anomaly_generation_kwargs,
    select_defect_validation_kwargs,
    validate_anomalygen_outputs,
)
from dags.workflows.aoi_anomalygen_dag.callables.config_generation import generate_configs
from dags.workflows.aoi_anomalygen_dag.models.payload import AoiAnomalyGenPayload, default_payload

logger = logging.getLogger(__name__)
DAG_ID = "aoi_anomalygen_dag"
MANIFEST_ENV = "AOI_ANOMALYGEN_K8S_MANIFEST_PATH"
CONFIG_DIR = Path(__file__).parent / "configs"
ANOMALY_GENERATION_TASK_ID = "anomaly_generation"
DEFECT_VALIDATION_TASK_ID = "validate_anomalygen_outputs"


class AoiAnomalyGenDAGBuilder:
    def __init__(self, manifest_path: str):
        try:
            self.builder = ComponentBuilder(manifest_path=manifest_path)
        except Exception as exc:
            raise AirflowConfigException(f"Cannot load AOI AnomalyGen manifest: {manifest_path}") from exc

    def build_dag(self, dag_id: str = DAG_ID) -> DAG:
        with DAG(
            dag_id=dag_id,
            default_args={"owner": "NVIDIA", "depends_on_past": False},
            description="AnomalyGen only (DIG defect mode): clean PCB images -> defect images and masks",
            start_date=datetime(2026, 1, 1),
            schedule=None,
            catchup=False,
            dagrun_timeout=resolve_dag_timeout(self.builder.manifest.deployment.dag_timeout),
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            max_active_runs=5,
            tags=["sdg", "kubernetes", "defect-image-generation", "aoi", "anomalygen"],
            params={"payload": model_payload_param(AoiAnomalyGenPayload, default_payload())},
        ) as dag:
            validate_payload = ValidatePayloadTaskGroup(
                model_class=AoiAnomalyGenPayload
            ).get_validate_payload_task(task_id="validate_payload")
            configs = PythonOperator(
                task_id="generate_configs",
                python_callable=generate_configs,
                retries=0,
                op_kwargs={
                    "payload": "{{ ti.xcom_pull(task_ids='validate_payload', key='return_value') | tojson }}",
                    "run_id": "{{ run_id }}",
                },
            )
            package = PythonOperator.partial(
                task_id="anomaly_package",
                python_callable=package_anomalygen_inputs,
                retries=0,
            ).expand(op_kwargs=XComArg(configs))
            generation = self.builder.partial_task(
                "anomaly_generation", task_id=ANOMALY_GENERATION_TASK_ID
            ).expand_kwargs(XComArg(package).map(select_anomaly_generation_kwargs))
            # ALL_DONE: a failed pod must not stop the other configurations' validation. A failed
            # configuration has no export_manifest.json, so its own validation fails too (F1).
            outputs = PythonOperator.partial(
                task_id=DEFECT_VALIDATION_TASK_ID,
                python_callable=validate_anomalygen_outputs,
                trigger_rule=TriggerRule.ALL_DONE,
                retries=0,
            ).expand(op_kwargs=XComArg(package).map(select_defect_validation_kwargs))

            # Run outcome, as in DIG: a run succeeds only when every task did. Without this watcher
            # the ALL_DONE validation would be the only leaf, and a failed pod whose outputs still
            # validate (or an anomaly_package that failed for every configuration) would end green.
            pipeline_success = EmptyOperator(task_id="pipeline_success")
            fail_pipeline = PythonOperator(
                task_id="fail_pipeline",
                python_callable=self._fail_pipeline,
                trigger_rule=TriggerRule.ONE_FAILED,
                retries=0,
            )

            validate_payload >> configs >> package >> generation >> outputs >> pipeline_success
            [validate_payload, configs, package, generation, outputs] >> fail_pipeline
        return dag

    @staticmethod
    def _fail_pipeline():
        raise AirflowFailException("AOI AnomalyGen run failed; see the failed task and validate_anomalygen_outputs")


_manifest = os.environ.get(MANIFEST_ENV, str(CONFIG_DIR / "aoi_anomalygen_k8s_manifest.yaml"))
if os.path.isfile(_manifest):
    try:
        aoi_anomalygen_dag = AoiAnomalyGenDAGBuilder(_manifest).build_dag()
    except Exception:
        logger.exception("Failed to build %s from %s", DAG_ID, _manifest)
