# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Curate AV Dataset Generation videos and retrieve similar clips with Cosmos Dataset Search.

Cosmos3-Nano generation defaults to RGB transfer from retrieved
clips, with WSM control available via control_mode="wsm"
as optional. Both modes require physical-plausibility validation. Each run
deploys and cleans up its services, validates outputs, and reports
performance while preserving task failures through cleanup.
"""

from __future__ import annotations

import base64
import logging
import os
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowConfigException, AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule

from dags.shared.models import PHYSICAL_PLAUSIBILITY_CHECKER
from dags.shared.task_groups import ValidatePayloadTaskGroup
from dags.shared.task_groups.input_preparation import InputPreparationTaskGroup
from dags.shared.task_groups.service_lifecycle import (
    ServiceLifecycleSpec,
    ServiceLifecycleTaskGroup,
)
from dags.shared.task_groups.validated_output import ValidatedOutputTaskGroup
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.dag_utils import (
    model_payload_param,
    on_failure_callback,
    on_success_callback,
    resolve_dag_timeout,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables import (
    create_avdg_configs,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.augmentation import (
    FINALIZE_OUTPUTS_TASK_ID,
    WSM_PROBE_TASK_ID,
    finalize_augmentation_outputs,
    generate_augmentation_configs,
    select_augmentation_tasks,
    validate_augmentation_outputs,
    validate_combined_outputs,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.augmentation import (
    prepare_wsm_probe as prepare_wsm_probe_callable,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.collect_results import (
    collect_autonomous_vehicle_dataset_generation_results,
    validate_avdg_pipeline_outputs,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.config_generation import (
    build_curator_args as build_curator_args_callable,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.curator_summary import (
    prepare_curator_probe as prepare_curator_probe_callable,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.curator_summary import (
    reconcile_curator_outputs as reconcile_curator_outputs_callable,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.evaluator_lifecycle import (
    AVEvaluatorTaskGroup,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.callables.input_preparation import (
    prepare_avdg_input,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.models import (
    AVDatasetGenerationPayload,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.models.payload import (
    Cosmos3NanoTaskConfig,
)
from dags.workflows.autonomous_vehicle_dataset_generation_dag.tasks.cosmos import AVCosmosTaskGroup

logger = logging.getLogger(__name__)

AUTONOMOUS_VEHICLE_DATASET_GENERATION_CONFIG_DIR = Path(__file__).resolve().parent / "configs"
AUTONOMOUS_VEHICLE_DATASET_GENERATION_SERVICE_SPECS = (
    ServiceLifecycleSpec("milvus_service", "wait_for_milvus_service"),
    ServiceLifecycleSpec("cds_service", "wait_for_cds_service"),
    ServiceLifecycleSpec("cosmos3_service", "wait_for_cosmos3"),
)


_PAYLOAD = "(ti.xcom_pull(task_ids='validate_payload', key='return_value') or {})"
_CONFIGS = "(ti.xcom_pull(task_ids='generate_configs', key='return_value') or {})"
_SERVICE_ENDPOINT = (
    "(ti.xcom_pull(task_ids='service_startup.startup_{}', key='endpoint') or {{}}).get('url', '')"
)
_CDS_URL = _SERVICE_ENDPOINT.format("cds_service")
_COSMOS_TASK_CONFIG = "{{ ti.xcom_pull(task_ids='validate_cosmos_payload') }}"
_OUTPUT_PROBE = "(ti.xcom_pull(task_ids='cosmos_augmentation.validate_outputs') or {})"


def _media_probe_environment(
    request_uri: str, result_uri: str, script_name: str = "media_probe.py"
) -> list[str]:
    return [
        f"MSC_CREDENTIALS_SCRIPT_B64:{{{{ {_CONFIGS}.get('msc_credentials_script_b64', '') }}}}",
        f"MEDIA_PROBE_SCRIPT_B64:{{{{ {_CONFIGS}.get('media_probe_script_b64', '') }}}}",
        f"CONTROL_VIDEO_PROBE_SCRIPT_B64:{{{{ {_CONFIGS}.get('control_video_probe_script_b64', '') }}}}",
        f"PROBE_SCRIPT:{script_name}",
        f"PROBE_REQUEST_URI:{request_uri}",
        f"PROBE_RESULT_URI:{result_uri}",
        f"OBJECT_STORE_ENDPOINT:{{{{ {_CONFIGS}.get('object_store_endpoint', '') }}}}",
        f"PIXI_ENV:{{{{ {_CONFIGS}.get('pixi_env', 'default') }}}}",
    ]


def _default_manifest_path() -> str:
    return os.environ.get(
        "AUTONOMOUS_VEHICLE_DATASET_GENERATION_K8S_MANIFEST_PATH",
        str(
            AUTONOMOUS_VEHICLE_DATASET_GENERATION_CONFIG_DIR
            / "autonomous_vehicle_dataset_generation_k8s_manifest.yaml"
        ),
    )


class AutonomousVehicleDatasetGenerationDAGBuilder:
    """Build the Kubernetes retrieval DAG with optional RGB or WSM Cosmos3-Nano generation."""

    def __init__(self, manifest_path: str | Path | None = None, logging_level: str = "INFO"):
        logging_level = os.environ.get("LOGGING_LEVEL", logging_level)
        logger.setLevel(getattr(logging, logging_level.upper(), logging.INFO))
        if manifest_path is None:
            manifest_path = _default_manifest_path()
        self.manifest_path = str(manifest_path)

        try:
            self.builder = ComponentBuilder(manifest_path=self.manifest_path)
        except FileNotFoundError as exc:
            raise AirflowConfigException(
                f"Autonomous Vehicle Dataset Generation Kubernetes manifest not found at {self.manifest_path}"
            ) from exc
        except Exception as exc:
            raise AirflowConfigException(
                "Failed to load autonomous vehicle dataset generation Kubernetes manifest "
                f"from {self.manifest_path}: {exc}"
            ) from exc

    @staticmethod
    def _fail_pipeline() -> None:
        """Fail the DagRun when any upstream task failed."""
        raise AirflowFailException(
            "Autonomous Vehicle Dataset Generation pipeline failed - at least one upstream "
            "task is in failed state."
        )

    def build_dag(
        self,
        dag_id: str = "autonomous_vehicle_dataset_generation_dag_k8s",
        description: str = (
            "Curate autonomous-vehicle videos, index and retrieve clips with Cosmos "
            "Dataset Search, and optionally generate Cosmos variants on Kubernetes"
        ),
        tags: list[str] | None = None,
    ) -> DAG:
        dagrun_timeout = resolve_dag_timeout(self.builder.manifest.deployment.dag_timeout)

        with DAG(
            dag_id=dag_id,
            default_args={"owner": "NVIDIA", "depends_on_past": False},
            description=description,
            start_date=datetime(2026, 1, 1),
            schedule=None,
            catchup=False,
            dagrun_timeout=dagrun_timeout,
            tags=tags or ["sdg", "kubernetes", "autonomous-vehicle-dataset-generation"],
            params={
                "payload": model_payload_param(
                    AVDatasetGenerationPayload,
                    {},
                ),
            },
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            max_active_runs=5,
        ) as dag:
            validate_payload = ValidatePayloadTaskGroup(
                model_class=AVDatasetGenerationPayload
            ).get_validate_payload_task(task_id="validate_payload")
            validate_cosmos_payload = ValidatePayloadTaskGroup(
                model_class=Cosmos3NanoTaskConfig
            ).get_validate_payload_task(
                task_id="validate_cosmos_payload",
                payload=f"{{{{ {_PAYLOAD}.get('cosmos', {{}}) }}}}",
            )

            # ---------------------------------------------------- Dataset Search
            service_lifecycle = ServiceLifecycleTaskGroup(
                builder=self.builder,
                service_lifecycle_config=(
                    "{{ {'milvus_service': {'enabled': true}, 'cds_service': {'enabled': true}, "
                    f"'cosmos3_service': {{'enabled': {_PAYLOAD}.get('cosmos', {{}}).get('enabled', false)}}"
                    "} | tojson }}"
                ),
                service_specs=AUTONOMOUS_VEHICLE_DATASET_GENERATION_SERVICE_SPECS,
            )
            startup_tasks = service_lifecycle.get_service_startup_task_group()
            shutdown_tasks = service_lifecycle.get_service_shutdown_task_group()
            service_waits = service_lifecycle.get_wait_for_services_tasks()
            wait_for_milvus_service = service_waits["milvus_service"]
            wait_for_cds_service = service_waits["cds_service"]
            wait_for_cosmos3 = service_waits["cosmos3_service"]
            wait_for_cosmos3.timeout_seconds = 5400
            deploy_tasks = service_lifecycle.startup_deploy_tasks
            service_waits["milvus_service"] >> deploy_tasks["cds_service"]
            cds = deploy_tasks["cds_service"]
            for variable, filename in (
                ("CDS_VECTOR_BOOTSTRAP_B64", "cds_vector_pipeline.py"),
                ("CDS_VECTOR_COMPONENTS_B64", "cds_vector_components.py"),
            ):
                script = Path(__file__).parent / "callables" / filename
                encoded = base64.b64encode(script.read_bytes()).decode("ascii")
                cds.container_environment_variables.append(f"{variable}:{encoded}")

            # ----------------------------------------------------------- Curator
            generate_configs = PythonOperator(
                task_id="generate_configs",
                python_callable=create_avdg_configs,
                op_kwargs={
                    "payload": f"{{{{ {_PAYLOAD} }}}}",
                    "run_id": "{{ run_id }}",
                },
            )

            input_preparation = InputPreparationTaskGroup(
                prepare_input_callable=prepare_avdg_input,
            ).get_input_preparation_task_group()

            build_curator_args = PythonOperator(
                task_id="build_curator_args",
                python_callable=build_curator_args_callable,
                op_kwargs={
                    "configs": f"{{{{ {_CONFIGS} }}}}",
                    "prepared_input": "{{ ti.xcom_pull(task_ids='input_preparation.prepare_input') }}",
                },
            )

            download_curator_model = self.builder.build_task(
                "curator_model_download",
                task_id="download_curator_model",
            )

            run_curator = self.builder.build_task(
                "curator",
                task_id="run_curator",
                container_environment_variables=[
                    ("CURATOR_CONFIG_URI:{{ ti.xcom_pull(task_ids='build_curator_args') }}"),
                    (
                        "MSC_CREDENTIALS_SCRIPT_B64:"
                        f"{{{{ {_CONFIGS}.get('msc_credentials_script_b64', '') }}}}"
                    ),
                    f"CURATOR_INPUT_URI:{{{{ {_CONFIGS}.get('input_video_path', '') }}}}",
                    f"CURATOR_OUTPUT_URI:{{{{ {_CONFIGS}.get('curator_output_dir', '') }}}}",
                    f"CURATOR_QUERY_INPUT_URI:{{{{ {_CONFIGS}.get('query_video', '') if {_CONFIGS}.get('query_curator_config_uri') else '' }}}}",
                    f"QUERY_CURATOR_CONFIG_URI:{{{{ {_CONFIGS}.get('query_curator_config_uri', '') }}}}",
                    f"PIXI_ENV:{{{{ {_CONFIGS}.get('pixi_env', 'default') }}}}",
                ],
            )

            prepare_curator_probe = PythonOperator(
                task_id="prepare_curator_probe",
                python_callable=prepare_curator_probe_callable,
                op_kwargs={"configs": f"{{{{ {_CONFIGS} }}}}"},
            )
            probe_curator_outputs = self.builder.build_task(
                "media_probe",
                task_id="probe_curator_outputs",
                container_environment_variables=_media_probe_environment(
                    "{{ ti.xcom_pull(task_ids='prepare_curator_probe')['request_uri'] }}",
                    "{{ ti.xcom_pull(task_ids='prepare_curator_probe')['result_uri'] }}",
                ),
            )
            reconcile_curator_outputs = PythonOperator(
                task_id="reconcile_curator_outputs",
                python_callable=reconcile_curator_outputs_callable,
                op_kwargs={"configs": f"{{{{ {_CONFIGS} }}}}"},
            )

            # ---------------------------------------- Curator and CDS join
            ingest_cds = self.builder.build_task(
                "cds_ingest",
                task_id="ingest_cds",
                container_environment_variables=[
                    (
                        "MSC_CREDENTIALS_SCRIPT_B64:"
                        f"{{{{ {_CONFIGS}.get('msc_credentials_script_b64', '') }}}}"
                    ),
                    f"CDS_INGEST_SCRIPT_B64:{{{{ {_CONFIGS}.get('cds_ingest_script_b64', '') }}}}",
                    f"CURATOR_DIR:{{{{ {_CONFIGS}.get('curator_output_dir', '') }}}}",
                    f"QUERY_CURATOR_DIR:{{{{ {_CONFIGS}.get('query_curator_output_dir', '') }}}}",
                    f"PARQUET_URI:{{{{ {_CONFIGS}.get('parquet_uri', '') }}}}",
                    f"CDS_URL:{{{{ {_CDS_URL} }}}}",
                    f"COLLECTION_NAME:{{{{ {_CONFIGS}.get('collection_name', '') }}}}",
                    f"PIPELINE:{{{{ {_CONFIGS}.get('pipeline', '') }}}}",
                    f"INGEST_MODE:{{{{ {_CONFIGS}.get('ingest_mode', 'documents') }}}}",
                    f"SEARCH_MODE:{{{{ {_CONFIGS}.get('search_mode', 'embedding') }}}}",
                    f"QUERY_VIDEO:{{{{ {_CONFIGS}.get('query_video', '') }}}}",
                    f"TOP_K:{{{{ {_CONFIGS}.get('top_k', '10') }}}}",
                    f"RESULT_PREFIX:{{{{ {_CONFIGS}.get('run_output_dir', '') }}}}",
                    (
                        "OBJECT_STORE_ENDPOINT:"
                        f"{{{{ {_CONFIGS}.get('object_store_endpoint', '') }}}}"
                    ),
                    (
                        "POLL_TIMEOUT_SECONDS:"
                        f"{{{{ {_CONFIGS}.get('job_timeout_seconds', '900') }}}}"
                    ),
                ],
            )

            collect_results = PythonOperator(
                task_id="collect_results",
                python_callable=collect_autonomous_vehicle_dataset_generation_results,
                op_kwargs={
                    "payload": f"{{{{ {_PAYLOAD} }}}}",
                    "configs": f"{{{{ {_CONFIGS} }}}}",
                    "run_id": "{{ run_id }}",
                },
            )

            validate_retrieval = PythonOperator(
                task_id="validate_retrieval",
                python_callable=validate_avdg_pipeline_outputs,
                op_kwargs={
                    "payload": f"{{{{ {_PAYLOAD} }}}}",
                    "configs": f"{{{{ {_CONFIGS} }}}}",
                    "run_id": "{{ run_id }}",
                },
            )
            cosmos = AVCosmosTaskGroup(
                builder=self.builder,
                config_generation_callable=generate_augmentation_configs,
                output_validation_callable=validate_augmentation_outputs,
                task_config="{{ ti.xcom_pull(task_ids='validate_cosmos_payload') }}",
                validate_payload_task_id="validate_cosmos_payload",
            ).get_cosmos_augmentation_task_group()
            cosmos_tasks = list(cosmos.children.values())

            prepare_wsm_probe = PythonOperator(
                task_id="prepare_wsm_probe",
                python_callable=prepare_wsm_probe_callable,
                op_kwargs={"cosmos_config": _COSMOS_TASK_CONFIG},
            )
            probe_wsm = self.builder.build_task(
                "media_probe",
                task_id=WSM_PROBE_TASK_ID,
                container_environment_variables=_media_probe_environment(
                    "{{ ti.xcom_pull(task_ids='prepare_wsm_probe', key='request_uri') }}",
                    "{{ ti.xcom_pull(task_ids='prepare_wsm_probe', key='result_uri') }}",
                    script_name="control_video_probe.py",
                ),
            )
            probe_outputs = self.builder.build_task(
                "media_probe",
                task_id="probe_augmentation_outputs",
                container_environment_variables=_media_probe_environment(
                    f"{{{{ {_OUTPUT_PROBE}.get('probe_request_uri', '') }}}}",
                    f"{{{{ {_OUTPUT_PROBE}.get('probe_result_uri', '') }}}}",
                ),
            )
            finalize_outputs = PythonOperator(
                task_id=FINALIZE_OUTPUTS_TASK_ID,
                python_callable=finalize_augmentation_outputs,
                op_kwargs={"cosmos_config": _COSMOS_TASK_CONFIG, "run_id": "{{ run_id }}"},
                retries=0,
            )
            evaluator = AVEvaluatorTaskGroup(
                builder=self.builder,
                default_config_path=(
                    AUTONOMOUS_VEHICLE_DATASET_GENERATION_CONFIG_DIR / "cosmos3_nano_config.yaml"
                ),
                checker_names=(PHYSICAL_PLAUSIBILITY_CHECKER,),
            )
            evaluator_startup = evaluator.get_startup_task_group()
            evaluator_shutdown = evaluator.get_shutdown_task_group()
            evaluator_tasks = list(evaluator_startup.children.values())
            choose_augmentation = BranchPythonOperator(
                task_id="choose_augmentation",
                python_callable=select_augmentation_tasks,
                op_kwargs={
                    "task_ids": [
                        task.task_id
                        for task in [prepare_wsm_probe, *cosmos_tasks, *evaluator_tasks]
                    ]
                },
            )
            skip_augmentation = EmptyOperator(task_id="skip_augmentation")
            validated_output = ValidatedOutputTaskGroup(
                validation_callable=validate_combined_outputs,
                op_kwargs={
                    "payload": f"{{{{ {_PAYLOAD} }}}}",
                    "configs": f"{{{{ {_CONFIGS} }}}}",
                    "run_id": "{{ run_id }}",
                },
            ).get_validated_output_task_group()
            dag.get_task(
                "validated_output.validate_outputs"
            ).trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

            fail_pipeline = PythonOperator(
                task_id="fail_pipeline",
                python_callable=self._fail_pipeline,
                trigger_rule=TriggerRule.ONE_FAILED,
                retries=0,
            )
            pipeline_success = EmptyOperator(
                task_id="pipeline_success",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )
            validate_payload >> [
                validate_cosmos_payload,
                input_preparation,
                generate_configs,
                startup_tasks,
                wait_for_milvus_service,
                wait_for_cds_service,
                wait_for_cosmos3,
                download_curator_model,
            ]
            input_preparation >> startup_tasks
            input_preparation >> download_curator_model
            [generate_configs, input_preparation] >> build_curator_args
            [build_curator_args, download_curator_model] >> run_curator
            (
                run_curator
                >> prepare_curator_probe
                >> probe_curator_outputs
                >> reconcile_curator_outputs
            )
            [reconcile_curator_outputs, wait_for_cds_service] >> ingest_cds
            ingest_cds >> dag.get_task("service_shutdown.shutdown_cds_service")
            collect_results >> dag.get_task("service_shutdown.shutdown_milvus_service")
            ingest_cds >> collect_results >> validate_retrieval >> choose_augmentation
            validate_cosmos_payload >> choose_augmentation
            choose_augmentation >> [
                skip_augmentation,
                prepare_wsm_probe,
                *cosmos_tasks,
                *evaluator_tasks,
            ]
            generate_cosmos_configs = dag.get_task("cosmos_augmentation.generate_cosmos_configs")
            prepare_wsm_probe >> probe_wsm >> generate_cosmos_configs
            wait_for_cosmos3 >> generate_cosmos_configs
            dag.get_task("wait_for_cosmos_evaluator.ready") >> dag.get_task(
                "cosmos_augmentation.generate_cosmos_configs"
            )
            [*evaluator_tasks, skip_augmentation, cosmos] >> evaluator_shutdown
            cosmos >> probe_outputs >> finalize_outputs
            [skip_augmentation, finalize_outputs] >> validated_output
            [skip_augmentation, cosmos] >> dag.get_task("service_shutdown.shutdown_cosmos3_service")

            for task in (
                validate_payload,
                validate_cosmos_payload,
                generate_configs,
                input_preparation,
                build_curator_args,
                download_curator_model,
                run_curator,
                prepare_curator_probe,
                probe_curator_outputs,
                reconcile_curator_outputs,
                ingest_cds,
                collect_results,
                validated_output,
                validate_retrieval,
                choose_augmentation,
                prepare_wsm_probe,
                probe_wsm,
                probe_outputs,
                finalize_outputs,
                wait_for_cosmos3,
                wait_for_milvus_service,
                wait_for_cds_service,
                startup_tasks,
                shutdown_tasks,
                *cosmos_tasks,
                *evaluator_tasks,
                *evaluator_shutdown.children.values(),
            ):
                task >> [fail_pipeline, pipeline_success]

        return dag


def _create_autonomous_vehicle_dataset_generation_dag_if_manifest_exists(
    manifest_path: str | Path,
) -> DAG | None:
    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        logger.info(
            "Autonomous Vehicle Dataset Generation Kubernetes manifest does not exist at %s, skipping DAG",
            manifest_path,
        )
        return None
    try:
        return AutonomousVehicleDatasetGenerationDAGBuilder(manifest_path=manifest_path).build_dag()
    except Exception:
        logger.warning(
            "Failed to load autonomous vehicle dataset generation Kubernetes manifest at %s",
            manifest_path,
            exc_info=True,
        )
        return None


_AUTONOMOUS_VEHICLE_DATASET_GENERATION_K8S_MANIFEST = _default_manifest_path()

autonomous_vehicle_dataset_generation_dag_k8s = (
    _create_autonomous_vehicle_dataset_generation_dag_if_manifest_exists(
        _AUTONOMOUS_VEHICLE_DATASET_GENERATION_K8S_MANIFEST
    )
)
