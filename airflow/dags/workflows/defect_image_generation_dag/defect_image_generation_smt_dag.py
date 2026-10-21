# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""SMT DIG: simulate a component image, augment it, then generate a defect directly."""

import logging
import os
from datetime import datetime

from airflow.exceptions import AirflowConfigException, AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule

from dags.shared.task_groups import CosmosTaskGroup, ValidatePayloadTaskGroup
from dags.shared.task_groups.reporting import PerformanceReportingTaskGroup
from dags.shared.task_groups.service_lifecycle import (
    ServiceLifecycleSpec,
    ServiceLifecycleTaskGroup,
)
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.dag_utils import (
    model_payload_param,
    on_failure_callback,
    on_success_callback,
    resolve_dag_timeout,
)
from dags.workflows.defect_image_generation_dag.callables.smt.anomalygen import (
    package_anomalygen_inputs,
    select_anomaly_generation_kwargs,
    select_defect_validation_kwargs,
    validate_anomalygen_outputs,
)
from dags.workflows.defect_image_generation_dag.callables.smt.augmentation import (
    generate_smt_augmentation_configs,
    validate_smt_augmentation_outputs,
)
from dags.workflows.defect_image_generation_dag.callables.smt.config_generation import (
    CONFIG_DIR,
    default_payload,
    generate_smt_configs,
    select_generated_config_list,
)
from dags.workflows.defect_image_generation_dag.callables.smt.models import (
    SmtDefectImageGenerationPayload,
)
from dags.workflows.defect_image_generation_dag.callables.smt.performance_reporting import (
    generate_smt_performance_report,
)

logger = logging.getLogger(__name__)
MANIFEST_ENV = "DEFECT_IMAGE_GENERATION_SMT_K8S_MANIFEST_PATH"

# Group-qualified IDs of the tasks measured by performance reporting.
SIMULATION_TASK_ID = "image_generation"
AUGMENTATION_VALIDATION_TASK_ID = "augmentation.validate_outputs"
ANOMALY_GENERATION_TASK_ID = "anomaly_generation"
DEFECT_VALIDATION_TASK_ID = "validate_anomalygen_outputs"
AUGMENTATION_TASK_IDS = [
    "augmentation.augmentation_external",
    "augmentation.augmentation_internal",
]


class SmtDefectImageGenerationDAGBuilder:
    def __init__(self, manifest_path: str | None = None):
        manifest_path = manifest_path or os.environ.get(
            MANIFEST_ENV, str(CONFIG_DIR / "defect_image_generation_k8s_manifest.yaml")
        )
        try:
            self.builder = ComponentBuilder(manifest_path=manifest_path)
        except Exception as exc:
            raise AirflowConfigException(
                f"Cannot load SMT deployment manifest: {manifest_path}"
            ) from exc

    @staticmethod
    def _select_input_route(pipeline_mode: str) -> str:
        return "anomaly_package" if pipeline_mode == "defect" else "image_generation"

    @staticmethod
    def _continue_after_simulation(pipeline_mode: str) -> bool:
        return pipeline_mode != "simulation"

    @staticmethod
    def _select_checkpoint_route(**context) -> str:
        payload = context["ti"].xcom_pull(
            task_ids="validate_payload", key="return_value"
        )
        return (
            "join_after_checkpoint"
            if payload.get("external_services", False)
            else "image_edit_checkpoint_download"
        )

    @staticmethod
    def _continue_after_augmentation(pipeline_mode: str) -> bool:
        return pipeline_mode == "full"

    @staticmethod
    def _fail_pipeline():
        raise AirflowFailException("SMT defect image generation failed")

    def build_dag(self, dag_id: str = "defect_image_generation_smt_dag_k8s") -> DAG:
        with DAG(
            dag_id=dag_id,
            default_args={"owner": "NVIDIA", "depends_on_past": False},
            description="Generate SMT component images, augment them, and add defects",
            start_date=datetime(2026, 1, 1),
            schedule=None,
            catchup=False,
            dagrun_timeout=resolve_dag_timeout(
                self.builder.manifest.deployment.dag_timeout
            ),
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            max_active_runs=5,
            tags=["sdg", "kubernetes", "defect-image-generation", "smt"],
            params={
                "payload": model_payload_param(
                    SmtDefectImageGenerationPayload,
                    default_payload().model_dump(),
                )
            },
        ) as dag:
            validate_payload = ValidatePayloadTaskGroup(
                model_class=SmtDefectImageGenerationPayload
            ).get_validate_payload_task(task_id="validate_payload")
            payload = "{{ ti.xcom_pull(task_ids='validate_payload', key='return_value') | tojson }}"
            generate_configs = PythonOperator(
                task_id="generate_configs",
                python_callable=generate_smt_configs,
                do_xcom_push=False,
                op_kwargs={"payload": payload, "run_id": "{{ run_id }}"},
            )
            generated = {
                key: PythonOperator(
                    task_id=f"prepare_{key}",
                    python_callable=select_generated_config_list,
                    op_kwargs={"config_key": key},
                )
                for key in ("image_generation_environments", "anomaly_package_kwargs")
            }
            simulation = self.builder.partial_task(
                "image_generation", task_id=SIMULATION_TASK_ID
            ).expand(
                runtime_environment_variables=XComArg(
                    generated["image_generation_environments"]
                )
            )
            mode = "{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('pipeline_mode', 'full') }}"
            select_input_route = BranchPythonOperator(
                task_id="select_input_route",
                python_callable=self._select_input_route,
                op_kwargs={"pipeline_mode": mode},
            )
            continue_after_simulation = ShortCircuitOperator(
                task_id="continue_after_simulation",
                python_callable=self._continue_after_simulation,
                op_kwargs={"pipeline_mode": mode},
                # Allow the terminal reporting branch to observe simulation-only runs.
                ignore_downstream_trigger_rules=False,
            )
            select_checkpoint_route = BranchPythonOperator(
                task_id="select_checkpoint_route",
                python_callable=self._select_checkpoint_route,
            )
            checkpoint = self.builder.build_task(
                "image_edit_checkpoint_download",
                task_id="image_edit_checkpoint_download",
            )
            join_after_checkpoint = EmptyOperator(
                task_id="join_after_checkpoint",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )
            lifecycle = ServiceLifecycleTaskGroup(
                builder=self.builder,
                service_lifecycle_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('service_lifecycle', {}) | tojson }}",
                service_specs=(
                    ServiceLifecycleSpec("image_edit_service", "wait_for_image_edit"),
                ),
            )
            wait = lifecycle.get_wait_for_services_tasks()["image_edit_service"]
            startup = lifecycle.get_service_startup_task_group()
            shutdown = lifecycle.get_service_shutdown_task_group()
            augmentation = CosmosTaskGroup(
                builder=self.builder,
                config_generation_callable=generate_smt_augmentation_configs,
                config_generation_op_kwargs={
                    "image_edit_model": self.builder.manifest.deployment.components.endpoints[
                        "image_edit_service"
                    ].environment["IMAGE_EDIT_MODEL"]
                },
                output_validation_callable=validate_smt_augmentation_outputs,
                task_config=payload,
                external_augmentation_pool="external_image_edit_service_pool",
                group_id="augmentation",
            ).get_cosmos_augmentation_task_group()
            # Keep the augmentation tail skipped when defect mode bypasses both workers.
            dag.get_task(
                "augmentation.join_after_augmentation"
            ).trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            continue_after_augmentation = ShortCircuitOperator(
                task_id="continue_after_augmentation",
                python_callable=self._continue_after_augmentation,
                op_kwargs={"pipeline_mode": mode},
                # Skip defect tasks normally, allowing the failure watcher to observe cleanup.
                ignore_downstream_trigger_rules=False,
            )
            package = PythonOperator.partial(
                task_id="anomaly_package",
                python_callable=package_anomalygen_inputs,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            ).expand(op_kwargs=XComArg(generated["anomaly_package_kwargs"]))
            generation = self.builder.partial_task(
                "anomaly_generation", task_id=ANOMALY_GENERATION_TASK_ID
            ).expand_kwargs(XComArg(package).map(select_anomaly_generation_kwargs))
            outputs = PythonOperator.partial(
                task_id=DEFECT_VALIDATION_TASK_ID,
                python_callable=validate_anomalygen_outputs,
            ).expand(op_kwargs=XComArg(package).map(select_defect_validation_kwargs))
            performance_reporting = PerformanceReportingTaskGroup(
                report_generation_callable=generate_smt_performance_report,
                report_op_kwargs={
                    "completion_task_ids": {
                        "simulation": SIMULATION_TASK_ID,
                        "sim2real": AUGMENTATION_VALIDATION_TASK_ID,
                        "full": DEFECT_VALIDATION_TASK_ID,
                        "defect": DEFECT_VALIDATION_TASK_ID,
                    },
                    "workload_task_ids": {
                        "configs": generate_configs.task_id,
                        "golden_images": AUGMENTATION_VALIDATION_TASK_ID,
                        "defect_images": DEFECT_VALIDATION_TASK_ID,
                    },
                    "stage_task_ids": {
                        "simulation": [SIMULATION_TASK_ID],
                        "augmentation": AUGMENTATION_TASK_IDS,
                        "anomaly_generation": [ANOMALY_GENERATION_TASK_ID],
                    },
                },
            ).get_performance_reporting_task_group()
            pipeline_success = EmptyOperator(task_id="pipeline_success")
            fail_pipeline = PythonOperator(
                task_id="fail_pipeline",
                python_callable=self._fail_pipeline,
                trigger_rule=TriggerRule.ONE_FAILED,
                retries=0,
            )

            validate_payload >> generate_configs >> list(generated.values())
            generate_configs >> select_input_route >> [simulation, package]
            simulation >> continue_after_simulation >> select_checkpoint_route
            select_checkpoint_route >> [checkpoint, join_after_checkpoint]
            checkpoint >> join_after_checkpoint >> [startup, wait]
            [continue_after_simulation, wait] >> augmentation
            augmentation >> [shutdown, continue_after_augmentation]
            (
                continue_after_augmentation
                >> package
                >> generation
                >> outputs
                >> pipeline_success
            )
            [
                simulation,
                dag.get_task(AUGMENTATION_VALIDATION_TASK_ID),
                outputs,
            ] >> performance_reporting
            # Image-edit service GPUs are released before AnomalyGen starts.
            shutdown >> generation
            [
                validate_payload,
                generate_configs,
                select_input_route,
                simulation,
                select_checkpoint_route,
                checkpoint,
                *lifecycle.startup_deploy_tasks.values(),
                *lifecycle.shutdown_tasks.values(),
                dag.get_task("augmentation.generate_cosmos_configs"),
                *[dag.get_task(task_id) for task_id in AUGMENTATION_TASK_IDS],
                dag.get_task(AUGMENTATION_VALIDATION_TASK_ID),
                package,
                generation,
                outputs,
            ] >> fail_pipeline
        return dag


_manifest = os.environ.get(
    MANIFEST_ENV, str(CONFIG_DIR / "defect_image_generation_k8s_manifest.yaml")
)
if os.path.isfile(_manifest):
    try:
        defect_image_generation_smt_dag_k8s = SmtDefectImageGenerationDAGBuilder(
            _manifest
        ).build_dag()
    except Exception:
        logger.exception("Failed to build the SMT DIG DAG from %s", _manifest)
