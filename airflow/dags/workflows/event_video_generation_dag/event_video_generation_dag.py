# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Airflow DAG for Event Video Generation. Input is a image, which is then converted to a video showing an anomaly."""

import logging
import os
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowConfigException, AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup, chain
from airflow.task.trigger_rule import TriggerRule

from dags.shared.task_groups.captioning import CaptioningTaskGroup
from dags.shared.task_groups.cosmos import CosmosTaskGroup
from dags.shared.task_groups.cosmos_evaluator import (
    ATTRIBUTE_VERIFICATION_CHECKER,
    PHYSICAL_PLAUSIBILITY_CHECKER,
    CosmosEvaluatorTaskGroup,
)
from dags.shared.task_groups.detection_and_tracking import DetectionAndTrackingTaskGroup
from dags.shared.task_groups.event_and_person_attribute_search import (
    EventAndPersonAttributeSearchTaskGroup,
)
from dags.shared.task_groups.input_preparation import InputPreparationTaskGroup
from dags.shared.task_groups.reporting import PerformanceReportingTaskGroup
from dags.shared.task_groups.service_lifecycle import (
    ServiceLifecycleSpec,
    ServiceLifecycleTaskGroup,
)
from dags.shared.task_groups.validate_payload import ValidatePayloadTaskGroup
from dags.shared.task_groups.validated_output import ValidatedOutputTaskGroup
from dags.shared.task_groups.visual_qa import VisualQATaskGroup
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.dag_utils import (
    model_payload_param,
    on_failure_callback,
    on_success_callback,
    resolve_dag_timeout,
)
from dags.workflows.event_video_generation_dag.callables import (
    generate_anomaly_dataset as generate_anomaly_dataset_callable,
)
from dags.workflows.event_video_generation_dag.callables import (
    generate_event_video_generation_cosmos_configs,
    prepare_event_video_generation_captioning_configs,
    prepare_event_video_generation_detection_and_tracking_configs,
    prepare_event_video_generation_image_input,
    prepare_event_video_generation_person_attribute_search_configs,
    prepare_event_video_generation_visual_qa_configs,
    validate_event_video_generation_cosmos_outputs,
    validate_event_video_generation_pipeline_outputs,
)
from dags.workflows.event_video_generation_dag.models import (
    EventVideoGenerationDagPayloadConfig,
)
from dags.workflows.event_video_generation_dag.tasks import (
    generate_event_video_generation_performance_report,
)

logger = logging.getLogger(__name__)

EVENT_VIDEO_GENERATION_CONFIG_DIR = Path(__file__).resolve().parent / "configs"
EVENT_VIDEO_GENERATION_COSMOS_EVALUATOR_CONFIG_PATH = (
    EVENT_VIDEO_GENERATION_CONFIG_DIR / "cosmos_evaluator_config.yaml"
)
EVENT_VIDEO_GENERATION_COSMOS_EVALUATOR_CHECKERS = (
    ATTRIBUTE_VERIFICATION_CHECKER,
    PHYSICAL_PLAUSIBILITY_CHECKER,
)
EVENT_VIDEO_GENERATION_SERVICE_SPECS = (
    ServiceLifecycleSpec("vlm_service", "wait_for_vlm"),
    ServiceLifecycleSpec("image2video_service", "wait_for_image2video"),
)

INPUT_PREPARATION_TASK_ID = "input_preparation.prepare_input"
COSMOS_VALIDATE_OUTPUTS_TASK_ID = "cosmos_augmentation.validate_outputs"
COSMOS_AUGMENTATION_TASK_IDS = [
    "cosmos_augmentation.augmentation_external",
    "cosmos_augmentation.augmentation_internal",
]
VALIDATED_OUTPUT_TASK_ID = "validated_output.validate_outputs"


class EventVideoGenerationDAGBuilder:
    """Builder class for Event Video Generation DAGs."""

    def __init__(
        self,
        manifest_path: str | Path,
        logging_level: str = "INFO",
        use_external_cosmos_evaluator: bool = False,
    ):
        logging_level = os.environ.get("LOGGING_LEVEL", logging_level)
        logger.setLevel(getattr(logging, logging_level.upper(), logging.INFO))
        self.manifest_path = str(manifest_path)
        self.use_external_cosmos_evaluator = use_external_cosmos_evaluator

        try:
            self.builder = ComponentBuilder(manifest_path=self.manifest_path)
        except FileNotFoundError as e:
            raise AirflowConfigException(
                f"Event Video Generation manifest not found at {self.manifest_path}"
            ) from e
        except Exception as e:
            raise AirflowConfigException(
                f"Failed to load Event Video Generation manifest from {self.manifest_path}: {e}"
            ) from e

    @staticmethod
    def _fail_pipeline() -> None:
        """Fail the DagRun when augmentation processing failed."""
        raise AirflowFailException(
            "Event Video Generation pipeline failed — at least one upstream task is in failed state."
        )

    def build_dag(
        self,
        dag_id: str,
        description: str,
        tags: list[str] | None = None,
    ) -> DAG:
        dagrun_timeout = resolve_dag_timeout(self.builder.manifest.deployment.dag_timeout)

        with DAG(
            dag_id=dag_id,
            default_args={
                "owner": "NVIDIA",
                "depends_on_past": False,
            },
            description=description,
            start_date=datetime(2026, 1, 1),
            dagrun_timeout=dagrun_timeout,
            schedule=None,
            catchup=False,
            tags=tags or ["sdg", "event_video_generation"],
            params={
                "payload": model_payload_param(
                    EventVideoGenerationDagPayloadConfig,
                    EventVideoGenerationDagPayloadConfig().model_dump(),
                ),
            },
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            max_active_runs=5,
        ) as dag:
            validate_payload = ValidatePayloadTaskGroup(
                model_class=EventVideoGenerationDagPayloadConfig
            ).get_validate_payload_task(task_id="validate_payload")

            input_preparation = InputPreparationTaskGroup(
                prepare_input_callable=prepare_event_video_generation_image_input,
            ).get_input_preparation_task_group()

            service_lifecycle = ServiceLifecycleTaskGroup(
                builder=self.builder,
                service_lifecycle_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('service_lifecycle', {}) | tojson }}",
                service_specs=EVENT_VIDEO_GENERATION_SERVICE_SPECS,
                shutdown_group_id=None,
            )
            wait_tasks = service_lifecycle.get_wait_for_services_tasks()
            wait_for_vlm = wait_tasks["vlm_service"]
            wait_for_image2video = wait_tasks["image2video_service"]
            startup_tasks = service_lifecycle.get_service_startup_task_group()
            service_lifecycle.get_service_shutdown_task_group()
            shutdown_image2video_id = service_lifecycle.shutdown_xcom_task_id("image2video_service")
            shutdown_image2video = (
                dag.get_task(shutdown_image2video_id)
                if dag.has_task(shutdown_image2video_id)
                else None
            )
            late_shutdown_tasks = [
                dag.get_task(service_lifecycle.shutdown_xcom_task_id(component_key))
                for component_key in ("vlm_service",)
                if dag.has_task(service_lifecycle.shutdown_xcom_task_id(component_key))
            ]
            services_ready = EmptyOperator(task_id="services_ready")

            wait_for_cosmos_evaluator = None
            cosmos_evaluator_shutdown = None
            cosmos_evaluator_failure_tasks = []
            if CosmosEvaluatorTaskGroup.is_supported(
                self.builder,
                checker_names=EVENT_VIDEO_GENERATION_COSMOS_EVALUATOR_CHECKERS,
            ):
                cosmos_evaluator_task_group = CosmosEvaluatorTaskGroup(
                    builder=self.builder,
                    default_config_path=EVENT_VIDEO_GENERATION_COSMOS_EVALUATOR_CONFIG_PATH,
                    checker_names=EVENT_VIDEO_GENERATION_COSMOS_EVALUATOR_CHECKERS,
                )
                wait_for_cosmos_evaluator = cosmos_evaluator_task_group.get_startup_task_group()
                cosmos_evaluator_failure_tasks = [
                    task
                    for task in wait_for_cosmos_evaluator.children.values()
                    if not isinstance(task, EmptyOperator)
                ]
                cosmos_evaluator_shutdown = cosmos_evaluator_task_group.get_shutdown_task_group()

            cosmos_augmentation = CosmosTaskGroup(
                builder=self.builder,
                config_generation_callable=generate_event_video_generation_cosmos_configs,
                output_validation_callable=validate_event_video_generation_cosmos_outputs,
                task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('cosmos', {}) }}",
                external_augmentation_pool="external_image2video_service_pool",
                config_generation_op_kwargs={
                    "default_config_path": str(
                        EVENT_VIDEO_GENERATION_COSMOS_EVALUATOR_CONFIG_PATH
                        if (
                            wait_for_cosmos_evaluator is not None
                            or self.use_external_cosmos_evaluator
                        )
                        else EVENT_VIDEO_GENERATION_CONFIG_DIR / "cosmos_config.yaml"
                    )
                },
            ).get_cosmos_augmentation_task_group()

            annotation_payload = (
                "{{ ti.xcom_pull(task_ids='validate_payload', key='return_value') | tojson }}"
            )
            with TaskGroup(group_id="auto_labeling") as auto_labeling_group:
                detection_and_tracking = DetectionAndTrackingTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config=annotation_payload,
                    group_id="detection_and_tracking",
                    output_group_id="auto_labeling",
                    prepare_args_callable=prepare_event_video_generation_detection_and_tracking_configs,
                ).get_detection_and_tracking_task_group()

                captioning = CaptioningTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config=annotation_payload,
                    group_id="captioning",
                    output_group_id="auto_labeling",
                    prepare_args_callable=prepare_event_video_generation_captioning_configs,
                ).get_captioning_task_group()

                anomaly_visual_qa = VisualQATaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config=annotation_payload,
                    group_id="anomaly_visual_qa",
                    component_name="visual_qa",
                    output_group_id="auto_labeling",
                    prepare_args_callable=prepare_event_video_generation_visual_qa_configs,
                    prepare_args_op_kwargs={"visual_qa_profile": "anomaly"},
                ).get_visual_qa_task_group()

                person_attribute_visual_qa = VisualQATaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config=annotation_payload,
                    group_id="person_attribute_visual_qa",
                    component_name="visual_qa",
                    output_group_id="auto_labeling",
                    prepare_args_callable=prepare_event_video_generation_visual_qa_configs,
                    prepare_args_op_kwargs={"visual_qa_profile": "person_attribute"},
                ).get_visual_qa_task_group()

                person_attribute_search = EventAndPersonAttributeSearchTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config=annotation_payload,
                    group_id="person_attribute_search",
                    component_name="event_and_person_attribute_search",
                    output_group_id="auto_labeling",
                    prepare_args_callable=prepare_event_video_generation_person_attribute_search_configs,
                ).get_event_and_person_attribute_search_task_group()

                chain(
                    detection_and_tracking,
                    captioning,
                    anomaly_visual_qa,
                    person_attribute_visual_qa,
                    person_attribute_search,
                )

            generate_anomaly_dataset = PythonOperator(
                task_id="generate_anomaly_dataset",
                python_callable=generate_anomaly_dataset_callable,
                op_kwargs={
                    "payload": "{{ ti.xcom_pull(task_ids='validate_payload', key='return_value') }}",
                    "run_id": "{{ run_id }}",
                },
            )

            validated_output = ValidatedOutputTaskGroup(
                validation_callable=validate_event_video_generation_pipeline_outputs,
            ).get_validated_output_task_group()

            performance_reporting = PerformanceReportingTaskGroup(
                report_generation_callable=generate_event_video_generation_performance_report,
                report_op_kwargs={
                    "completion_task_id": VALIDATED_OUTPUT_TASK_ID,
                    "workload_task_ids": {
                        "input": INPUT_PREPARATION_TASK_ID,
                        "successful_augmentations": COSMOS_VALIDATE_OUTPUTS_TASK_ID,
                        "final_dataset": generate_anomaly_dataset.task_id,
                    },
                    "augmentation_stage_task_ids": COSMOS_AUGMENTATION_TASK_IDS,
                },
            ).get_performance_reporting_task_group()

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

            validate_payload >> input_preparation
            input_preparation >> startup_tasks
            validate_payload >> [wait_for_vlm, wait_for_image2video]
            if wait_for_cosmos_evaluator is not None:
                input_preparation >> wait_for_cosmos_evaluator
            for task in [
                input_preparation,
                wait_for_vlm,
                wait_for_image2video,
            ]:
                task >> services_ready
            if wait_for_cosmos_evaluator is not None:
                wait_for_cosmos_evaluator >> services_ready
            services_ready >> cosmos_augmentation
            # Tear down image2video after Cosmos so autolabeling can reuse its GPUs.
            # Keep cleanup tasks independent so VLM cleanup is not pulled forward.
            # OSMO manifests omit internal endpoints, so this edge is optional.
            if shutdown_image2video is not None:
                cosmos_augmentation >> shutdown_image2video
                shutdown_image2video >> auto_labeling_group
            chain(
                cosmos_augmentation,
                auto_labeling_group,
                generate_anomaly_dataset,
                validated_output,
            )
            validated_output >> performance_reporting
            dag.get_task("performance_reporting.generate_report") >> [
                fail_pipeline,
                pipeline_success,
            ]
            for task in cosmos_evaluator_failure_tasks:
                task >> fail_pipeline
            if cosmos_evaluator_shutdown is not None:
                cosmos_augmentation >> cosmos_evaluator_shutdown
                validated_output >> cosmos_evaluator_shutdown
                fail_pipeline >> cosmos_evaluator_shutdown
                # The failure gate can finish before a delayed evaluator deploy.
                list(wait_for_cosmos_evaluator.children.values()) >> cosmos_evaluator_shutdown

            gate_upstreams = [
                validate_payload,
                input_preparation,
                wait_for_vlm,
                wait_for_image2video,
                detection_and_tracking,
                captioning,
                anomaly_visual_qa,
                person_attribute_visual_qa,
                person_attribute_search,
                generate_anomaly_dataset,
                validated_output,
            ]
            for task in gate_upstreams:
                task >> [fail_pipeline, pipeline_success]
                if late_shutdown_tasks:
                    task >> late_shutdown_tasks

        return dag


def _create_event_video_generation_dag_if_manifest_exists(
    manifest_path: str | Path,
    platform: str,
    description: str,
    tags: list[str],
) -> DAG | None:
    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        logger.info(
            "Event Video Generation %s manifest does not exist at %s, skipping DAG generation",
            platform,
            manifest_path,
        )
        return None

    try:
        return EventVideoGenerationDAGBuilder(
            manifest_path=manifest_path,
            use_external_cosmos_evaluator=platform == "osmo",
        ).build_dag(
            dag_id=f"event_video_generation_dag_{platform}",
            description=description,
            tags=tags,
        )
    except Exception:
        logger.warning(
            "Failed to load Event Video Generation %s manifest at %s, skipping %s DAG generation",
            platform,
            manifest_path,
            platform,
            exc_info=True,
        )
        return None


# --- DAG Generation ---
# Each manifest produces its own DAG. Users see one DAG per platform in the Airflow UI.

_EVENT_VIDEO_GENERATION_K8S_MANIFEST = os.environ.get(
    "EVENT_VIDEO_GENERATION_K8S_MANIFEST_PATH",
    EVENT_VIDEO_GENERATION_CONFIG_DIR / "event_video_generation_k8s_manifest.yaml",
)

event_video_generation_dag_k8s = _create_event_video_generation_dag_if_manifest_exists(
    manifest_path=_EVENT_VIDEO_GENERATION_K8S_MANIFEST,
    platform="k8s",
    description="Run the Event Video Generation workflow on Native Kubernetes",
    tags=["sdg", "kubernetes", "event_video_generation"],
)

_EVENT_VIDEO_GENERATION_OSMO_MANIFEST = os.environ.get(
    "EVENT_VIDEO_GENERATION_OSMO_MANIFEST_PATH",
    EVENT_VIDEO_GENERATION_CONFIG_DIR / "event_video_generation_osmo_manifest.yaml",
)
event_video_generation_dag_osmo = _create_event_video_generation_dag_if_manifest_exists(
    manifest_path=_EVENT_VIDEO_GENERATION_OSMO_MANIFEST,
    platform="osmo",
    description="Run the Event Video Generation workflow on OSMO",
    tags=["sdg", "osmo", "event_video_generation"],
)
