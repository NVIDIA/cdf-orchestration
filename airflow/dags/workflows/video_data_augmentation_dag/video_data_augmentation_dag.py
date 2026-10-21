# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Airflow DAG for the Video Data Augmentation Workflow on NVCF.

The workflow contains the following steps:
    1. Validate payload - Validate the input parameters.
    2. Prepare Input - Resolve input_path as a single file or a directory of videos via MSC.
    3. Super-Resolution (Input) - Process input videos into the input auto-labeling folder.
    4. Detection and Tracking (Input) - Generate object detections and tracks.
    5. Captioning (Input) - Generate captions for input videos.
    6. Visual QA (Input) - Generate question-answer annotations for input videos.
    7. Reasoning (Input) - Generate reasoning labels for input visual QA annotations.
    8. Training Export (Input) - Aggregate input scene DAFT outputs into training datasets.
    9. Cosmos Augmentation - Apply style transfer effects to the synthetic ground truth data.
    10. Super-Resolution (Output) - Process augmented videos into the output auto-labeling folder.
    11. Detection and Tracking (Output) - Generate object detections and tracks.
    12. Captioning (Output) - Generate captions for augmented output videos.
    13. Visual QA (Output) - Generate question-answer annotations for augmented output videos.
    14. Reasoning (Output) - Generate reasoning labels for augmented visual QA annotations.
    15. Training Export (Output) - Aggregate output scene DAFT outputs into training datasets.
    16. Validate outputs - Verify required storage artifacts exist.
    17. Success/failure gates and cleanup.

``input_path`` is either one video object (``is_file``) or a directory prefix listing video files.

Uses ComponentBuilder to construct tasks from manifest YAML configuration,
enabling flexible GPU cluster selection for H100 availability.
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowConfigException, AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup, chain
from airflow.task.trigger_rule import TriggerRule

from dags.shared.models import EndpointXComValue
from dags.shared.task_groups import (
    CaptioningTaskGroup,
    CosmosEvaluatorTaskGroup,
    CosmosTaskGroup,
    DetectionAndTrackingTaskGroup,
    InputPreparationTaskGroup,
    ReasoningTaskGroup,
    ServiceLifecycleTaskGroup,
    SuperResolutionTaskGroup,
    TrainingExportTaskGroup,
    ValidatedOutputTaskGroup,
    ValidatePayloadTaskGroup,
    VisualQATaskGroup,
)
from dags.shared.task_groups.cosmos_evaluator import (
    ATTRIBUTE_VERIFICATION_CHECKER,
    HALLUCINATION_CHECKER,
    PHYSICAL_PLAUSIBILITY_CHECKER,
)
from dags.shared.task_groups.service_lifecycle import ServiceLifecycleSpec
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.dag_utils import (
    model_payload_param,
    on_failure_callback,
    on_success_callback,
    resolve_dag_timeout,
)
from dags.workflows.video_data_augmentation_dag.callables import (
    generate_cosmos_configs,
    prepare_video_data_augmentation_input,
    validate_cosmos_outputs,
    validate_video_data_augmentation_pipeline_outputs,
)
from dags.workflows.video_data_augmentation_dag.models import (
    VideoDataAugmentationDagConfig,
)

VIDEO_DATA_AUGMENTATION_CONFIG_DIR = Path(__file__).resolve().parent / "configs"
VIDEO_DATA_AUGMENTATION_VQA_QUESTION_BANK_PATH = (
    VIDEO_DATA_AUGMENTATION_CONFIG_DIR / "vqa_question_bank.json"
)
VIDEO_DATA_AUGMENTATION_SERVICE_SPECS = (
    ServiceLifecycleSpec("vlm_service", "wait_for_vlm"),
    ServiceLifecycleSpec("llm_service", "wait_for_llm"),
    ServiceLifecycleSpec("cosmos_transfer_service", "wait_for_cosmos_transfer"),
)
SECTIONS_WITH_TOP_LEVEL_OUTPUT_DIRECTORY = (
    "cosmos",
    "super_resolution",
    "detection_and_tracking",
    "captioning",
    "visual_qa",
    "reasoning",
    "training_export",
)
VDA_EXTERNAL_AUGMENTATION_POOL = "cosmos_transfer_3_service_pool"
VDA_K8S_SHARED_LANGUAGE_MODEL = "Qwen/Qwen3.8-27B-FP8"
VDA_COSMOS_EVALUATOR_CHECKERS = (
    HALLUCINATION_CHECKER,
    ATTRIBUTE_VERIFICATION_CHECKER,
    PHYSICAL_PLAUSIBILITY_CHECKER,
)


class VideoDataAugmentationPayloadValidator(ValidatePayloadTaskGroup):
    """Apply platform-specific model defaults after shared payload validation."""

    def __init__(self, shared_language_model: str | None = None):
        super().__init__(model_class=VideoDataAugmentationDagConfig)
        self.shared_language_model = shared_language_model

    def validate_payload(self, payload: str) -> dict:
        validated = super().validate_payload(payload)
        if not self.shared_language_model:
            return validated

        for section_name in ("cosmos", "captioning", "visual_qa"):
            section = validated.get(section_name)
            if isinstance(section, dict) and not section.get("external_services", False):
                section["vlm_model"] = self.shared_language_model
                section["llm_model"] = self.shared_language_model

        reasoning = validated.get("reasoning")
        if isinstance(reasoning, dict) and not reasoning.get("external_services", False):
            reasoning["llm_model"] = self.shared_language_model
        return validated


def publish_service_endpoint_alias(
    source_component_key: str,
    **context,
) -> dict | None:
    """Publish one managed service endpoint under another lifecycle component name."""
    ti = context["ti"]
    source_task_id = (
        f"{ServiceLifecycleTaskGroup.STARTUP_GROUP_ID}."
        f"{ServiceLifecycleTaskGroup.get_startup_task_name(source_component_key)}"
    )
    endpoint_raw = ti.xcom_pull(task_ids=source_task_id, key="endpoint")
    if endpoint_raw is None:
        return None
    try:
        endpoint = EndpointXComValue.model_validate(endpoint_raw).model_dump()
    except Exception as e:
        raise AirflowFailException(
            f"Endpoint XCom is invalid for shared service {source_task_id!r}: {e}"
        ) from e
    ti.xcom_push(key="endpoint", value=endpoint)
    return endpoint


class VideoDataAugmentationDAGBuilder:
    """Builder class for the Video Data Augmentation DAG."""

    def __init__(
        self,
        manifest_path: str | None = None,
        logging_level: str = "INFO",
        shared_language_model: str | None = None,
    ):
        """
        Initialize the VideoDataAugmentationDAGBuilder by loading the configuration.

        Args:
            manifest_path: Path to the deployment manifest YAML. If None, reads from
                VIDEO_DATA_AUGMENTATION_MANIFEST_PATH env var or defaults to the K8s manifest.
            logging_level: Logging level.
            shared_language_model: Model served by ``vlm_service`` for both VLM and LLM roles.
                When set, no separate ``llm_service`` lifecycle is created.
        """
        logging_level = os.environ.get("LOGGING_LEVEL", logging_level)
        self.logger = self._setup_logging(logging_level)
        self.logger.info("Logging level: %s", logging_level)

        # Initialize ComponentBuilder with deployment manifest
        if manifest_path is None:
            manifest_path = os.environ.get(
                "VIDEO_DATA_AUGMENTATION_MANIFEST_PATH",
                str(
                    VIDEO_DATA_AUGMENTATION_CONFIG_DIR / "video_data_augmentation_k8s_manifest.yaml"
                ),
            )
        try:
            self.builder = ComponentBuilder(manifest_path=manifest_path)
        except FileNotFoundError as e:
            raise AirflowConfigException(
                f"Video Data Augmentation manifest not found at {manifest_path}. "
                f"Ensure the manifest file exists or set VIDEO_DATA_AUGMENTATION_MANIFEST_PATH environment variable. "
                f"Error: {e}"
            ) from e
        except Exception as e:
            raise AirflowConfigException(
                f"Failed to load Video Data Augmentation manifest from {manifest_path}: {e}"
            ) from e
        self.shared_language_model = shared_language_model
        self.service_specs = tuple(
            spec
            for spec in VIDEO_DATA_AUGMENTATION_SERVICE_SPECS
            if not (shared_language_model and spec.component_key == "llm_service")
        )

    def _setup_logging(self, logging_level: str) -> logging.Logger:
        """
        Setup logging for the VideoDataAugmentationDAGBuilder.
        """
        logger = logging.getLogger(__name__)
        logger.setLevel(getattr(logging, logging_level.upper(), logging.INFO))
        if logger.handlers:
            return logger

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
        return logger

    @staticmethod
    def _fail_pipeline() -> None:
        """Fail-fast task used with TriggerRule.ONE_FAILED."""
        raise AirflowFailException(
            "Pipeline failed — at least one upstream task is in failed state."
        )

    @staticmethod
    def _default_payload(shared_language_model: str | None = None) -> dict:
        """Build the Airflow UI default payload for this DAG."""
        payload = VideoDataAugmentationDagConfig().model_dump()
        payload.setdefault("cosmos", {})["variable_distribution"] = {
            "variables": {
                "weather": {"cloudy": 0.5, "sunny": 0.5},
                "time_of_day": {"morning": 0.5, "afternoon": 0.5},
            }
        }
        for section in SECTIONS_WITH_TOP_LEVEL_OUTPUT_DIRECTORY:
            section_payload = payload.get(section)
            if isinstance(section_payload, dict):
                section_payload.pop("output_directory", None)
        if shared_language_model:
            for section in ("cosmos", "captioning", "visual_qa"):
                payload[section]["vlm_model"] = shared_language_model
                payload[section]["llm_model"] = shared_language_model
            payload["reasoning"]["llm_model"] = shared_language_model
        return payload

    def build_dag(
        self,
        dag_id: str = "video_data_augmentation_dag_k8s",
        description: str = "Run the Video Data Augmentation Workflow",
        tags: list[str] | None = None,
    ):
        """
        Build the Video Data Augmentation Workflow DAG.

        Flow:
            validate_payload
                │
                ├──────────────────────────────┐
                ▼                              ▼
            input_preparation            wait_for_vlm / wait_for_llm
                │                              │
                ▼                              │
            service_startup                    │
                │                              │
                └──────────────┬───────────────┘
                               │
                ┌──────────────┴───────────────┐
                ▼                              ▼
            auto_labeling_input       cosmos_augmentation
                │                              │
                │                              ▼
                │                      auto_labeling_output
                │                              │
                └──────────────┬───────────────┘
                               ▼
                          reporting
                               │
                               ▼
                       validated_output
                               │
                ┌──────────────┴───────────────┐
                ▼                              ▼
            fail_pipeline              pipeline_success

            shutdown runs after validated_output (ALL_DONE), in
            parallel with the gates, so cleanup always executes.
        """
        default_args = {
            "owner": "NVIDIA",
            "depends_on_past": False,
            "retries": 3,
            "retry_delay": timedelta(minutes=2),
        }

        dagrun_timeout = resolve_dag_timeout(self.builder.manifest.deployment.dag_timeout)

        with DAG(
            dag_id,
            default_args=default_args,
            description=description,
            start_date=datetime(2026, 1, 1),
            dagrun_timeout=dagrun_timeout,
            catchup=False,
            schedule=None,
            tags=tags or ["sdg", "video-data-augmentation", "webdataset"],
            params={
                "payload": model_payload_param(
                    VideoDataAugmentationDagConfig,
                    self._default_payload(self.shared_language_model),
                ),
            },
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            max_active_runs=5,
        ) as dag:
            # Step 1: Validate payload
            validate_payload = VideoDataAugmentationPayloadValidator(
                shared_language_model=self.shared_language_model
            ).get_validate_payload_task(task_id="validate_payload")

            # Step 2: Prepare input (single file or directory listing)
            input_preparation = InputPreparationTaskGroup(
                prepare_input_callable=prepare_video_data_augmentation_input,
                group_id="input_preparation",
            ).get_input_preparation_task_group()

            service_lifecycle = ServiceLifecycleTaskGroup(
                builder=self.builder,
                service_lifecycle_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('service_lifecycle', {}) | tojson }}",
                service_specs=self.service_specs,
                shutdown_group_id=None,
            )
            wait_tasks = service_lifecycle.get_wait_for_services_tasks()
            wait_for_vlm = wait_tasks["vlm_service"]
            wait_for_cosmos_transfer = wait_tasks["cosmos_transfer_service"]
            startup_tasks = service_lifecycle.get_service_startup_task_group()
            service_lifecycle.get_service_shutdown_task_group()
            shutdown_roots = list(service_lifecycle.shutdown_tasks.values()) or [
                service_lifecycle.shutdown_join_task
            ]
            if self.shared_language_model:
                with startup_tasks:
                    startup_llm_service = PythonOperator(
                        task_id="startup_llm_service",
                        python_callable=publish_service_endpoint_alias,
                        op_kwargs={"source_component_key": "vlm_service"},
                    )
                wait_for_vlm >> startup_llm_service
                wait_for_llm = startup_llm_service
            else:
                wait_for_llm = wait_tasks["llm_service"]

            wait_for_cosmos_evaluator = None
            cosmos_evaluator_shutdown = None
            cosmos_evaluator_failure_tasks = []
            if CosmosEvaluatorTaskGroup.is_supported(
                self.builder,
                checker_names=VDA_COSMOS_EVALUATOR_CHECKERS,
            ):
                cosmos_evaluator_task_group = CosmosEvaluatorTaskGroup(
                    builder=self.builder,
                    default_config_path=(VIDEO_DATA_AUGMENTATION_CONFIG_DIR / "cosmos_config.yaml"),
                    checker_names=VDA_COSMOS_EVALUATOR_CHECKERS,
                )
                wait_for_cosmos_evaluator = cosmos_evaluator_task_group.get_startup_task_group()
                cosmos_evaluator_failure_tasks = [
                    task
                    for task in wait_for_cosmos_evaluator.children.values()
                    if not isinstance(task, EmptyOperator)
                ]
                cosmos_evaluator_shutdown = cosmos_evaluator_task_group.get_shutdown_task_group()

            with TaskGroup(group_id="auto_labeling_input") as auto_labeling_input_group:
                super_resolution_input = SuperResolutionTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="input_preparation.prepare_input",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('super_resolution', {}) }}",
                    group_id="super_resolution_input",
                    output_group_id="auto_labeling_input",
                ).get_super_resolution_task_group()

                detection_and_tracking_input = DetectionAndTrackingTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="input_preparation.prepare_input",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('detection_and_tracking', {}) }}",
                    group_id="detection_and_tracking_input",
                    output_group_id="auto_labeling_input",
                ).get_detection_and_tracking_task_group()

                captioning_input = CaptioningTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="input_preparation.prepare_input",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('captioning', {}) }}",
                    group_id="captioning_input",
                    output_group_id="auto_labeling_input",
                ).get_captioning_task_group()

                visual_qa_input = VisualQATaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="input_preparation.prepare_input",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('visual_qa', {}) }}",
                    group_id="visual_qa_input",
                    output_group_id="auto_labeling_input",
                    prepare_args_op_kwargs={
                        "question_bank_file_path": str(
                            VIDEO_DATA_AUGMENTATION_VQA_QUESTION_BANK_PATH
                        ),
                    },
                ).get_visual_qa_task_group()

                reasoning_input = ReasoningTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="input_preparation.prepare_input",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('reasoning', {}) }}",
                    group_id="reasoning_input",
                    output_group_id="auto_labeling_input",
                ).get_reasoning_task_group()

                training_export_input = TrainingExportTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="input_preparation.prepare_input",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('training_export', {}) }}",
                    group_id="training_export_input",
                    output_group_id="auto_labeling_input",
                ).get_training_export_task_group()

            cosmos_augmentation = CosmosTaskGroup(
                builder=self.builder,
                config_generation_callable=generate_cosmos_configs,
                output_validation_callable=validate_cosmos_outputs,
                task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('cosmos', {}) }}",
                external_augmentation_pool=VDA_EXTERNAL_AUGMENTATION_POOL,
                config_generation_op_kwargs={"share_vlm_for_llm": bool(self.shared_language_model)},
            ).get_cosmos_augmentation_task_group()

            with TaskGroup(group_id="auto_labeling_output") as auto_labeling_output_group:
                super_resolution_output = SuperResolutionTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('super_resolution', {}) }}",
                    group_id="super_resolution_output",
                    output_group_id="auto_labeling_output",
                ).get_super_resolution_task_group()

                detection_and_tracking_output = DetectionAndTrackingTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('detection_and_tracking', {}) }}",
                    group_id="detection_and_tracking_output",
                    output_group_id="auto_labeling_output",
                ).get_detection_and_tracking_task_group()

                captioning_output = CaptioningTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('captioning', {}) }}",
                    group_id="captioning_output",
                    output_group_id="auto_labeling_output",
                ).get_captioning_task_group()

                visual_qa_output = VisualQATaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('visual_qa', {}) }}",
                    group_id="visual_qa_output",
                    output_group_id="auto_labeling_output",
                    prepare_args_op_kwargs={
                        "question_bank_file_path": str(
                            VIDEO_DATA_AUGMENTATION_VQA_QUESTION_BANK_PATH
                        ),
                    },
                ).get_visual_qa_task_group()

                reasoning_output = ReasoningTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('reasoning', {}) }}",
                    group_id="reasoning_output",
                    output_group_id="auto_labeling_output",
                ).get_reasoning_task_group()

                training_export_output = TrainingExportTaskGroup(
                    builder=self.builder,
                    input_xcom_task_id="cosmos_augmentation.validate_outputs",
                    input_xcom_key="return_value",
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('training_export', {}) }}",
                    group_id="training_export_output",
                    output_group_id="auto_labeling_output",
                ).get_training_export_task_group()

            # Failure sentinel: if any upstream task fails (including within
            # task groups), this task runs and fails the DagRun. This avoids
            # direct metadata DB access, which is disallowed in Airflow 3 task runtime.
            fail_pipeline = PythonOperator(
                task_id="fail_pipeline",
                python_callable=self._fail_pipeline,
                trigger_rule=TriggerRule.ONE_FAILED,
                retries=0,
            )

            # Success gate only opens when all upstream tasks succeeded or were skipped.
            pipeline_success = EmptyOperator(
                task_id="pipeline_success",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            validated_output = ValidatedOutputTaskGroup(
                validation_callable=validate_video_data_augmentation_pipeline_outputs,
            ).get_validated_output_task_group()

            # Define the order of execution for the Video Data Augmentation Workflow DAG
            # validate_payload -> input_preparation (and startup_tasks in parallel)
            # wait_for_* must run after validate_payload (they template service_lifecycle from its XCom)
            validate_payload >> input_preparation
            input_preparation >> startup_tasks
            language_service_waits = [wait_for_vlm]
            if wait_for_llm is not wait_for_vlm:
                language_service_waits.append(wait_for_llm)
            validate_payload >> [*language_service_waits, wait_for_cosmos_transfer]
            if wait_for_cosmos_evaluator is not None:
                input_preparation >> wait_for_cosmos_evaluator

            # Main processing graph:
            # input_preparation fans out to the input auto-labeling group and
            # Cosmos. Cosmos feeds the output auto-labeling group, then
            # final output validation completes the pipeline.
            # Shutdown/cleanup is NOT part of this chain so that the
            # fail_pipeline / pipeline_success gates observe processing
            # outcomes directly.  If a processing task fails, downstream
            # processing tasks are skipped (default trigger rule) and
            # fail_pipeline fires, marking the DagRun as failed.
            # Shutdown runs independently (ALL_DONE) so cleanup always
            # executes regardless of processing failures.
            for language_service_wait in language_service_waits:
                language_service_wait >> cosmos_augmentation
                language_service_wait >> [captioning_input, captioning_output]
                language_service_wait >> [visual_qa_input, visual_qa_output]
            wait_for_cosmos_transfer >> cosmos_augmentation
            if wait_for_cosmos_evaluator is not None:
                wait_for_cosmos_evaluator >> cosmos_augmentation
            wait_for_llm >> [reasoning_input, reasoning_output]

            chain(
                super_resolution_input,
                detection_and_tracking_input,
                captioning_input,
                visual_qa_input,
                reasoning_input,
                training_export_input,
            )
            chain(
                super_resolution_output,
                detection_and_tracking_output,
                captioning_output,
                visual_qa_output,
                reasoning_output,
                training_export_output,
            )
            input_preparation >> auto_labeling_input_group
            input_preparation >> cosmos_augmentation
            cosmos_augmentation >> auto_labeling_output_group
            auto_labeling_input_group >> validated_output
            auto_labeling_output_group >> validated_output

            gate_upstreams = [
                auto_labeling_input_group,
                cosmos_augmentation,
                auto_labeling_output_group,
                validated_output,
            ]

            # Connect all critical steps to the success/failure gates.
            # Airflow 3 does not support list >> [downstreams]; set edges per task.
            for task in cosmos_evaluator_failure_tasks:
                task >> fail_pipeline
            for task in gate_upstreams:
                task >> [fail_pipeline, pipeline_success]
            # Keep shutdown directly downstream of processing completion with
            # ALL_DONE semantics on the cleanup tasks so they are eligible
            # to run regardless of upstream task outcomes.
            for task in gate_upstreams:
                task >> shutdown_roots
                if cosmos_evaluator_shutdown is not None:
                    task >> cosmos_evaluator_shutdown
            if cosmos_evaluator_shutdown is not None:
                fail_pipeline >> cosmos_evaluator_shutdown
                # A failure can reach the gate while evaluator deployments are
                # still running. Wait for every startup attempt before cleanup.
                list(wait_for_cosmos_evaluator.children.values()) >> cosmos_evaluator_shutdown

        return dag


# --- DAG Generation ---
# Each manifest produces its own DAG. Users see one DAG per platform in the Airflow UI.

# Kubernetes DAG (generated if manifest exists)
_K8S_MANIFEST = os.environ.get(
    "VIDEO_DATA_AUGMENTATION_K8S_MANIFEST_PATH",
    str(VIDEO_DATA_AUGMENTATION_CONFIG_DIR / "video_data_augmentation_k8s_manifest.yaml"),
)
if os.path.exists(_K8S_MANIFEST):
    try:
        video_data_augmentation_k8s_dag_builder = VideoDataAugmentationDAGBuilder(
            manifest_path=_K8S_MANIFEST,
            shared_language_model=VDA_K8S_SHARED_LANGUAGE_MODEL,
        )
        video_data_augmentation_dag_k8s = video_data_augmentation_k8s_dag_builder.build_dag(
            dag_id="video_data_augmentation_dag_k8s",
            description="Run the Video Data Augmentation Workflow on Native Kubernetes",
            tags=["sdg", "kubernetes", "video-data-augmentation", "webdataset"],
        )
    except Exception:
        logging.getLogger(__name__).warning(
            "Failed to load K8s manifest at %s, skipping K8s DAG generation",
            _K8S_MANIFEST,
            exc_info=True,
        )

# OSMO DAG (generated if manifest exists)
_OSMO_MANIFEST = os.environ.get(
    "VIDEO_DATA_AUGMENTATION_OSMO_MANIFEST_PATH",
    str(VIDEO_DATA_AUGMENTATION_CONFIG_DIR / "video_data_augmentation_osmo_manifest.yaml"),
)
if os.path.exists(_OSMO_MANIFEST):
    try:
        video_data_augmentation_osmo_dag_builder = VideoDataAugmentationDAGBuilder(
            manifest_path=_OSMO_MANIFEST
        )
        video_data_augmentation_dag_osmo = video_data_augmentation_osmo_dag_builder.build_dag(
            dag_id="video_data_augmentation_dag_osmo",
            description="Run the Video Data Augmentation Workflow on OSMO",
            tags=["sdg", "osmo", "video-data-augmentation", "webdataset"],
        )
    except Exception:
        logging.getLogger(__name__).warning(
            "Failed to load OSMO manifest at %s, skipping OSMO DAG generation",
            _OSMO_MANIFEST,
            exc_info=True,
        )
