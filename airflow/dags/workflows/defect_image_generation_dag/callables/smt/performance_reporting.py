# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""SMT DIG workload semantics for shared orchestration reporting."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dags.shared.task_groups.reporting import (
    OrchestrationReportDefinition,
    generate_orchestration_report,
    render_polished_dashboard,
    xcom_length,
)
from dags.shared.task_groups.reporting import (
    collect_run_metadata as _collect_run_metadata,
)
from dags.shared.task_groups.reporting.task_timing import (
    latest_end_time,
    rate,
    seconds_between,
    stage_wall_seconds,
)
from dags.shared.utils.msc_utils import write_file_to_directory
from dags.workflows.defect_image_generation_dag.callables.smt.config_generation import (
    validate_payload,
)
from dags.workflows.defect_image_generation_dag.callables.smt.images import (
    count_rendered_simulation_images,
)

REPORT_FILENAME = "cdf_orchestration_stats.yaml"
HTML_REPORT_FILENAME = "cdf_orchestration_stats.html"
REPORT_TYPE = "cdf_smt_defect_image_generation_orchestration_stats"
SCHEMA_VERSION = 1

_FINAL_STAGE = {
    "simulation": (
        "Simulation",
        "Simulation images",
        "simulation_stage_wall_seconds",
    ),
    "sim2real": (
        "Augmentation",
        "Golden images",
        "augmentation_stage_wall_seconds",
    ),
    "full": (
        "AnomalyGen",
        "Validated defect images",
        "anomaly_generation_stage_wall_seconds",
    ),
    "defect": (
        "AnomalyGen",
        "Validated defect images",
        "anomaly_generation_stage_wall_seconds",
    ),
}


def collect_run_metadata(dag_id: str, run_id: str) -> dict[str, Any]:
    """Compatibility seam that delegates REST collection to shared reporting."""
    return _collect_run_metadata(dag_id, run_id)


def _xcom(ti: Any, task_id: str | None, key: str = "return_value") -> Any:
    if not task_id:
        return None
    try:
        return ti.xcom_pull(task_ids=task_id, key=key)
    except Exception:
        return None


def _simulation_image_count(goldens: Any) -> int | None:
    if not isinstance(goldens, list):
        return None
    return sum(
        count_rendered_simulation_images(golden["simulation_directory"]) for golden in goldens
    )


def _requested_defect_count(items: Any) -> int | None:
    if not isinstance(items, list):
        return None
    total = 0
    for item in items:
        config = item.get("anomaly_generation_config", {}) if isinstance(item, Mapping) else {}
        counts = config.get("per_defect_counts")
        if isinstance(counts, Mapping):
            values = list(counts.values())
            if not all(isinstance(value, int) for value in values):
                return None
            total += sum(values)
        elif isinstance(config.get("num_sdg"), int):
            total += config["num_sdg"]
        else:
            return None
    return total


def _validated_defect_count(value: Any) -> int | None:
    if isinstance(value, Mapping):
        value = [value]
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return None
    counts = [item.get("item_count") for item in value if isinstance(item, Mapping)]
    if len(counts) != len(value) or not all(isinstance(count, int) for count in counts):
        return None
    return sum(counts)


def _task_categories(
    report: dict[str, Any], stage_task_ids: dict[str, list[str]]
) -> dict[str, str]:
    simulation_tasks = set(stage_task_ids.get("simulation", []))
    augmentation_tasks = set(stage_task_ids.get("augmentation", []))
    anomalygen_tasks = set(stage_task_ids.get("anomaly_generation", []))
    categories = {}
    for task in report.get("task_instances") or []:
        task_id = task.get("task_id")
        if not task_id:
            continue
        if task_id in simulation_tasks:
            category = "simulation"
        elif task_id in augmentation_tasks:
            category = "augmentation"
        elif task_id in anomalygen_tasks:
            category = "anomalygen"
        elif task_id.startswith(("service_startup.", "service_shutdown.")):
            category = "services"
        elif "validate" in task_id:
            category = "validation"
        else:
            category = "orchestration"
        categories[task_id] = category
    return categories


def _dashboard_display(mode: str) -> dict[str, Any]:
    stage_label, final_label, stage_seconds_key = _FINAL_STAGE[mode]
    outputs = {
        "simulation": [("Simulation", "simulation_images", "simulation_images")],
        "sim2real": [("Golden", "golden_images", "successful_golden_images")],
        "defect": [("Defect", "defect_images", "validated_defect_images")],
        "full": [
            ("Golden", "golden_images", "successful_golden_images"),
            ("Defect", "defect_images", "validated_defect_images"),
        ],
    }[mode]
    return {
        "title": "SMT Defect Image Generation",
        "unit_singular": "image",
        "unit_plural": "images",
        "input_label": "simulation images",
        "input_count_key": "simulation_images",
        "per_input_count_key": "requested_defect_images",
        "per_input_label": "defects requested",
        "final_label": final_label,
        "final_count_key": "final_images",
        "successful_count_key": "final_images",
        "successful_label": "validated final images",
        "stage_seconds_key": stage_seconds_key,
        "stage_label": stage_label,
        "throughput_cards": [
            {
                "label": f"{label} image throughput",
                "rate_key": rate_key,
                "count_key": count_key,
                "seconds_key": f"{rate_key}_elapsed_seconds",
            }
            for label, rate_key, count_key in outputs
        ],
        "throughput_description": (
            "Each rate is the number of output images divided by elapsed time from DAG start "
            f"until {'simulation completes' if mode == 'simulation' else 'those images pass validation'}. "
            "Rates include setup and waiting time and describe this run, not steady-state capacity."
        ),
        "timeline_legend": {
            "simulation": "Simulation",
            "augmentation": "Augmentation",
            "anomalygen": "AnomalyGen",
            "services": "Service deploy",
            "validation": "Validation",
            "orchestration": "Orchestration",
        },
    }


def generate_smt_performance_report(
    payload: str | dict[str, Any] = "",
    run_id: str = "",
    *,
    excluded_task_ids: list[str] | None = None,
    completion_task_ids: dict[str, str] | None = None,
    workload_task_ids: dict[str, str] | None = None,
    stage_task_ids: dict[str, list[str]] | None = None,
    **context: Any,
) -> dict[str, Any]:
    """Write SMT DIG output throughput, stage timings, YAML, and HTML artifacts."""
    config = validate_payload(payload)
    completion_task_ids = completion_task_ids or {}
    workload_task_ids = workload_task_ids or {}
    stage_task_ids = stage_task_ids or {}

    def workload(_config, report_context: dict[str, Any]) -> dict[str, Any]:
        ti = report_context["ti"]
        simulation_images = _simulation_image_count(
            _xcom(ti, workload_task_ids.get("configs"), "golden_work_items")
        )
        golden_images = xcom_length(ti, workload_task_ids.get("golden_images"))
        requested_defects = _requested_defect_count(
            _xcom(ti, workload_task_ids.get("configs"), "anomaly_package_kwargs")
        )
        defect_images = _validated_defect_count(_xcom(ti, workload_task_ids.get("defect_images")))
        final_images = {
            "simulation": simulation_images,
            "sim2real": golden_images,
            "full": defect_images,
            "defect": defect_images,
        }[config.pipeline_mode]
        return {
            "pipeline_mode": config.pipeline_mode,
            "simulation_images": simulation_images,
            "successful_golden_images": golden_images,
            "requested_defect_images": requested_defects,
            "validated_defect_images": defect_images,
            "final_images": final_images,
        }

    def throughput(
        workload_data: dict[str, Any], tasks: list[dict[str, Any]], elapsed: float | None
    ) -> dict[str, Any]:
        simulation_seconds = stage_wall_seconds(tasks, stage_task_ids.get("simulation", []))
        augmentation_seconds = stage_wall_seconds(tasks, stage_task_ids.get("augmentation", []))
        anomaly_seconds = stage_wall_seconds(tasks, stage_task_ids.get("anomaly_generation", []))
        golden_seconds = None
        if config.pipeline_mode in {"full", "sim2real"}:
            # The shared elapsed window ends at workflow completion. Remove the
            # portion after golden validation to retain the same DAG-start boundary.
            after_golden = seconds_between(
                latest_end_time(tasks, completion_task_ids.get(config.pipeline_mode, "")),
                latest_end_time(tasks, workload_task_ids.get("golden_images", "")),
            )
            if elapsed is not None and after_golden is not None:
                golden_seconds = round(max(0, elapsed - after_golden), 3)
        defect_seconds = elapsed if config.pipeline_mode in {"full", "defect"} else None
        simulation_elapsed = elapsed if config.pipeline_mode == "simulation" else None
        return {
            "simulation_stage_wall_seconds": simulation_seconds,
            "augmentation_stage_wall_seconds": augmentation_seconds,
            "anomaly_generation_stage_wall_seconds": anomaly_seconds,
            "simulation_images_elapsed_seconds": simulation_elapsed,
            "simulation_images": rate(workload_data["simulation_images"], simulation_elapsed),
            "golden_images_elapsed_seconds": golden_seconds,
            "golden_images": rate(workload_data["successful_golden_images"], golden_seconds),
            "defect_images_elapsed_seconds": defect_seconds,
            "defect_images": rate(workload_data["validated_defect_images"], defect_seconds),
        }

    display = _dashboard_display(config.pipeline_mode)
    definition = OrchestrationReportDefinition(
        report_type=REPORT_TYPE,
        schema_version=SCHEMA_VERSION,
        yaml_filename=REPORT_FILENAME,
        html_filename=HTML_REPORT_FILENAME,
        parse_payload=validate_payload,
        output_directory=lambda parsed: parsed.output_directory,
        workload_builder=workload,
        throughput_builder=throughput,
        render_html=lambda report: render_polished_dashboard(
            report, display, _task_categories(report, stage_task_ids)
        ),
    )
    return generate_orchestration_report(
        definition,
        config.model_dump(),
        run_id,
        completion_task_id=completion_task_ids.get(config.pipeline_mode, ""),
        excluded_task_ids=excluded_task_ids,
        metadata_collector=collect_run_metadata,
        writer=write_file_to_directory,
        **context,
    )
