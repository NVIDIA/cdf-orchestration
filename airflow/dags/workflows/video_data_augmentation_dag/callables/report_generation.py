# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Video Data Augmentation workflow report generation callable."""

import ast
import copy
import logging
from datetime import datetime, timezone
from typing import Dict, List

from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import InputPreparationResult
from dags.shared.utils.msc_utils import (
    copy_file,
    delete_path,
    is_directory_empty,
    is_file,
    read_file_as_dict,
    write_file_to_directory,
)
from dags.shared.utils.video_input_utils import normalize_directory_path
from dags.workflows.video_data_augmentation_dag.models import (
    AutoLabelingSubcomponentMetadata,
    CosmosMetadata,
    Report,
    ReportAutoLabelingSubcomponent,
    ReportCosmos,
    ReportCosmosLoop,
    ReportCosmosLoopChecker,
    ReportOutput,
    ReportTiming,
    ReportTimingAggregatedTask,
    ReportTimingCosmosSubcomponent,
    ReportTimingTask,
    VideoDataAugmentationDagConfig,
)


def _read_pl_subcomponent_metadata(
    auto_labeling_dir: str,
    *,
    subcomponent: str,
) -> AutoLabelingSubcomponentMetadata | None:
    """Determine PL subcomponent success from the flat output directory."""
    output_files: dict[str, str] = {
        "mcq": "video_mcq.json",
        "dt": "objects.json",
    }
    sentinel = output_files.get(subcomponent)
    if sentinel and is_file(auto_labeling_dir + sentinel):
        return AutoLabelingSubcomponentMetadata(total_videos=1, successful=1, failed=0)

    return None


def _copy_pl_output_files(
    source_dir: str,
    dest_dir: str,
    filenames: list[str],
    *,
    label: str,
) -> None:
    """Copy specific files from a flat PL output directory to a destination."""
    copied = 0
    for filename in filenames:
        src = source_dir + filename
        if is_file(src):
            try:
                copy_file(src, dest_dir + filename)
                copied += 1
            except Exception as e:
                raise AirflowFailException(
                    f"Failed to copy auto-labeling {label} output: {src} -> {dest_dir + filename}: {e}"
                ) from e
    if copied == 0:
        logging.warning("No auto-labeling %s output found in %s", label, source_dir)
    else:
        logging.info("Copied %d %s file(s) from %s to %s", copied, label, source_dir, dest_dir)


class AutoLabelingReportingUtils:
    """Utility functions for auto-labeling reporting."""

    @staticmethod
    def get_auto_labeling_metadata_lists(
        auto_labeling_directory: str,
        output_statuses: dict,
    ) -> tuple[
        List[AutoLabelingSubcomponentMetadata], List[AutoLabelingSubcomponentMetadata], dict
    ]:
        """Get detection/tracking and MCQ generation metadata lists."""
        dt_metadata_list: List[AutoLabelingSubcomponentMetadata] = []
        mcq_metadata_list: List[AutoLabelingSubcomponentMetadata] = []
        new_output_statuses = copy.deepcopy(output_statuses)
        for video_statuses in new_output_statuses.values():
            for status in video_statuses:
                status["passed"] = False

        for video_name, video_statuses in output_statuses.items():
            for aug_idx, _ in enumerate(video_statuses):
                auto_labeling_dir = normalize_directory_path(
                    auto_labeling_directory.rstrip("/") + "/" + video_name + "/" + str(aug_idx)
                )
                mcq_metadata = _read_pl_subcomponent_metadata(auto_labeling_dir, subcomponent="mcq")
                dt_metadata = _read_pl_subcomponent_metadata(auto_labeling_dir, subcomponent="dt")

                if mcq_metadata is None and dt_metadata is None:
                    logging.warning(
                        "No auto-labeling output found in %s. "
                        "The augmentation will not be included in the report.",
                        auto_labeling_dir,
                    )
                    continue

                successful = True

                if mcq_metadata is not None:
                    mcq_metadata_list.append(mcq_metadata)
                    successful = successful and bool(mcq_metadata.successful)
                else:
                    logging.warning("No MCQ generation output found in %s", auto_labeling_dir)

                if dt_metadata is not None:
                    dt_metadata_list.append(dt_metadata)
                    successful = successful and bool(dt_metadata.successful)
                else:
                    logging.warning(
                        "No detection and tracking output found in %s", auto_labeling_dir
                    )

                try:
                    new_output_statuses[video_name][aug_idx]["passed"] = (
                        successful and output_statuses[video_name][aug_idx]["passed"]
                    )
                except Exception as e:
                    raise AirflowFailException(
                        f"Failed to update auto-labeling output status for video: {video_name}[{aug_idx}]: {e}"
                    ) from e

        logging.info(
            "Found %d valid auto-labeling detection and tracking metadata in %s",
            len(dt_metadata_list),
            auto_labeling_directory,
        )
        logging.info(
            "Found %d valid auto-labeling MCQ generation metadata in %s",
            len(mcq_metadata_list),
            auto_labeling_directory,
        )

        return dt_metadata_list, mcq_metadata_list, new_output_statuses

    @staticmethod
    def get_auto_labeling_subcomponent_report(
        auto_labeling_subcomponent_metadata_list: List[AutoLabelingSubcomponentMetadata],
    ) -> ReportAutoLabelingSubcomponent:
        """Get the auto-labeling subcomponent report."""
        total = sum(metadata.total_videos for metadata in auto_labeling_subcomponent_metadata_list)
        successful = sum(
            metadata.successful for metadata in auto_labeling_subcomponent_metadata_list
        )
        failed = sum(metadata.failed for metadata in auto_labeling_subcomponent_metadata_list)
        success_percentage = round(successful / total, 2) if total > 0 else 0.0

        return ReportAutoLabelingSubcomponent(
            total=total,
            successful=successful,
            failed=failed,
            success_percentage=success_percentage,
        )

    @staticmethod
    def process_auto_labeling_directory(
        auto_labeling_directory: str, output_statuses: dict
    ) -> tuple[ReportAutoLabelingSubcomponent, ReportAutoLabelingSubcomponent, dict]:
        """Process auto-labeling output and update output statuses."""
        try:
            auto_labeling_directory_empty = is_directory_empty(auto_labeling_directory)
        except Exception as e:
            raise AirflowFailException(
                f"An error occurred while checking if the directory {auto_labeling_directory} is empty: {e}"
            ) from e
        if auto_labeling_directory_empty:
            logging.warning(
                "No auto-labeling directory found at path: %s. "
                "No auto-labeling output will be processed.",
                auto_labeling_directory,
            )
        else:
            logging.info("Processing auto-labeling output at path: %s", auto_labeling_directory)

        (
            dt_metadata_list,
            mcq_metadata_list,
            output_statuses,
        ) = AutoLabelingReportingUtils.get_auto_labeling_metadata_lists(
            auto_labeling_directory=auto_labeling_directory, output_statuses=output_statuses
        )

        report_dt = AutoLabelingReportingUtils.get_auto_labeling_subcomponent_report(
            dt_metadata_list
        )
        report_mcq = AutoLabelingReportingUtils.get_auto_labeling_subcomponent_report(
            mcq_metadata_list
        )

        return report_dt, report_mcq, output_statuses


class CosmosReportingUtils:
    """Utilities for Cosmos reporting."""

    @staticmethod
    def create_cosmos_report(metadata_list: List[CosmosMetadata]) -> ReportCosmos | None:
        """Add Cosmos metadata to the report."""
        if len(metadata_list) == 0:
            logging.warning("No cosmos metadata found when creating cosmos report.")
            return None

        report = ReportCosmos()
        report.successful = sum(1 for metadata in metadata_list if metadata.passed)
        report.failed = sum(1 for metadata in metadata_list if not metadata.passed)
        report.total = report.successful + report.failed
        report.success_percentage = (
            round(report.successful / report.total, 2) if report.total else 0
        )
        report.average_number_of_loops = round(
            sum(len(metadata.loops) for metadata in metadata_list) / len(metadata_list), 2
        )
        report.augmentation_loops = []

        max_augmentation_loops = 0
        for metadata in metadata_list:
            max_augmentation_loops = max(max_augmentation_loops, len(metadata.loops))

        for i in range(max_augmentation_loops):
            loop_report = ReportCosmosLoop()
            loop_report.checkers = {
                "hallucination_check": ReportCosmosLoopChecker(),
                "attribute_verification_check": ReportCosmosLoopChecker(),
            }
            for metadata in metadata_list:
                if len(metadata.loops) <= i:
                    continue

                loop = metadata.loops[i]
                loop_report.total += 1
                passed = loop.hallucination_check.passed and (
                    loop.attribute_verification_check is None
                    or loop.attribute_verification_check.passed
                )
                if passed:
                    loop_report.passed += 1
                else:
                    loop_report.failed += 1

                hallucination = loop_report.checkers["hallucination_check"]
                hallucination.total += 1
                if loop.hallucination_check.passed:
                    hallucination.passed += 1
                else:
                    hallucination.failed += 1

                if loop.attribute_verification_check is not None:
                    attribute = loop_report.checkers["attribute_verification_check"]
                    attribute.total += 1
                    if loop.attribute_verification_check.passed:
                        attribute.passed += 1
                    else:
                        attribute.failed += 1

            loop_report.success_percentage = (
                round(loop_report.passed / loop_report.total, 2) if loop_report.total > 0 else 0.0
            )
            for checker in loop_report.checkers.values():
                checker.success_percentage = (
                    round(checker.passed / checker.total, 2) if checker.total > 0 else 0.0
                )

            report.augmentation_loops.append(loop_report)
        return report

    @staticmethod
    def get_cosmos_timing_details(
        cosmos_metadata_list: List[CosmosMetadata],
    ) -> Dict[str, ReportTimingCosmosSubcomponent]:
        """Get timing details for the Cosmos output."""
        service_durations = []
        loop_durations = []
        augmentation_durations = []
        checkers_durations = {}
        for metadata in cosmos_metadata_list:
            service_duration = 0
            for loop in metadata.loops:
                service_duration += loop.duration_seconds
                loop_durations.append(loop.duration_seconds)
                augmentation_durations.append(loop.augmentation_duration_seconds)
                checkers = ["hallucination_check", "attribute_verification_check"]
                for checker in checkers:
                    if checker not in checkers_durations:
                        checkers_durations[checker] = []
                    if getattr(loop, checker) is not None:
                        checkers_durations[checker].append(
                            getattr(loop, checker).details.duration_seconds
                        )
            service_durations.append(service_duration)

        details = {}
        if len(service_durations) > 0:
            details["service"] = ReportTimingCosmosSubcomponent(
                count=len(service_durations),
                min_duration_seconds=round(min(service_durations), 2),
                max_duration_seconds=round(max(service_durations), 2),
                average_duration_seconds=round(sum(service_durations) / len(service_durations), 2),
            )
        if len(loop_durations) > 0:
            details["loop"] = ReportTimingCosmosSubcomponent(
                count=len(loop_durations),
                min_duration_seconds=round(min(loop_durations), 2),
                max_duration_seconds=round(max(loop_durations), 2),
                average_duration_seconds=round(sum(loop_durations) / len(loop_durations), 2),
            )
        if len(augmentation_durations) > 0:
            details["augmentation"] = ReportTimingCosmosSubcomponent(
                count=len(augmentation_durations),
                min_duration_seconds=round(min(augmentation_durations), 2),
                max_duration_seconds=round(max(augmentation_durations), 2),
                average_duration_seconds=round(
                    sum(augmentation_durations) / len(augmentation_durations), 2
                ),
            )
        for checker, durations in checkers_durations.items():
            if len(durations) > 0:
                details[checker] = ReportTimingCosmosSubcomponent(
                    count=len(durations),
                    min_duration_seconds=round(min(durations), 2),
                    max_duration_seconds=round(max(durations), 2),
                    average_duration_seconds=round(sum(durations) / len(durations), 2),
                )
        return details

    @staticmethod
    def get_cosmos_metadata_list(
        cosmos_directory: str,
        output_statuses: dict,
    ) -> tuple[List[CosmosMetadata], dict]:
        """Get the Cosmos metadata list."""
        cosmos_metadata_list: List[CosmosMetadata] = []
        logging.info(
            "Checking cosmos metadata for %d expected key(s) in %s",
            len(output_statuses),
            cosmos_directory,
        )

        new_output_statuses = copy.deepcopy(output_statuses)
        for video_statuses in new_output_statuses.values():
            for status in video_statuses:
                status["passed"] = False

        for video_name, video_statuses in output_statuses.items():
            for aug_idx, _ in enumerate(video_statuses):
                cosmos_dir = normalize_directory_path(
                    cosmos_directory.rstrip("/") + "/" + video_name + "/" + str(aug_idx)
                )
                metadata_path = cosmos_dir + "metadata.json"
                if not is_file(cosmos_dir + "output.mp4"):
                    logging.warning(
                        "No output.mp4 file found in augmentation output directory: %s. "
                        "The augmentation will not be included in the output or report.",
                        cosmos_dir,
                    )
                    continue
                if not is_file(metadata_path):
                    logging.warning(
                        "No metadata.json file found in augmentation output directory: %s. "
                        "The augmentation will not be included in the output or report.",
                        cosmos_dir,
                    )
                    continue

                cosmos_metadata = read_file_as_dict(
                    path=metadata_path,
                    model_class=CosmosMetadata,
                )
                if cosmos_metadata is not None:
                    cosmos_metadata_list.append(cosmos_metadata)

                try:
                    new_output_statuses[video_name][aug_idx]["passed"] = (
                        cosmos_metadata.passed and output_statuses[video_name][aug_idx]["passed"]
                    )
                except Exception as e:
                    raise AirflowFailException(
                        f"Failed to update cosmos output status for video: {video_name}[{aug_idx}]: {e}"
                    ) from e

        logging.info(
            "Found %d valid cosmos metadata in %s", len(cosmos_metadata_list), cosmos_directory
        )
        return cosmos_metadata_list, new_output_statuses

    @staticmethod
    def process_cosmos_directory(
        cosmos_directory: str,
        output_statuses: dict,
    ) -> tuple[ReportCosmos | None, Dict[str, ReportTimingCosmosSubcomponent] | None, dict]:
        """Process Cosmos output and update output statuses."""
        try:
            cosmos_directory_empty = is_directory_empty(cosmos_directory)
            if cosmos_directory_empty:
                logging.warning(
                    "No cosmos directory found at path: %s. No cosmos output will be processed.",
                    cosmos_directory,
                )
                return None, None, output_statuses
        except Exception as e:
            raise AirflowFailException(
                f"An error occurred while checking if the cosmos directory {cosmos_directory} is empty: {e}"
            ) from e

        cosmos_metadata_list, output_statuses = CosmosReportingUtils.get_cosmos_metadata_list(
            cosmos_directory=cosmos_directory, output_statuses=output_statuses
        )

        report = CosmosReportingUtils.create_cosmos_report(cosmos_metadata_list)
        cosmos_timing_details = CosmosReportingUtils.get_cosmos_timing_details(cosmos_metadata_list)
        return report, cosmos_timing_details, output_statuses


def setup_output_directory(
    output_directory: str,
    video_keys: list[str],
) -> dict:
    """Set up the output directory and return per-video output statuses."""
    try:
        if not is_directory_empty(output_directory):
            delete_path(path_url=output_directory, recursive=True)
            logging.info("Deleted existing output directory: %s", output_directory)
        else:
            logging.info("No existing output directory found: %s", output_directory)
    except Exception as e:
        raise AirflowFailException(
            f"A failure occurred while setting up the output directory using the multistorage client: {e}"
        ) from e

    output_statuses = {}
    for video_key in video_keys:
        output_statuses.setdefault(video_key, []).append({"passed": True})

    return output_statuses


def copy_to_output_directory(
    cosmos_path: str,
    output_path: str,
    output_statuses: dict,
    output_base_url: str,
    copy_pseudo_labeling_outputs: bool = False,
) -> None:
    """Copy successful videos into the final output directory."""
    for video_name, video_statuses in output_statuses.items():
        for aug_idx, output_status in enumerate(video_statuses):
            if not output_status["passed"]:
                continue

            cosmos_video_path = cosmos_path + "/" + video_name + "/" + str(aug_idx) + "/output.mp4"
            output_video_path = (
                output_path + "/videos/" + video_name + "/" + str(aug_idx) + "/video.mp4"
            )
            logging.info("Cosmos video path: %s", cosmos_video_path)
            logging.info("Output video path: %s", output_video_path)
            if is_file(cosmos_video_path):
                try:
                    copy_file(cosmos_video_path, output_video_path)
                except Exception as e:
                    raise AirflowFailException(
                        f"Failed to move augmentation video: {cosmos_video_path} to {output_video_path}: {e}"
                    ) from e
            else:
                logging.warning("No cosmos output video found at path: %s", cosmos_video_path)

            if not copy_pseudo_labeling_outputs:
                continue

            pl_dir_url = (
                output_base_url.rstrip("/")
                + "/auto_labeling_output/"
                + video_name
                + "/"
                + str(aug_idx)
                + "/"
            )
            output_video_dir = (
                output_base_url.rstrip("/")
                + "/output/videos/"
                + video_name
                + "/"
                + str(aug_idx)
                + "/"
            )

            _copy_pl_output_files(
                pl_dir_url,
                output_video_dir + "mcq-generation/",
                ["video_mcq.json"],
                label="MCQ generation",
            )
            _copy_pl_output_files(
                pl_dir_url,
                output_video_dir + "detection-and-tracking/",
                ["objects.json", "instances.json", "events.json"],
                label="detection and tracking",
            )


def create_timing_report(
    mapped_timings: Dict[str, ReportTimingAggregatedTask] | None = None,
    **context,
) -> ReportTiming:
    """Get the timing report using execution context only."""
    report_timing = ReportTiming()

    if mapped_timings:
        report_timing.mapped_tasks.update(mapped_timings)

    ti = context.get("ti")
    if (
        ti is not None
        and getattr(ti, "task_id", None) is not None
        and getattr(ti, "start_date", None) is not None
        and getattr(ti, "end_date", None) is not None
        and getattr(ti, "duration", None) is not None
    ):
        if getattr(ti, "map_index", -1) >= 0:
            report_timing.mapped_tasks[ti.task_id] = ReportTimingAggregatedTask(
                count=1,
                min_duration_seconds=ti.duration,
                max_duration_seconds=ti.duration,
                average_duration_seconds=round(ti.duration, 2),
            )
        else:
            report_timing.tasks[ti.task_id] = ReportTimingTask(
                start_time=ti.start_date.isoformat(),
                end_time=ti.end_date.isoformat(),
                duration_seconds=ti.duration,
            )

    dag_run = context.get("dag_run")
    if dag_run is not None and hasattr(dag_run, "start_date") and dag_run.start_date is not None:
        report_timing.start_time = dag_run.start_date.isoformat()

    now = datetime.now(timezone.utc)
    report_timing.end_time = now.isoformat()

    if report_timing.start_time is not None:
        try:
            start = datetime.fromisoformat(report_timing.start_time)
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            duration = (now - start).total_seconds()
            report_timing.duration_seconds = duration
        except Exception as e:
            logging.warning("Could not compute duration_seconds: %s", e)

    return report_timing


def create_output_report(
    output_directory: str,
    output_statuses: dict,
) -> ReportOutput:
    """Create the output report."""
    successful_outputs = 0
    failed_outputs = 0
    total_outputs = 0
    logging.info("Output statuses: %s", output_statuses)
    for video_statuses in output_statuses.values():
        logging.info("Video: %s", video_statuses)
        for status in video_statuses:
            logging.info("Status: %s", status)
            if status["passed"]:
                successful_outputs += 1
            else:
                failed_outputs += 1
            total_outputs += 1
    success_percentage = round(successful_outputs / total_outputs, 2) if total_outputs > 0 else 0.0
    return ReportOutput(
        total=total_outputs,
        successful=successful_outputs,
        failed=failed_outputs,
        success_percentage=success_percentage,
        directory=output_directory,
    )


def generate_report(**context) -> None:
    """Generate the workflow report."""
    logging.info("\n\n")
    logging.info("=" * 40)
    logging.info("Generating report")
    logging.info("=" * 40)
    logging.info("Beginning setup for report generation...")

    try:
        input_data = VideoDataAugmentationDagConfig.model_validate(
            ast.literal_eval(context["payload"])
        )
    except Exception as e:
        raise AirflowFailException(f"Payload validation failed: {e}") from e

    run_id = context["run_id"]

    prepared_filenames: list[str] | None = None
    try:
        prepared_raw = context["ti"].xcom_pull(
            task_ids="input_preparation.prepare_input", key="return_value"
        )
        try:
            prepared = InputPreparationResult.model_validate(prepared_raw)
            prepared_filenames = [
                v.video_key
                for v in prepared.videos
                for _ in range(input_data.cosmos.num_augmentation)
            ]
        except Exception as e:
            raise AirflowFailException(
                f"prepare_input XCom is not a valid InputPreparationResult: {e}"
            ) from e
    except Exception as e:
        raise AirflowFailException(f"Failed to get input directory path from XCom: {e}") from e

    output_base_url = str(URL(input_data.cosmos.output_directory) / run_id)

    cosmos_directory = str(URL(output_base_url) / "cosmos")
    output_directory = str(URL(output_base_url) / "output")
    logging.info("Report generation setup complete!")

    logging.info("Setting up output directory...")
    output_statuses = setup_output_directory(
        output_directory=output_directory,
        video_keys=prepared_filenames,
    )

    logging.info("Processing Cosmos directory...")
    cosmos_report, cosmos_timing_details, output_statuses = (
        CosmosReportingUtils.process_cosmos_directory(
            cosmos_directory=cosmos_directory, output_statuses=output_statuses
        )
    )
    logging.info("Cosmos directory processing complete!")

    logging.info("Copying successful videos to output directory...")
    if not is_directory_empty(cosmos_directory):
        copy_to_output_directory(
            cosmos_path=cosmos_directory,
            output_path=output_directory,
            output_statuses=output_statuses,
            output_base_url=output_base_url,
        )
    output_report = create_output_report(output_directory, output_statuses)
    logging.info("Successful videos copied to output directory!")

    logging.info("Creating timing report...")
    mapped_timings: Dict[str, ReportTimingAggregatedTask] = {}
    if cosmos_timing_details is not None:
        service_timing = cosmos_timing_details.get("service")
        if service_timing is not None:
            mapped_timings["cosmos_augmentation.augmentation"] = ReportTimingAggregatedTask(
                count=service_timing.count,
                min_duration_seconds=service_timing.min_duration_seconds,
                max_duration_seconds=service_timing.max_duration_seconds,
                average_duration_seconds=service_timing.average_duration_seconds,
                details=cosmos_timing_details,
            )
    timing_report = create_timing_report(mapped_timings=mapped_timings or None, **context)
    logging.info("Timing report created!")

    logging.info("Writing report to output directory...")
    report = Report(
        output=output_report,
        cosmos=cosmos_report,
        timing=timing_report,
    )
    logging.info("Report: " + report.model_dump_json(indent=2))
    report_url = str(URL(output_base_url) / "output" / "report.json")
    try:
        write_file_to_directory(
            report_url,
            report.model_dump_json(indent=2).encode("utf-8"),
        )
    except Exception as e:
        raise AirflowFailException(f"Failed to write report to output directory: {e}") from e
    logging.info("Report written to output directory at path: " + report_url)

    logging.info("=" * 40)
    logging.info("Report generation complete!")
    logging.info("=" * 40)
