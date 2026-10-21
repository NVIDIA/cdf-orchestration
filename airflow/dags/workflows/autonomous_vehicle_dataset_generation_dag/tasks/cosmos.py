# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Managed Cosmos3-Nano generation tasks for RGB and WSM controls in the AV workflow."""

import datetime

from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule
from dags.shared.task_groups.cosmos import CosmosTaskGroup


class AVCosmosTaskGroup(CosmosTaskGroup):
    """Generate RGB or WSM-conditioned variants through the managed Cosmos3 service."""

    def get_cosmos_augmentation_task_group(self) -> TaskGroup:
        with TaskGroup(group_id=self.group_id) as task_group:
            configs = self.get_generate_cosmos_configs_task()
            augmentation = self.builder.partial_task(
                component_name="augmentation",
                task_id="augmentation",
                pool=self.DEFAULT_POOL,
                name="cosmos-augmentation-pipeline-{{ run_id | replace(':', '_') "
                "| replace('+', '_') | replace('.', '_') }}",
                retry_delay=datetime.timedelta(seconds=15),
                max_active_tis_per_dagrun=self._max_concurrency_from_manifest(),
            ).expand(container_args=XComArg(configs))
            validation = self.get_validate_cosmos_outputs_task()
            validation.trigger_rule = TriggerRule.ALL_SUCCESS
            # Preserve validation/reporting after every mapped attempt, including failures.
            join_after_augmentation = EmptyOperator(
                task_id="join_after_augmentation", trigger_rule=TriggerRule.ALL_DONE
            )
            configs >> augmentation >> join_after_augmentation >> validation
            configs >> validation
        return task_group
