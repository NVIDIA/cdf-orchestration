# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""OSMO Plugin for Airflow."""

from airflow.plugins_manager import AirflowPlugin


class OsmoPlugin(AirflowPlugin):
    """
    Airflow plugin for NVIDIA OSMO workflow orchestration.

    This plugin provides hooks and operators for submitting and managing
    workflows on the OSMO platform from within Airflow DAGs.

    """

    name = "osmo_plugin"
