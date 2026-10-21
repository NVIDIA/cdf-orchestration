# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Operator that runs a single container task on OSMO.

This operator mirrors the interface of ``NVCFTaskOperator`` so it can be
used interchangeably through the :class:`ComponentBuilder` factory.  Instead
of calling the NVCF API, it dynamically generates a minimal single-task OSMO
workflow YAML and submits it via the :class:`OsmoHook`.

The OSMO platform then schedules the container on the requested resource pool
(GPU type, CPU, memory, etc.) and the operator polls for completion, streams
logs, and pushes XCom results identically to ``NVCFTaskOperator``.
"""

from __future__ import annotations

import inspect
import logging
import os
import re
import shlex
import subprocess
import tempfile
import time
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence

import yaml
from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

from osmo_plugin.hooks.hook import OsmoHook

_log = logging.getLogger(__name__)

_BASE_OPERATOR_PARAMS: frozenset[str] = frozenset(
    inspect.signature(BaseOperator.__init__).parameters.keys()
) - {"self"}


class OsmoTaskOperator(BaseOperator):
    """Run a single container as an OSMO workflow task.

    Parameters that are OSMO-specific (``osmo_pool``, ``osmo_gpu``, etc.)
    configure the resource request in the generated YAML.  Any kwargs
    that are not recognised by ``BaseOperator`` are silently dropped so
    the :class:`ComponentBuilder` can pass the same merged config to both
    NVCF and OSMO operators without error.

    XCom keys pushed:
        * ``workflow_id`` – the submitted OSMO workflow identifier
        * ``status`` – terminal status string (e.g. ``COMPLETED``, ``FAILED``)
    """

    template_fields: Sequence[str] = (
        "name",
        "container_image",
        "container_command",
        "container_args",
        "container_environment_variables",
        "runtime_environment_variables",
        "secrets",
        "osmo_pool",
        "osmo_priority",
        "osmo_inputs",
        "osmo_token",
    )

    def __init__(
        self,
        *,
        container_image: str,
        container_command: Optional[str] = None,
        container_args: Optional[str] = None,
        container_environment_variables: Optional[List[str]] = None,
        runtime_environment_variables: Optional[List[str]] = None,
        secrets: Optional[List[str]] = None,
        osmo_pool: Optional[str] = None,
        osmo_priority: Optional[str] = None,
        osmo_gpu: Optional[int] = 1,
        osmo_cpu: Optional[int] = 11,
        osmo_memory: Optional[str] = "100Gi",
        osmo_storage: Optional[str] = "100Gi",
        osmo_inputs: Optional[List[Dict[str, str]]] = None,
        poll_interval_seconds: int = 30,
        max_runtime_duration_iso: Optional[str] = None,
        osmo_binary: str = "osmo",
        osmo_env_vars: Optional[Dict[str, str]] = None,
        osmo_token: Optional[str] = None,
        osmo_url: str = "https://us-west-2-aws.osmo.nvidia.com",
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        base_kwargs = {k: v for k, v in kwargs.items() if k in _BASE_OPERATOR_PARAMS}
        dropped = set(kwargs) - set(base_kwargs)
        if dropped:
            _log.debug("OsmoTaskOperator: ignoring unknown kwargs: %s", dropped)

        if max_runtime_duration_iso and "execution_timeout" not in base_kwargs:
            base_kwargs["execution_timeout"] = self._parse_iso_duration(max_runtime_duration_iso)

        super().__init__(**base_kwargs)

        self.container_image = container_image
        self.container_command = container_command
        self.container_args = container_args
        self.container_environment_variables = container_environment_variables or []
        self.runtime_environment_variables = runtime_environment_variables or []
        self.secrets = secrets or []
        self.osmo_pool = osmo_pool
        self.osmo_priority = osmo_priority
        self.osmo_gpu = osmo_gpu
        self.osmo_cpu = osmo_cpu
        self.osmo_memory = osmo_memory
        self.osmo_storage = osmo_storage
        self.osmo_inputs = osmo_inputs
        self.poll_interval_seconds = poll_interval_seconds
        self.osmo_binary = osmo_binary
        self.osmo_env_vars = osmo_env_vars
        self.osmo_token = osmo_token
        self.osmo_url = osmo_url
        self.name = name or (base_kwargs.get("task_id") or "osmo-task")

        self._hook: Optional[OsmoHook] = None
        self._workflow_id: Optional[str] = None
        self._seen_log_lines: int = 0
        self._tmp_workflow_file: Optional[str] = None

    # ------------------------------------------------------------------
    # Hook access
    # ------------------------------------------------------------------

    def _get_hook(self) -> OsmoHook:
        if self._hook is None:
            self._hook = OsmoHook(
                osmo_binary=self.osmo_binary,
                env_vars=self.osmo_env_vars,
            )
        return self._hook

    # ------------------------------------------------------------------
    # YAML generation
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_iso_duration(value: str) -> timedelta:
        """Convert an ISO 8601 duration like ``PT2H`` or ``PT30M`` to a timedelta."""
        match = re.match(
            r"^P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$",
            value,
        )
        if not match:
            raise ValueError(f"Cannot parse ISO duration: {value!r}")
        days = int(match.group(1) or 0)
        hours = int(match.group(2) or 0)
        minutes = int(match.group(3) or 0)
        seconds = int(match.group(4) or 0)
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """Produce a DNS-safe name from an arbitrary string."""
        s = re.sub(r"[^a-z0-9-]", "-", name.lower())
        s = re.sub(r"-+", "-", s).strip("-")
        return s[:63].strip("-") or "task"

    @staticmethod
    def _parse_kv_list(items: List[str]) -> Dict[str, str]:
        """Convert ``["KEY:VALUE", ...]`` to ``{"KEY": "VALUE"}``."""
        result: Dict[str, str] = {}
        for item in items:
            key, _, value = item.partition(":")
            result[key] = value
        return result

    def _build_workflow_dict(self) -> Dict[str, Any]:
        """Build the OSMO workflow YAML as a Python dict.

        Produces the structure expected by the OSMO CLI::

            workflow:
              name: <safe-name>
              resources:
                default: {gpu: 1, cpu: 11, memory: 100Gi, storage: 100Gi}
              tasks:
              - name: <safe-name>
                image: <container_image>
                command: [...]
                args: [...]
                environment: {KEY: value}
        """
        safe_name = self._sanitize_name(self.name)

        task: Dict[str, Any] = {
            "name": safe_name,
            "image": self.container_image,
        }

        if self.container_command:
            task["command"] = shlex.split(self.container_command)

        if self.container_args:
            parts = shlex.split(self.container_args)
            if parts and not self.container_command and not parts[0].startswith("-"):
                task["command"] = [parts[0]]
                parts = parts[1:]
            if parts:
                task["args"] = parts

        env = self._parse_kv_list(self.container_environment_variables)
        if self.runtime_environment_variables:
            env = {**env, **self._parse_kv_list(self.runtime_environment_variables)}
        if self.secrets:
            secret_env = self._parse_kv_list(self.secrets)
            env = {**env, **secret_env}
        if env:
            task["environment"] = env

        if self.osmo_inputs:
            task["inputs"] = self.osmo_inputs

        return {
            "workflow": {
                "name": safe_name,
                "resources": {
                    "default": {
                        "gpu": 1 if self.osmo_gpu is None else self.osmo_gpu,
                        "cpu": self.osmo_cpu or 11,
                        "memory": self.osmo_memory or "100Gi",
                        "storage": self.osmo_storage or "100Gi",
                    },
                },
                "tasks": [task],
            },
        }

    @staticmethod
    def _redact_workflow_for_logging(workflow_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of workflow dict with sensitive env values redacted."""
        redacted = yaml.safe_load(yaml.safe_dump(workflow_dict))
        tasks = redacted.get("workflow", {}).get("tasks", [])
        sensitive_markers = ("token", "secret", "password", "api_key", "apikey", "key")

        for task in tasks:
            env = task.get("environment")
            if not isinstance(env, dict):
                continue
            for k in list(env.keys()):
                key_l = str(k).lower()
                if key_l in OsmoTaskOperator._ALWAYS_REDACT_ENV_KEYS or any(
                    marker in key_l for marker in sensitive_markers
                ):
                    env[k] = "***REDACTED***"
        return redacted

    def _write_workflow_file(self, workflow_dict: Dict[str, Any]) -> str:
        """Write workflow dict to a temporary YAML file and return its path."""
        fd, path = tempfile.mkstemp(suffix=".yaml", prefix="osmo_task_")
        try:
            fileobj = os.fdopen(fd, "w")
        except Exception:
            os.close(fd)
            raise
        with fileobj:
            yaml.dump(workflow_dict, fileobj, default_flow_style=False)
        return path

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        hook = self._get_hook()

        if self.osmo_token:
            hook.login(self.osmo_token, self.osmo_url)

        workflow_dict = self._build_workflow_dict()
        redacted_workflow_dict = self._redact_workflow_for_logging(workflow_dict)
        self.log.info(
            "Generated OSMO workflow configuration:\n%s",
            yaml.safe_dump(redacted_workflow_dict, sort_keys=False),
        )
        self._tmp_workflow_file = self._write_workflow_file(workflow_dict)

        self.log.info(
            "Submitting OSMO single-task workflow (image=%s, pool=%s)",
            self.container_image,
            self.osmo_pool,
        )

        try:
            submit_resp = hook.submit_workflow(
                self._tmp_workflow_file,
                pool=self.osmo_pool,
                priority=self.osmo_priority,
            )
        except Exception:
            self._cleanup_tmp()
            raise

        workflow_id = self._extract_workflow_id(submit_resp)
        if not workflow_id:
            self._cleanup_tmp()
            raise AirflowException(
                f"Could not determine workflow_id from submit response: {submit_resp}"
            )

        self._workflow_id = workflow_id
        dashboard_url = f"{self.osmo_url}/workflows/{workflow_id}"
        self.log.info("OSMO workflow submitted: %s", workflow_id)
        self.log.info("OSMO dashboard: %s", dashboard_url)

        status = self._poll_until_terminal(hook, workflow_id)
        self._stream_logs(hook, workflow_id, label="final")
        self._cleanup_tmp()

        ti = context["ti"]
        ti.xcom_push(key="workflow_id", value=workflow_id)
        ti.xcom_push(key="status", value=status)
        ti.xcom_push(key="dashboard_url", value=dashboard_url)

        if hook.is_failed(status):
            raise AirflowException(f"OSMO workflow {workflow_id} ended with status {status}")

        self.log.info("OSMO workflow %s completed: %s", workflow_id, status)
        return {"workflow_id": workflow_id, "status": status}

    def on_kill(self) -> None:
        if not self._workflow_id:
            return
        self.log.info("Cancelling OSMO workflow %s", self._workflow_id)
        try:
            hook = self._get_hook()
            status = hook.get_workflow_status(self._workflow_id)
            if not hook.is_terminal(status):
                hook.cancel_workflow(
                    self._workflow_id,
                    message="Cancelled by Airflow on_kill",
                    force=True,
                )
        except Exception as exc:
            self.log.warning("Failed to cancel OSMO workflow %s: %s", self._workflow_id, exc)
        finally:
            self._cleanup_tmp()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    _TRANSIENT_ERRORS = (OSError, TimeoutError, subprocess.SubprocessError)
    _MAX_POLL_RETRIES = 5
    _POLL_RETRY_BACKOFF_BASE = 10
    _POLL_RETRY_BACKOFF_CAP = 120
    _ALWAYS_REDACT_ENV_KEYS = frozenset({"multistorageclient_configuration"})

    def _poll_until_terminal(self, hook: OsmoHook, workflow_id: str) -> str:
        consecutive_errors = 0
        while True:
            try:
                status = hook.get_workflow_status(workflow_id)
                consecutive_errors = 0
            except self._TRANSIENT_ERRORS as exc:
                consecutive_errors += 1
                if consecutive_errors > self._MAX_POLL_RETRIES:
                    raise AirflowException(
                        f"OSMO workflow {workflow_id}: gave up after "
                        f"{self._MAX_POLL_RETRIES} consecutive poll failures"
                    ) from exc
                backoff = min(
                    self._POLL_RETRY_BACKOFF_BASE * (2 ** (consecutive_errors - 1)),
                    self._POLL_RETRY_BACKOFF_CAP,
                )
                self.log.warning(
                    "OSMO %s: transient poll error (%s/%s), retrying in %ds: %s",
                    workflow_id,
                    consecutive_errors,
                    self._MAX_POLL_RETRIES,
                    backoff,
                    exc,
                )
                time.sleep(backoff)
                continue

            self.log.info("OSMO %s status: %s", workflow_id, status)

            if status == "RUNNING":
                self._stream_logs(hook, workflow_id, label="progress")

            if hook.is_terminal(status):
                return status

            time.sleep(self.poll_interval_seconds)

    def _stream_logs(self, hook: OsmoHook, workflow_id: str, *, label: str = "") -> None:
        try:
            log_text = hook.get_logs(workflow_id)
            if not log_text or not log_text.strip():
                return
            all_lines = log_text.strip().splitlines()
            new_lines = all_lines[self._seen_log_lines :]
            for line in new_lines:
                self.log.info("[OSMO %s] %s", label, line)
            self._seen_log_lines = len(all_lines)
        except Exception as exc:
            self.log.debug("Unable to fetch OSMO logs: %s", exc)

    def _cleanup_tmp(self) -> None:
        if self._tmp_workflow_file and os.path.exists(self._tmp_workflow_file):
            try:
                os.unlink(self._tmp_workflow_file)
            except OSError:
                pass
            self._tmp_workflow_file = None

    @staticmethod
    def _extract_workflow_id(resp: Dict[str, Any]) -> Optional[str]:
        for key in ("workflow_id", "workflowId", "id", "name"):
            if key in resp:
                return str(resp[key])
        return None
