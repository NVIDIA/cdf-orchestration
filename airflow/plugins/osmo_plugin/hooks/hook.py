# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Hook for interacting with the NVIDIA OSMO platform via CLI.

This module provides the OsmoHook class which wraps the ``osmo`` CLI to
submit, monitor, and manage workflows on OSMO from within Airflow DAGs.

The hook shells out to ``osmo workflow …`` commands with ``--format-type json``
and parses the structured output. If OSMO publishes a Python SDK or a
documented REST API in the future, this hook can be swapped to use that
without changing operator or DAG code.

Authentication is handled through the standard OSMO CLI profile
(``osmo profile``) or via environment variables injected at runtime.

Example:
    Basic usage::

        from osmo_plugin.hooks.hook import OsmoHook

        hook = OsmoHook()
        result = hook.submit_workflow("/path/to/workflow.yaml", pool="gpu-pool")
        workflow_id = result["workflow_id"]

        status = hook.query_workflow(workflow_id)
        logs = hook.get_logs(workflow_id, task="train")
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from typing import Any, Dict, List, Optional, Sequence

from airflow.sdk import BaseHook

log = logging.getLogger(__name__)


class OsmoHook(BaseHook):
    """Hook to interact with the NVIDIA OSMO platform.

    Wraps the ``osmo`` CLI binary.  All heavy-lifting methods accept explicit
    parameters and return parsed JSON dicts so operators never need to deal
    with raw subprocess output.

    Attributes:
        TERMINAL_STATES: Workflow statuses that indicate the workflow will
            not transition further.
        FAILED_STATES: Subset of terminal states that indicate failure.
    """

    TERMINAL_STATES = frozenset(
        {
            "COMPLETED",
            "FAILED",
            "FAILED_EXEC_TIMEOUT",
            "FAILED_SERVER_ERROR",
            "FAILED_QUEUE_TIMEOUT",
            "FAILED_SUBMISSION",
            "FAILED_CANCELED",
            "FAILED_BACKEND_ERROR",
            "FAILED_IMAGE_PULL",
            "FAILED_EVICTED",
            "FAILED_START_ERROR",
            "FAILED_START_TIMEOUT",
            "FAILED_PREEMPTED",
        }
    )

    FAILED_STATES = TERMINAL_STATES - {"COMPLETED"}
    _REDACT_ENV_VAR_PATTERNS = (
        re.compile(r"(MULTISTORAGECLIENT_CONFIGURATION=)([^\s]+)"),
        re.compile(r"(MULTISTORAGECLIENT_CONFIGURATION:\s*)([^\s]+)"),
        re.compile(r"(--token\s+)([^\s]+)"),
        re.compile(r"(--token=)([^\s]+)"),
    )

    def __init__(
        self,
        osmo_binary: str = "osmo",
        env_vars: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__()
        self.osmo_binary = osmo_binary
        self.env_vars = env_vars or {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(
        self,
        args: Sequence[str],
        *,
        check: bool = True,
        parse_json: bool = False,
        timeout: Optional[int] = 120,
    ) -> subprocess.CompletedProcess | Dict[str, Any]:
        env = {**os.environ, **self.env_vars}
        cmd = [self.osmo_binary, *args]
        safe_cmd = self._redact_text(" ".join(cmd))
        log.info("Running OSMO CLI: %s", safe_cmd)
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                check=False,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise subprocess.SubprocessError(
                f"OSMO CLI timed out after {timeout}s: {safe_cmd}"
            ) from exc

        if result.returncode != 0:
            log.error("OSMO CLI stderr: %s", self._redact_text(result.stderr))
            log.error("OSMO CLI stdout: %s", self._redact_text(result.stdout))
            if check:
                raise subprocess.CalledProcessError(
                    result.returncode, cmd, result.stdout, result.stderr
                )

        if parse_json:
            return json.loads(result.stdout)

        return result

    @classmethod
    def _redact_text(cls, text: Optional[str]) -> str:
        """Redact known sensitive env var payloads from log text."""
        if text is None:
            return ""
        redacted = text
        for pattern in cls._REDACT_ENV_VAR_PATTERNS:
            redacted = pattern.sub(r"\1***REDACTED***", redacted)
        return redacted

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def login(
        self,
        token: str,
        url: str,
    ) -> None:
        """Authenticate with OSMO using a Personal Access Token.

        Runs ``osmo login --method token --token <token> <url>`` so that
        subsequent CLI calls in the same environment are authenticated.
        """
        log.info("Authenticating with OSMO at %s", url)
        self._run(
            ["login", "--method", "token", "--token", token, url],
            timeout=30,
        )
        log.info("OSMO authentication successful")

    # ------------------------------------------------------------------
    # Workflow lifecycle
    # ------------------------------------------------------------------

    def submit_workflow(
        self,
        workflow_file: str,
        *,
        pool: Optional[str] = None,
        set_values: Optional[Dict[str, str]] = None,
        set_string_values: Optional[Dict[str, str]] = None,
        set_env: Optional[Dict[str, str]] = None,
        priority: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        args: List[str] = ["workflow", "submit", workflow_file, "--format-type", "json"]

        if pool:
            args += ["--pool", pool]
        if priority:
            args += ["--priority", priority]
        if dry_run:
            args.append("--dry-run")

        for k, v in (set_values or {}).items():
            args += ["--set", f"{k}={v}"]
        for k, v in (set_string_values or {}).items():
            args += ["--set-string", f"{k}={v}"]
        for k, v in (set_env or {}).items():
            args += ["--set-env", f"{k}={v}"]

        result = self._run(args)
        stdout = result.stdout

        try:
            return json.loads(stdout)
        except (json.JSONDecodeError, TypeError):
            pass

        # Fall back to plaintext parsing when JSON is unavailable
        workflow_id = self.parse_workflow_id(stdout)
        if workflow_id:
            return {"workflow_id": workflow_id, "_raw": stdout}

        log.warning("Could not parse submit output as JSON or text: %s", stdout)
        return {"_raw": stdout}

    def query_workflow(self, workflow_id: str) -> Dict[str, Any]:
        result = self._run(
            ["workflow", "query", workflow_id, "--format-type", "json"],
        )
        try:
            return json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError):
            pass

        # Fall back to plaintext: extract status from lines like "Status      : COMPLETED"
        parsed: Dict[str, Any] = {"_raw": result.stdout}
        for line in result.stdout.splitlines():
            if ":" in line:
                key, _, value = line.partition(":")
                key = key.strip().lower().replace(" ", "_")
                if key:
                    parsed[key] = value.strip()
        return parsed

    def get_workflow_status(self, workflow_id: str) -> str:
        info = self.query_workflow(workflow_id)
        return str(info.get("status", info.get("state", "UNKNOWN"))).upper()

    def is_terminal(self, status: str) -> bool:
        return status in self.TERMINAL_STATES

    def is_failed(self, status: str) -> bool:
        return status in self.FAILED_STATES

    def get_logs(
        self,
        workflow_id: str,
        *,
        task: Optional[str] = None,
        last_n_lines: Optional[int] = None,
        error: bool = False,
    ) -> str:
        args: List[str] = ["workflow", "logs", workflow_id]

        if task:
            args += ["--task", task]
        if last_n_lines is not None:
            args += ["-n", str(last_n_lines)]
        if error:
            args.append("--error")

        result = self._run(args, check=False)
        return result.stdout

    def cancel_workflow(
        self,
        workflow_id: str,
        *,
        message: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        args: List[str] = ["workflow", "cancel", workflow_id, "--format-type", "json"]

        if message:
            args += ["--message", message]
        if force:
            args.append("--force")

        result = self._run(args)
        try:
            return json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError):
            return {"_raw": result.stdout}

    def list_workflows(
        self,
        *,
        count: Optional[int] = None,
        status: Optional[Sequence[str]] = None,
        pool: Optional[Sequence[str]] = None,
        name: Optional[str] = None,
        all_users: bool = False,
    ) -> Dict[str, Any]:
        args: List[str] = ["workflow", "list", "--format-type", "json"]

        if count is not None:
            args += ["--count", str(count)]
        if name:
            args += ["--name", name]
        if all_users:
            args.append("--all-users")
        for s in status or []:
            args += ["--status", s]
        for p in pool or []:
            args += ["--pool", p]

        result = self._run(args)
        try:
            return json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError):
            return {"_raw": result.stdout}

    def validate_workflow(
        self,
        workflow_file: str,
        *,
        pool: Optional[str] = None,
        set_values: Optional[Dict[str, str]] = None,
    ) -> subprocess.CompletedProcess:
        args: List[str] = ["workflow", "validate", workflow_file]

        if pool:
            args += ["--pool", pool]
        for k, v in (set_values or {}).items():
            args += ["--set", f"{k}={v}"]

        return self._run(args)

    # ------------------------------------------------------------------
    # Utility: parse workflow ID from text output
    # ------------------------------------------------------------------

    @staticmethod
    def parse_workflow_id(text: str) -> Optional[str]:
        match = re.search(r"Workflow ID\s*[-–—:]\s*(\S+)", text)
        return match.group(1) if match else None
