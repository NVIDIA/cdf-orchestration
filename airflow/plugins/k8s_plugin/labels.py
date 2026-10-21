# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""DAG-run labels and best-effort cleanup for Kubernetes resources."""

from __future__ import annotations

import hashlib
import os
import re
from typing import Any, Dict, Iterable, Mapping, Optional

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes import client
from kubernetes.client.rest import ApiException

from k8s_plugin.connection import DEFAULT_KUBERNETES_CONN_ID, resolve_kubernetes_backend

DAG_RUN_ID_LABEL = "airflow-run-id"
DAG_ID_LABEL = "airflow-dag-id"
TASK_ID_LABEL = "airflow-task-id"
CREATED_BY_LABEL = "created-by"
CREATED_BY_VALUE = "k8s-plugin"
DEFAULT_CLEANUP_NAMESPACES = ("sdg-workflow", "default")
CLEANUP_NAMESPACES_ENV_VAR = "K8S_RESOURCE_CLEANUP_NAMESPACES"

_LABEL_VALUE_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]")


def sanitize_k8s_label_value(value: str) -> str:
    """Return a Kubernetes-safe label value derived from ``value``.

    Label values must be 63 characters or fewer, start and end with an
    alphanumeric character, and contain only ``[A-Za-z0-9._-]``. Airflow
    ``run_id`` values often include ``:`` and ``+``, which are invalid.
    """
    if not value:
        return "unknown"
    sanitized = _LABEL_VALUE_SAFE_RE.sub("-", value).strip("-._")
    if not sanitized:
        sanitized = "unknown"
    if len(sanitized) > 63:
        return hashlib.sha256(value.encode()).hexdigest()[:63]
    if not sanitized[0].isalnum():
        sanitized = f"x{sanitized[1:]}"
    if not sanitized[-1].isalnum():
        sanitized = f"{sanitized[:-1]}x"
    return sanitized


def dag_run_resource_labels(
    run_id: str,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> Dict[str, str]:
    """Labels applied to every Kubernetes resource created by the k8s plugin."""
    labels = {
        CREATED_BY_LABEL: CREATED_BY_VALUE,
        DAG_RUN_ID_LABEL: sanitize_k8s_label_value(run_id),
    }
    if dag_id:
        labels[DAG_ID_LABEL] = sanitize_k8s_label_value(dag_id)
    if task_id:
        labels[TASK_ID_LABEL] = sanitize_k8s_label_value(task_id)
    return labels


def k8s_plugin_resource_labels(
    context: Mapping[str, Any],
    *,
    task_id: Optional[str] = None,
) -> Dict[str, str]:
    """Build plugin labels from an Airflow task context."""
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    ti = context.get("ti")
    resolved_task_id = task_id or getattr(ti, "task_id", None)
    return dag_run_resource_labels(
        run_id=getattr(dag_run, "run_id", None) or "",
        dag_id=getattr(dag, "dag_id", None),
        task_id=resolved_task_id,
    )


def apply_metadata_labels(metadata: Any, labels: Mapping[str, str]) -> None:
    """Merge ``labels`` into a Kubernetes object metadata.labels mapping."""
    if metadata is None:
        return
    merged = dict(metadata.labels or {})
    merged.update(labels)
    metadata.labels = merged


def cleanup_namespaces() -> tuple[str, ...]:
    raw = os.environ.get(CLEANUP_NAMESPACES_ENV_VAR, "")
    if raw.strip():
        return tuple(ns.strip() for ns in raw.split(",") if ns.strip())
    return DEFAULT_CLEANUP_NAMESPACES


def cleanup_label_selector(
    *,
    run_id: Optional[str] = None,
    all_plugin_resources: bool = False,
) -> Optional[str]:
    """Return the Kubernetes label selector for a cleanup sweep."""
    if all_plugin_resources:
        return f"{CREATED_BY_LABEL}={CREATED_BY_VALUE}"
    if run_id:
        return f"{DAG_RUN_ID_LABEL}={sanitize_k8s_label_value(run_id)}"
    return None


def cleanup_k8s_resources_for_dag_run(
    context: Mapping[str, Any] | None = None,
    *,
    run_id: Optional[str] = None,
    all_plugin_resources: bool = False,
    kubernetes_conn_id: Optional[str] = None,
    cluster_context: Optional[str] = None,
    in_cluster: Optional[bool] = None,
    namespaces: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Delete Pods, Deployments, and Services created by the k8s plugin.

    When ``all_plugin_resources`` is true, every resource labeled
    ``created-by=k8s-plugin`` is deleted. Otherwise ``run_id`` selects
    ``airflow-run-id``. When ``run_id`` is omitted, ``context["dag_run"].run_id``
    is used. The cleanup DAG must pass the *source* run id; its own run id is a
    different label.
    """
    if not all_plugin_resources and not run_id:
        dag_run = (context or {}).get("dag_run")
        run_id = getattr(dag_run, "run_id", None) if dag_run is not None else None
    selector = cleanup_label_selector(run_id=run_id, all_plugin_resources=all_plugin_resources)
    if not selector:
        return {"skipped": True, "reason": "missing_run_id"}
    ns_list = tuple(namespaces) if namespaces is not None else cleanup_namespaces()
    conn_id, resolved_context = resolve_kubernetes_backend(
        context=cluster_context,
        kubernetes_conn_id=kubernetes_conn_id or DEFAULT_KUBERNETES_CONN_ID,
        in_cluster=in_cluster,
    )
    hook_kwargs: Dict[str, Any] = {}
    if conn_id:
        hook_kwargs["conn_id"] = conn_id
    if resolved_context:
        hook_kwargs["cluster_context"] = resolved_context
    if in_cluster is not None:
        hook_kwargs["in_cluster"] = in_cluster

    try:
        hook = KubernetesHook(**hook_kwargs)
        api_client = hook.get_conn()
    except Exception as e:
        return {"skipped": True, "reason": f"kubernetes_hook_failed:{e}"}

    apps_v1 = client.AppsV1Api(api_client)
    core_v1 = client.CoreV1Api(api_client)
    results: Dict[str, Any] = {
        "selector": selector,
        "deployments_deleted": [],
        "services_deleted": [],
        "pods_deleted": [],
        "errors": [],
    }

    for namespace in ns_list:
        _delete_labeled(
            list_fn=lambda ns=namespace: apps_v1.list_namespaced_deployment(
                namespace=ns, label_selector=selector
            ),
            delete_fn=lambda name, ns: apps_v1.delete_namespaced_deployment(
                name=name, namespace=ns, propagation_policy="Background"
            ),
            kind="Deployment",
            results_key="deployments_deleted",
            results=results,
        )
        _delete_labeled(
            list_fn=lambda ns=namespace: core_v1.list_namespaced_service(
                namespace=ns, label_selector=selector
            ),
            delete_fn=lambda name, ns: core_v1.delete_namespaced_service(name=name, namespace=ns),
            kind="Service",
            results_key="services_deleted",
            results=results,
        )
        _delete_labeled(
            list_fn=lambda ns=namespace: core_v1.list_namespaced_pod(
                namespace=ns, label_selector=selector
            ),
            delete_fn=lambda name, ns: core_v1.delete_namespaced_pod(name=name, namespace=ns),
            kind="Pod",
            results_key="pods_deleted",
            results=results,
        )

    return results


def cleanup_k8s_resources_best_effort(
    context: Mapping[str, Any] | None = None,
    *,
    run_id: Optional[str] = None,
    all_plugin_resources: bool = False,
    namespaces: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Delete labeled plugin resources on every reachable Kubernetes backend.

    Tries the in-cluster API first, then ``kubernetes_remote``. A backend that
    cannot be connected (missing conn, bad kubeconfig) is skipped. List/delete
    errors on a connected backend are recorded and do not stop the other sweep.
    """
    attempts = (
        ("in_cluster", {"in_cluster": True, "kubernetes_conn_id": None}),
        (
            "remote",
            {"in_cluster": False, "kubernetes_conn_id": DEFAULT_KUBERNETES_CONN_ID},
        ),
    )
    combined: Dict[str, Any] = {
        "selector": None,
        "deployments_deleted": [],
        "services_deleted": [],
        "pods_deleted": [],
        "errors": [],
        "backends": {},
        "connected": [],
        "skipped_backends": [],
    }
    for name, backend_kwargs in attempts:
        result = cleanup_k8s_resources_for_dag_run(
            context,
            run_id=run_id,
            all_plugin_resources=all_plugin_resources,
            namespaces=namespaces,
            **backend_kwargs,
        )
        combined["backends"][name] = result
        if result.get("skipped"):
            reason = str(result.get("reason") or "unknown")
            if reason == "missing_run_id":
                return result
            combined["skipped_backends"].append({"name": name, "reason": reason})
            continue
        combined["connected"].append(name)
        combined["selector"] = result.get("selector")
        for key in ("deployments_deleted", "services_deleted", "pods_deleted"):
            combined[key].extend(f"{name}:{item}" for item in (result.get(key) or []))
        for err in result.get("errors") or []:
            combined["errors"].append(f"{name}: {err}")
    if not combined["connected"]:
        reasons = "; ".join(
            f"{item['name']}:{item['reason']}" for item in combined["skipped_backends"]
        )
        return {
            "skipped": True,
            "reason": f"no_reachable_cluster:{reasons}",
            "backends": combined["backends"],
            "skipped_backends": combined["skipped_backends"],
        }
    return combined


def _delete_labeled(
    *,
    list_fn: Any,
    delete_fn: Any,
    kind: str,
    results_key: str,
    results: Dict[str, Any],
) -> None:
    try:
        listed = list_fn()
    except ApiException as e:
        results["errors"].append(f"list {kind}s failed: {e}")
        return
    except Exception as e:
        results["errors"].append(f"list {kind}s failed: {e}")
        return

    for item in getattr(listed, "items", None) or []:
        metadata = getattr(item, "metadata", None)
        if metadata is None or not metadata.name or not metadata.namespace:
            continue
        name = metadata.name
        namespace = metadata.namespace
        try:
            delete_fn(name, namespace)
            results[results_key].append(f"{namespace}/{name}")
        except ApiException as e:
            if e.status == 404:
                continue
            results["errors"].append(f"delete {kind} {namespace}/{name} failed: {e}")
        except Exception as e:
            results["errors"].append(f"delete {kind} {namespace}/{name} failed: {e}")
