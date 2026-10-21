# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Manifest YAML Models (for ComponentBuilder)

These models validate the new manifest YAML format used by ComponentBuilder.
This format supports reusable components and deployment profiles.
See: xxxxxxxxxx_k8s_manifest.yaml for example format.
Pools are defined in Helm values (airflow.pools) and created at launch.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator


class DeploymentProfileConfig(BaseModel):
    """Configuration for a deployment profile."""

    operator: str = Field(
        ..., description="Operator class name (e.g., 'K8sServiceOperator', 'K8sTaskOperator')"
    )
    cleanup_operator: Optional[str] = Field(
        default=None,
        description="Cleanup operator class name for endpoint teardown (e.g., 'K8sCleanupOperator'). "
        "If not set, no cleanup task is created for this profile.",
    )
    configuration: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Default operator configuration for all components using this profile "
            "(backend, gpu, instance_type, node_selector, tolerations, health checks, polling, etc.). "
            "Merged with component.configuration; component values override profile defaults."
        ),
    )
    pool: str = Field(
        ...,
        description="Airflow pool name (e.g. h100_1, l40s_8). Pools are created at launch from Helm values. Required for all profiles.",
    )
    pool_slots: Optional[int] = Field(
        default=1,
        description=(
            "Number of pool slots this profile uses per replica/instance (default 1). "
            "Service lifecycle reserves pool_slots × replicas via mapped slot-hold tasks."
        ),
    )


class ComponentConfig(BaseModel):
    """Base configuration for a component (endpoint or task)."""

    enabled: bool = Field(
        default=True,
        description="Whether to enable the component",
    )
    container_image: Optional[str] = Field(
        default=None,
        description="Container image URI (mutually exclusive with helm_chart)",
    )
    helm_chart: Optional[str] = Field(
        default=None,
        description="NVCF Helm chart identifier (mutually exclusive with container_image)",
    )
    helm_chart_service: Optional[str] = Field(
        default=None,
        description="Static Helm chart service name used as the NVCF inference entry point",
    )
    deployment_profile: str = Field(..., description="Name of the deployment profile to use")
    container_args: Optional[str] = Field(
        default=None,
        description="Container arguments/command to run",
    )
    configuration: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional overrides for deployment profile configuration "
            "(health checks, instance counts, node_selector, tolerations, task timeouts, etc.)"
        ),
    )
    secrets: Optional[Dict[str, str]] = Field(
        default=None,
        description="Secrets as key-value pairs (values can be Jinja templates)",
    )
    environment: Optional[Dict[str, str]] = Field(
        default=None,
        description="Environment variables as key-value pairs",
        alias="env",
    )
    volumes: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Kubernetes volumes to mount on K8s task pods. Each entry needs a "
            "name, mountPath, and a volume source (for example emptyDir). "
            "The operator applies these as pod volumes and container volumeMounts."
        ),
    )
    models: Optional[List[str]] = Field(
        default=None,
        description=(
            "NGC models to mount at /config/models/<local_name> on NVCF, "
            "e.g. ['Qwen2.5-14B-Instruct:org/team/model:version']"
        ),
    )
    mount_model_cache: bool = Field(
        default=False,
        description=(
            "Deprecated alias for setting model_cache_pvc on the deployment profile. "
            "Prefer model_cache_pvc on the component or profile configuration."
        ),
    )
    model_cache_pvc: Optional[str] = Field(
        default=None,
        description=(
            "Helm-provisioned model-cache PVC name to mount at /config/models "
            "(e.g. ngc-model-cache). May also be set on the deployment profile "
            "configuration as model_cache_pvc."
        ),
    )
    io_cache_pvc: Optional[str] = Field(
        default=None,
        description=(
            "I/O-cache PVC name to mount at /mnt/ngc-io-cache for MSC POSIX "
            "reads and writes (e.g. ngc-io-cache). May also be set on the "
            "deployment profile configuration as io_cache_pvc."
        ),
    )

    class Config:
        populate_by_name = True  # Allow both 'environment' and 'env' aliases

    @model_validator(mode="after")
    def validate_deployment_artifact(self):
        """Require one supported artifact and reject ambiguous artifact definitions."""
        if self.container_image and self.helm_chart:
            raise ValueError("container_image and helm_chart are mutually exclusive")
        if not self.container_image and not self.helm_chart and not self.models:
            raise ValueError("one of container_image, helm_chart, or models is required")
        if self.helm_chart_service and not self.helm_chart:
            raise ValueError("helm_chart_service requires helm_chart")
        return self


class EndpointComponent(ComponentConfig):
    """Configuration for an endpoint component."""

    @model_validator(mode="after")
    def validate_helm_chart_entry_point(self):
        """Helm endpoint functions require a static service as their entry point."""
        if self.helm_chart and not self.helm_chart_service:
            raise ValueError("helm_chart_service is required when helm_chart is set")
        return self


class TaskComponent(ComponentConfig):
    """Configuration for a task component."""

    max_concurrency: int = Field(
        default=1,
        description=(
            "Maximum mapped instances of this task active in one DAG run. "
            "Defaults to 1. Internal and external augmentation apply this as "
            "max_active_tis_per_dagrun."
        ),
    )

    @model_validator(mode="after")
    def validate_max_concurrency(self):
        """Reject a non-positive task concurrency cap."""
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        return self


class DeploymentComponents(BaseModel):
    """Components section of the deployment manifest."""

    endpoints: Dict[str, EndpointComponent] = Field(
        default_factory=dict,
        description="Dictionary of endpoint component definitions",
    )
    tasks: Dict[str, TaskComponent] = Field(
        default_factory=dict,
        description="Dictionary of task component definitions",
    )


class DeploymentSection(BaseModel):
    """Deployment section of the manifest."""

    dag_timeout: Optional[int] = Field(
        default=43200,
        description=("DAG run timeout in seconds. Used to set Airflow DAG dagrun_timeout."),
    )
    profiles: Dict[str, DeploymentProfileConfig] = Field(
        ...,
        description="Dictionary of deployment profile definitions",
    )
    components: DeploymentComponents = Field(
        ...,
        description="Component definitions (endpoints and tasks)",
    )


class ManifestConfig(BaseModel):
    """
    Top-level model for the deployment manifest YAML format.

    This validates the structure used by ComponentBuilder:

    ```yaml
    version: 0.0.1
    deployment:
      profiles:
        profile_name:
          operator: K8sServiceOperator
          configuration: {...}
      components:
        endpoints:
          component_name:
            container_image: ...
            deployment_profile: profile_name
            ...
        tasks:
          component_name:
            container_image: ...
            deployment_profile: profile_name
            ...
    ```
    """

    version: Optional[str] = Field(
        default=None,
        description="Manifest version (optional, for documentation)",
    )
    deployment: DeploymentSection = Field(
        ...,
        description="Deployment configuration section",
    )

    @model_validator(mode="after")
    def validate_profile_references(self):
        """Validate that all components reference existing profiles."""
        profile_names = set(self.deployment.profiles.keys())
        errors = []

        # Check endpoint profile references
        if self.deployment.components.endpoints:
            for comp_name, comp in self.deployment.components.endpoints.items():
                if comp.deployment_profile not in profile_names:
                    errors.append(
                        f"Endpoint '{comp_name}' references unknown profile '{comp.deployment_profile}'"
                    )

        # Check task profile references
        if self.deployment.components.tasks:
            for comp_name, comp in self.deployment.components.tasks.items():
                if comp.deployment_profile not in profile_names:
                    errors.append(
                        f"Task '{comp_name}' references unknown profile '{comp.deployment_profile}'"
                    )

        if errors:
            raise ValueError(
                "Manifest validation errors:\n" + "\n".join(f"  - {e}" for e in errors)
            )

        return self
