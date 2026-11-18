# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Run configuration (DAG conf ``payload``) for aoi_anomalygen_dag."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# run_pipeline keys the DAG sets itself; a payload may not override them.
RESERVED_PIPELINE_KEYS = frozenset(
    {
        "input",
        "output",
        "recipe",
        "checkpoint",
        "stages",
        "staging",
        "mode",
        "num_sdg",
        "per_defect_counts",
        "output_naming",
        "amp_per_clean_pairing",
        "base_seed",
        "num_gpus",
        "amg_model_registry",
    }
)

ASTRA_MODEL = "azure/openai/gpt-6-astra"
ASTRA_ENDPOINT = {"model": ASTRA_MODEL, "base_url": "https://inference-api.nvidia.com/v1"}

# AnomalyGen's cloud PCB templates (ag_config/pipeline/templates/pcb_day1_*.yaml): GPT-6 Astra as
# proposer and judge through a model registry, no beam options. guardrail stays off: it needs the
# gated nvidia/Cosmos-Guardrail1 weights.
DEFAULT_PIPELINE_OPTIONS: dict[str, Any] = {
    "amg_gen_model": ASTRA_MODEL,
    "amg_judge_model": ASTRA_MODEL,
    "guardrail": False,
}
# Local alternative: {"amg_serve": "gemma-4-31b-it", "amg_beam_multi": 4, "amg_beam_pairwise": true,
# "amg_exemplar_captions": true, "amg_exemplar_visual_prompt": true, "amg_coverage_note": true}
# starts Gemma inside each chunk pod (2 GPUs) and ignores amg_endpoints.


class AmgEndpoint(BaseModel):
    """An OpenAI-compatible VLM endpoint. The key is never in conf: the pod gets it from the
    Airflow Variable vlm_api_key (Secret model-api-keys-secret, VLM_API_KEY in secrets.env)."""

    model_config = ConfigDict(extra="forbid")
    model: str = Field(min_length=1)
    base_url: str = Field(pattern=r"^https?://\S+$")


_STORAGE_PREFIXES = ("s3://", "msc://")
_ARTIFACT_PREFIXES = ("s3://", "msc://", "hf://", "ngc://")


class AoiAnomalyGenPayload(BaseModel):
    """One AnomalyGen round over a set of clean images, split into chunks of ``chunk_size``."""

    model_config = ConfigDict(extra="forbid")

    campaign_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,62}$")
    round: int = Field(default=1, ge=1, le=999)
    dataset: str = Field(
        description="AnomalyGen dataset prefix: defect_spec.jsonl, <class>/mask/<defect>/, "
        "<class>/anomaly_image/<defect>/ (reference defects used by amg and evaluate)."
    )
    clean_images: str = Field(
        description="Prefix holding the work items: <class>/clean_image/<file> and "
        "<class>/cad_mask/<stem>.png."
    )
    images: list[str] | None = Field(
        default=None,
        description="Subset to run, as <class>/clean_image/<file> relative to clean_images. "
        "Omit to run every clean image under clean_images.",
    )
    defects_by_class: dict[str, list[str]] = Field(
        min_length=1,
        description="Defect types per class, e.g. {'IC': ['IC+bridge']}; a class with several "
        "types gets them round-robin over its sorted images.",
    )
    image_defects: dict[str, str] = Field(
        default_factory=dict,
        description="Explicit defect type for given images (overrides the round-robin).",
    )
    chunk_size: int = Field(default=10, ge=1, le=50)
    outputs_per_image: int = Field(
        default=1,
        ge=1,
        le=5,
        description="Defect images requested per clean image (per_defect_counts = images x this). "
        "amg launches beams on the least-used clean image first but one beam can return up to 3 "
        "accepted masks, so 1 output per image often covers only a third of the images; 3 gives "
        "about one beam per image and the final trim keeps masks round-robin across images.",
    )
    output_root: str
    recipe: str
    checkpoint: str
    evaluate: bool = True
    require_full_coverage: bool = Field(
        default=False,
        description="Fail the run when a clean image gets no defect image. Off by default: amg "
        "does not promise one output per clean image, and uncovered images are listed in "
        "run_report.json as the next round's input.",
    )
    amg_endpoints: list[AmgEndpoint] = Field(
        default_factory=lambda: [AmgEndpoint(**ASTRA_ENDPOINT)],
        description="Cloud VLM endpoints for amg (model + base_url, version segment included). "
        "Used unless anomaly_generation sets amg_serve (local Gemma).",
    )
    anomaly_generation: dict[str, Any] = Field(
        default_factory=dict,
        validate_default=True,
        description="anomalygen run_pipeline keys merged over the defaults (GPT-6 Astra as "
        "amg_gen_model and amg_judge_model, guardrail false). Set amg_serve to run a local model "
        "in the pod instead.",
    )

    @field_validator("dataset", "clean_images", "output_root")
    @classmethod
    def _storage_prefix(cls, value: str) -> str:
        if not value.startswith(_STORAGE_PREFIXES):
            raise ValueError(f"must start with one of {_STORAGE_PREFIXES}: {value}")
        return value.rstrip("/") + "/"

    @field_validator("recipe", "checkpoint")
    @classmethod
    def _artifact_url(cls, value: str) -> str:
        if not value.startswith(_ARTIFACT_PREFIXES) or value.endswith("/"):
            raise ValueError(f"must be one file under one of {_ARTIFACT_PREFIXES}: {value}")
        return value

    @field_validator("images")
    @classmethod
    def _image_paths(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("images must not be empty; omit it to run every clean image")
        for rel in value:
            parts = rel.split("/")
            if len(parts) != 3 or parts[1] != "clean_image" or not parts[0] or not parts[2]:
                raise ValueError(f"image {rel!r} must be <class>/clean_image/<file>")
        if len(set(value)) != len(value):
            raise ValueError("images lists a file more than once")
        return value

    @field_validator("anomaly_generation")
    @classmethod
    def _pipeline_options(cls, value: dict[str, Any]) -> dict[str, Any]:
        reserved = sorted(RESERVED_PIPELINE_KEYS & set(value))
        if reserved:
            raise ValueError(f"anomaly_generation may not set {reserved}; the DAG sets them")
        merged = {**DEFAULT_PIPELINE_OPTIONS, **value}
        if merged.get("amg_serve"):
            # amg_serve names the model itself; drop the cloud defaults the caller did not set.
            for key in ("amg_gen_model", "amg_judge_model"):
                if key not in value:
                    merged.pop(key)
        return merged

    @model_validator(mode="after")
    def _defect_types(self) -> "AoiAnomalyGenPayload":
        for cls_name, types in self.defects_by_class.items():
            if not types:
                raise ValueError(f"defects_by_class[{cls_name!r}] is empty")
            for defect_type in types:
                if defect_type.split("+", 1)[0] != cls_name or "+" not in defect_type:
                    raise ValueError(
                        f"defect type {defect_type!r} must be '{cls_name}+<defect>' "
                        f"to apply to class {cls_name!r}"
                    )
        for rel, defect_type in self.image_defects.items():
            parts = rel.split("/")
            if len(parts) != 3 or parts[1] != "clean_image" or not parts[0] or not parts[2]:
                raise ValueError(f"image_defects key {rel!r} must be <class>/clean_image/<file>")
            cls_name = parts[0]
            if "+" not in defect_type or defect_type.split("+", 1)[0] != cls_name or not defect_type.split("+", 1)[1]:
                raise ValueError(f"image_defects[{rel!r}]={defect_type!r} is not a {cls_name} type")
            if self.images is not None and rel not in self.images:
                raise ValueError(f"image_defects names {rel!r}, which is not in images")
        if not self.anomaly_generation.get("amg_serve"):
            served = {e.model for e in self.amg_endpoints}
            for key in ("amg_gen_model", "amg_judge_model"):
                if self.anomaly_generation.get(key) not in served:
                    raise ValueError(
                        f"anomaly_generation.{key}={self.anomaly_generation.get(key)!r} is not a "
                        f"model in amg_endpoints {sorted(served)}"
                    )
        return self

    @property
    def local_vlm(self) -> bool:
        return bool(self.anomaly_generation.get("amg_serve"))


def default_payload() -> dict[str, Any]:
    """Example conf shown in the Airflow UI: replace <bucket> and the paths with your own."""
    return {
        "campaign_id": "example",
        "round": 1,
        "dataset": "s3://<bucket>/datasets/Cosmos-AnomalyGen-PCB-Dataset/",
        "clean_images": "s3://<bucket>/aoi/inputs/<clean-images>/",
        "defects_by_class": {
            "IC": ["IC+bridge"],
            "passive_component": ["passive_component+missing", "passive_component+excess_solder"],
        },
        "chunk_size": 10,
        "output_root": "s3://<bucket>/aoi/runs/",
        "recipe": "s3://<bucket>/aoi/config/cosmos-anomalygen-pcb-nano/exp_texture_ft_pcb.yaml",
        "checkpoint": "s3://<bucket>/aoi/config/cosmos-anomalygen-pcb-nano/iter_000015000.pt",
        "evaluate": True,
        "amg_endpoints": [dict(ASTRA_ENDPOINT)],
        "anomaly_generation": dict(DEFAULT_PIPELINE_OPTIONS),
    }
