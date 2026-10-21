# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read SMT image manifests for augmentation and simulation reporting."""

from pathlib import PurePosixPath

from airflow.exceptions import AirflowFailException
from multistorageclient.types import PatternType
from yarl import URL

from dags.shared.utils.msc_utils import ensure_msc_configured, list_directory, read_file_as_dict
from dags.shared.utils.video_input_utils import get_relative_storage_path


def relative_path(value: str) -> str:
    """Keep manifest paths within their artifact directory."""
    path = PurePosixPath(value)
    if not path.parts or path.is_absolute() or ".." in path.parts or ":" in value or "\\" in value:
        raise AirflowFailException(
            f"Image paths must stay inside their artifact directory: {value}"
        )
    return value


@ensure_msc_configured
def count_rendered_simulation_images(simulation_directory: str) -> int:
    """Count unique RGB captures across all pass manifests, without reading image data."""
    images = set()
    found_manifest = False
    for obj in list_directory(
        simulation_directory, patterns=[(PatternType.INCLUDE, "*/dataset_index.json")]
    ):
        relative = get_relative_storage_path(obj.key, simulation_directory)
        if relative is None:
            continue
        parts = PurePosixPath(relative).parts
        # Each capture pass publishes its own index at <pass>/dataset_index.json.
        if len(parts) != 2 or parts[1] != "dataset_index.json":
            continue
        found_manifest = True
        index = read_file_as_dict(obj.key)
        if index is None:
            raise AirflowFailException(f"Missing SMT simulation manifest: {obj.key}")
        capture = next((item for item in index["passes"] if item["name"] == parts[0]), None)
        if capture is None:
            raise AirflowFailException(f"Missing SMT simulation pass {parts[0]!r} in {obj.key}")
        root = URL(obj.key).parent
        for item in index["artifacts"]:
            if (
                item["pass_id"] == capture["pass_id"]
                and item["kind"] == "rgb"
                and item["role"] == "capture"
            ):
                images.add(str(root / relative_path(item["path"])))
    if not found_manifest:
        raise AirflowFailException(f"Missing SMT simulation manifests: {simulation_directory}")
    return len(images)


@ensure_msc_configured
def load_simulation_images(golden: dict) -> list[dict]:
    """Read RGB captures and component identities from the selected simulation pass."""
    root = URL(golden["simulation_directory"]) / golden["simulation_pass"]
    index = read_file_as_dict(str(root / "dataset_index.json"))
    if index is None:
        raise AirflowFailException(f"Missing SMT simulation manifest: {root}/dataset_index.json")
    capture = next(
        (item for item in index["passes"] if item["name"] == golden["simulation_pass"]), None
    )
    if capture is None:
        raise AirflowFailException(f"Missing SMT simulation pass: {golden['simulation_pass']}")
    frames = {}
    for trigger in capture["triggers"]:
        metadata = read_file_as_dict(str(root / relative_path(trigger["metadata_path"])))
        frames.update({frame["frame_id"]: frame for frame in metadata["frames"]})
    captures = sorted(
        (
            item
            for item in index["artifacts"]
            if item["pass_id"] == capture["pass_id"]
            and item["kind"] == "rgb"
            and item["role"] == "capture"
        ),
        key=lambda item: item["path"],
    )
    if not captures:
        raise AirflowFailException("No SMT simulation RGB images in the selected pass")
    if len({item["path"] for item in captures}) != len(captures):
        raise AirflowFailException("Duplicate SMT simulation image paths")
    selected_image = golden["simulation_image"]
    if selected_image:
        captures = [
            item for item in captures if str(root / relative_path(item["path"])) == selected_image
        ]
        if not captures:
            raise AirflowFailException(
                f"Selected SMT simulation image is absent from pass "
                f"{golden['simulation_pass']!r}: {selected_image}"
            )
    images = []
    for number, item in enumerate(captures):
        component = frames[item["frame_id"]].get("roi_selection", {})
        name = "image" if selected_image else f"image_{number:06d}"
        images.append(
            {
                "file": name + PurePosixPath(item["path"]).suffix,
                "simulation_image": str(root / relative_path(item["path"])),
                "refdes": component.get("refdes"),
                "component_type": component.get("component_type"),
            }
        )
    return images


@ensure_msc_configured
def load_augmented_images(manifest_path: str) -> list[dict]:
    """Read the image list written before augmentation tasks are mapped."""
    manifest = read_file_as_dict(manifest_path)
    if manifest is None:
        raise AirflowFailException(f"Missing SMT augmentation manifest: {manifest_path}")
    images = manifest["images"]
    if not images or len({item["file"] for item in images}) != len(images):
        raise AirflowFailException("SMT augmentation manifest must contain distinct images")
    for item in images:
        relative_path(item["file"])
    return images
