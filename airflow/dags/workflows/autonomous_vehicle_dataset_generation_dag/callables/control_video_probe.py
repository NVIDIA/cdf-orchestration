# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validate retrieved RGB clips or prepare aligned WSM control videos."""

import logging
import subprocess
from pathlib import Path

import media_probe


def resample_rgb(source: Path, target: Path, fps: int) -> None:
    """Resample in time without changing the clip's playback speed."""
    if fps <= 0:
        raise ValueError("RGB resampling requires positive FPS")
    completed = subprocess.run(
        [
            "/opt/ffmpeg/bin/ffmpeg",
            "-nostdin",
            "-hide_banner",
            "-v",
            "error",
            "-xerror",
            "-y",
            "-i",
            str(source),
            "-map",
            "0:v:0",
            "-an",
            "-vf",
            f"setpts=PTS-STARTPTS,fps={fps}",
            "-c:v",
            "libopenh264",
            "-b:v",
            "8M",
            "-pix_fmt",
            "yuv420p",
            "-threads",
            "2",
            str(target),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode:
        raise ValueError(
            "RGB resampling failed: " + (completed.stderr.strip() or f"exit {completed.returncode}")
        )


def extract_wsm_window(
    source: Path, target: Path, start_frame: int, num_frames: int, fps: int
) -> None:
    """Use FFmpeg to extract a specific frame range from a full-scene WSM video"""
    if start_frame < 0 or num_frames <= 0 or fps <= 0:
        raise ValueError("WSM extraction requires a nonnegative start and positive frames/FPS")

    completed = subprocess.run(
        [
            "/opt/ffmpeg/bin/ffmpeg",
            "-nostdin",
            "-hide_banner",
            "-v",
            "error",
            "-xerror",
            "-y",
            "-i",
            str(source),
            "-map",
            "0:v:0",
            "-an",
            "-vf",
            f"trim=start_frame={start_frame}:end_frame={start_frame + num_frames},"
            f"setpts=N/({fps}*TB)",
            "-enc_time_base:v",
            f"1:{fps}",
            "-fps_mode",
            "passthrough",
            "-c:v",
            "libopenh264",
            "-b:v",
            "8M",
            "-pix_fmt",
            "yuv420p",
            "-threads",
            "2",
            str(target),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode:
        raise ValueError(
            "WSM extraction with sidecar libopenh264 failed: "
            + (completed.stderr.strip() or f"exit {completed.returncode}")
        )


def probe_windows(items, work_dir: Path, endpoint_url):
    """Process each requested control with shared storage and the appropriate media decoder."""
    results = []
    for item in items:
        original = work_dir / "full.mp4"
        window = work_dir / "window.mp4"
        try:
            media_probe.fetch(item["uri"], original, endpoint_url)
            if item.get("kind") == "rgb_video":
                full = media_probe.validate_video_file(original, {})
                resample_rgb(original, window, item["expected"]["fps"])
                measured = media_probe.validate_video_file(window, item["expected"])
                if measured["frames"] < item["minimum_frames"]:
                    raise ValueError("RGB input does not cover the requested generation frames")
                media_probe.store(window.read_bytes(), item["output_uri"], endpoint_url)
                results.append(
                    {
                        "id": item["id"],
                        "uri": item["output_uri"],
                        **measured,
                        "source_uri": item["uri"],
                        "source_media": full,
                    }
                )
                continue
            full = media_probe.validate_video_file(original, item["expected"])
            if item.get("sha256") and item["sha256"] != full["sha256"]:
                raise ValueError("WSM checksum differs from the manifest")
            expected = item["window_expected"]
            extract_wsm_window(
                original, window, item["start_frame"], expected["num_frames"], expected["fps"]
            )
            measured = media_probe.validate_video_file(window, expected)
            media_probe.store(window.read_bytes(), item["output_uri"], endpoint_url)
            actual_start = item["control_start_ns"] + round(
                item["start_frame"] * 1e9 / expected["fps"]
            )
            control = {
                "uri": item["output_uri"],
                "sha256": measured["sha256"],
                "num_frames": expected["num_frames"],
                "start_ns": actual_start,
                "end_ns": actual_start + round(expected["num_frames"] * 1e9 / expected["fps"]),
                "full_scene_uri": item["uri"],
                "full_scene_sha256": full["sha256"],
                "start_frame": item["start_frame"],
                "alignment_error_ns": actual_start - item["source_start_ns"],
            }
            results.append({"id": item["id"], **measured, "control": control})
        except Exception as exc:
            results.append({"id": item["id"], "passed": False, "error": str(exc)})
        finally:
            original.unlink(missing_ok=True)
            window.unlink(missing_ok=True)
    return results


def run_control_video_probe_from_env(env=None):
    """Run the shared media probe with RGB validation or WSM window processing."""
    original = media_probe.probe_items
    try:
        media_probe.probe_items = probe_windows
        return media_probe.run_probe_from_env(env)
    finally:
        media_probe.probe_items = original


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_control_video_probe_from_env()
