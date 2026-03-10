"""
insta_intel/processing/download_reel.py
Downloads reel video files for audio extraction + transcription.
Uses yt-dlp (best free, no-login download tool for public IG reels).
"""

from __future__ import annotations
import os
import subprocess
from typing import Optional

from config.settings import DOWNLOAD_DIR
from utils.helpers import get_logger, ensure_dirs, shortcode_from_url

log = get_logger(__name__)


def download_reel(reel_url: str) -> Optional[str]:
    """
    Download reel video to DOWNLOAD_DIR.
    Returns local file path on success, None on failure.
    Requires: pip install yt-dlp
    """
    ensure_dirs(DOWNLOAD_DIR)

    shortcode = shortcode_from_url(reel_url) or reel_url.split("/")[-2]
    out_path  = os.path.join(DOWNLOAD_DIR, f"{shortcode}.mp4")

    if os.path.exists(out_path):
        log.debug("Already downloaded: %s", shortcode)
        return out_path

    cmd = [
        "yt-dlp",
        "--quiet",
        "--no-warnings",
        "-f", "mp4",
        "-o", out_path,
        reel_url,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0 and os.path.exists(out_path):
            log.info("Downloaded: %s → %s", shortcode, out_path)
            return out_path
        else:
            log.warning("yt-dlp failed for %s: %s", shortcode, result.stderr[:200])
            return None
    except subprocess.TimeoutExpired:
        log.error("Download timeout: %s", reel_url)
        return None
    except FileNotFoundError:
        log.error("yt-dlp not found. Install: pip install yt-dlp")
        return None
    except Exception as e:
        log.error("Download error: %s", e)
        return None


def cleanup_video(file_path: str) -> None:
    """Remove downloaded video file after processing."""
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            log.debug("Cleaned up: %s", file_path)
    except Exception as e:
        log.warning("Cleanup error: %s", e)
