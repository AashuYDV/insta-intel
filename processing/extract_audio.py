"""
insta_intel/processing/extract_audio.py
Extracts audio track from downloaded reel video using ffmpeg.
"""

from __future__ import annotations
import os
import subprocess
from typing import Optional

from config.settings import AUDIO_DIR
from utils.helpers import get_logger, ensure_dirs

log = get_logger(__name__)


def extract_audio(video_path: str) -> Optional[str]:
    """
    Extract audio from video file as .mp3.
    Returns path to audio file, or None on failure.
    Requires: ffmpeg installed on system.
    """
    ensure_dirs(AUDIO_DIR)

    base      = os.path.splitext(os.path.basename(video_path))[0]
    audio_out = os.path.join(AUDIO_DIR, f"{base}.mp3")

    if os.path.exists(audio_out):
        log.debug("Audio already extracted: %s", base)
        return audio_out

    cmd = [
        "ffmpeg",
        "-i", video_path,
        "-vn",                   # no video
        "-acodec", "libmp3lame",
        "-ar", "16000",          # 16kHz — optimal for Whisper
        "-ac", "1",              # mono
        "-b:a", "64k",
        "-y",                    # overwrite
        "-loglevel", "error",
        audio_out,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0 and os.path.exists(audio_out):
            log.info("Audio extracted: %s", audio_out)
            return audio_out
        else:
            log.warning("ffmpeg error: %s", result.stderr[:200])
            return None
    except subprocess.TimeoutExpired:
        log.error("ffmpeg timeout: %s", video_path)
        return None
    except FileNotFoundError:
        log.error("ffmpeg not found. Install: brew install ffmpeg  OR  apt install ffmpeg")
        return None
    except Exception as e:
        log.error("Audio extraction error: %s", e)
        return None


def cleanup_audio(file_path: str) -> None:
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    except Exception:
        pass
