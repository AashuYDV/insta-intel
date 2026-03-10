"""
insta_intel/processing/transcribe.py
Transcribes reel audio using OpenAI Whisper API.
"""

from __future__ import annotations
import os
from typing import Optional

import openai

from config.settings import OPENAI_API_KEY, WHISPER_MODEL
from utils.helpers import get_logger

log  = get_logger(__name__)
openai.api_key = OPENAI_API_KEY


def transcribe_audio(audio_path: str) -> str:
    """
    Send audio file to OpenAI Whisper and return transcript.
    Returns empty string on failure.
    """
    if not audio_path or not os.path.exists(audio_path):
        log.warning("Audio file not found: %s", audio_path)
        return ""

    file_size_mb = os.path.getsize(audio_path) / (1024 * 1024)
    if file_size_mb > 25:
        log.warning("Audio file too large for Whisper (%.1fMB > 25MB): %s", file_size_mb, audio_path)
        return ""

    try:
        with open(audio_path, "rb") as f:
            response = openai.audio.transcriptions.create(
                model=WHISPER_MODEL,
                file=f,
                language="en",
                response_format="text",
            )
        transcript = str(response).strip()
        log.info("Transcribed %s → %d chars", os.path.basename(audio_path), len(transcript))
        return transcript
    except openai.APIError as e:
        log.error("Whisper API error: %s", e)
        return ""
    except Exception as e:
        log.error("Transcription error: %s", e)
        return ""
