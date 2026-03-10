"""
insta_intel/processing/ai_analysis.py
Sends reel caption + transcript to OpenAI and extracts structured metadata:
hook, topic, cta, format, summary.
"""

from __future__ import annotations
import json
import time
from typing import Dict, Any, Optional

import openai

from config.settings import OPENAI_API_KEY, OPENAI_MODEL
from utils.helpers import get_logger

log = get_logger(__name__)
openai.api_key = OPENAI_API_KEY

# ── System prompt ─────────────────────────────────────────────
SYSTEM_PROMPT = """You are an expert Instagram content analyst specialising in the study-abroad and international education niche.
You analyse Instagram Reels from competitors in this space and extract structured insights that help content teams understand what is working.
Always respond with valid JSON only — no markdown, no explanation, just the JSON object."""

# ── User prompt template ──────────────────────────────────────
USER_PROMPT_TEMPLATE = """Analyse this Instagram Reel from a study-abroad / education account.

CAPTION:
{caption}

TRANSCRIPT (speech from the video):
{transcript}

Extract the following and return ONLY a valid JSON object with these exact keys:

{{
  "hook": "The opening line or technique used to grab attention in the first 3 seconds. Be specific.",
  "topic": "The main subject. Choose from: UK Visa, Canada Visa, Australia Visa, USA Visa, Germany Visa, IELTS Tips, PTE Tips, Scholarships, SOP/LOR, Education Loan, Student Life, University Admissions, Cost of Living, Part-time Jobs, Study Tips, Motivation, General Study Abroad. Pick the closest match.",
  "cta": "The call-to-action used. E.g. 'Follow for more', 'Save this reel', 'Comment below', 'DM us', 'Link in bio', 'Share this', or 'None'.",
  "format": "Content format. Choose from: Talking Head, Voiceover + Text, Listicle, Storytime, Q&A, Day in My Life, Tips & Tricks, Myth vs Fact, Before & After, Reaction, Tutorial, Testimonial.",
  "summary": "One sentence (max 120 chars) summarising what the reel is about and why it would appeal to Indian students planning to study abroad."
}}"""


def analyse_reel(caption: str, transcript: str) -> Dict[str, Any]:
    """
    Send caption + transcript to OpenAI and return structured analysis.
    Returns empty dict on failure.
    """
    if not caption and not transcript:
        return {}

    prompt = USER_PROMPT_TEMPLATE.format(
        caption    = (caption    or "No caption available.")[:800],
        transcript = (transcript or "No transcript available.")[:1200],
    )

    for attempt in range(3):
        try:
            response = openai.chat.completions.create(
                model       = OPENAI_MODEL,
                messages    = [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0.2,
                max_tokens  = 400,
            )
            raw = response.choices[0].message.content.strip()

            # Strip markdown fences if present
            raw = raw.replace("```json", "").replace("```", "").strip()
            result = json.loads(raw)

            # Validate required keys
            required = {"hook", "topic", "cta", "format", "summary"}
            if required.issubset(result.keys()):
                log.info("AI analysis OK — topic: %s | format: %s", result["topic"], result["format"])
                return result
            else:
                log.warning("AI response missing keys: %s", result.keys())

        except json.JSONDecodeError as e:
            log.warning("JSON parse error (attempt %d): %s", attempt + 1, e)
        except openai.RateLimitError:
            wait = 20 * (attempt + 1)
            log.warning("Rate limited — waiting %ds", wait)
            time.sleep(wait)
        except openai.APIError as e:
            log.error("OpenAI API error: %s", e)
            break
        except Exception as e:
            log.error("AI analysis error: %s", e)
            break

    return {}


def batch_analyse(
    reels: list,
    delay_s: float = 1.0,
) -> list:
    """
    Analyse a list of reel dicts in place.
    Adds 'ai_analysis' key to each dict.
    """
    total = len(reels)
    for i, reel in enumerate(reels, 1):
        if reel.get("ai_analysis"):
            log.debug("Skipping already-analysed reel %d/%d", i, total)
            continue
        log.info("Analysing reel %d/%d — @%s", i, total, reel.get("competitor"))
        analysis = analyse_reel(
            caption    = reel.get("caption",    ""),
            transcript = reel.get("transcript", ""),
        )
        reel["ai_analysis"] = analysis
        time.sleep(delay_s)   # be gentle with the API
    return reels
