"""
insta_intel/pipeline/run_pipeline.py
Master pipeline orchestrator. Runs end-to-end:
  1. Scrape reels from all accounts
  2. Download videos
  3. Extract audio
  4. Transcribe with Whisper
  5. Analyse with OpenAI
  6. Store in MongoDB

Run manually:    python -m pipeline.run_pipeline
Cron (every 2d): 0 6 */2 * * cd /path/to/insta_intel && python -m pipeline.run_pipeline >> logs/pipeline.log 2>&1
"""

from __future__ import annotations
import json
import os
import sys
import time
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings       import APIFY_RESULTS_PER_ACC
from scraper.apify_scraper import scrape_all_accounts
from processing.download_reel import download_reel, cleanup_video
from processing.extract_audio import extract_audio, cleanup_audio
from processing.transcribe    import transcribe_audio
from processing.ai_analysis   import analyse_reel
from database.mongo_client    import (
    ensure_indexes,
    bulk_upsert_reels,
    upsert_transcript,
    upsert_ai_analysis,
    get_all_reels,
)
from utils.helpers import get_logger

log = get_logger("pipeline")

ACCOUNTS_PATH = Path(__file__).parent.parent / "config" / "accounts.json"


def load_accounts() -> dict:
    with open(ACCOUNTS_PATH) as f:
        return json.load(f)


def run_scrape_step(accounts: dict) -> list:
    """Step 1 — Scrape all profiles and store raw reels."""
    log.info("═" * 60)
    log.info("STEP 1 — SCRAPING")
    log.info("═" * 60)

    all_reels = scrape_all_accounts(
        company_accounts = accounts["company_accounts"],
        creator_accounts = accounts["creator_accounts"],
    )

    log.info("Total reels scraped: %d", len(all_reels))

    stats = bulk_upsert_reels(all_reels)
    log.info("DB: %d inserted, %d skipped (duplicates)", stats["inserted"], stats["skipped"])

    return all_reels


def run_transcription_step() -> None:
    """Steps 2-4 — Download, extract audio, transcribe all reels missing transcripts."""
    log.info("═" * 60)
    log.info("STEP 2-4 — DOWNLOAD + AUDIO + TRANSCRIBE")
    log.info("═" * 60)

    # Get reels that don't have transcripts yet
    from database.mongo_client import reels_col
    reels = list(reels_col().find(
        {"transcript": {"$in": ["", None]}},
        {"reel_url": 1, "_id": 0}
    ).limit(50))   # process 50 at a time

    log.info("%d reels need transcription", len(reels))

    for i, reel in enumerate(reels, 1):
        url = reel["reel_url"]
        log.info("[%d/%d] Processing: %s", i, len(reels), url)

        video_path = audio_path = None
        try:
            # Download
            video_path = download_reel(url)
            if not video_path:
                upsert_transcript(url, "DOWNLOAD_FAILED")
                continue

            # Extract audio
            audio_path = extract_audio(video_path)
            if not audio_path:
                upsert_transcript(url, "AUDIO_FAILED")
                continue

            # Transcribe
            transcript = transcribe_audio(audio_path)
            upsert_transcript(url, transcript or "NO_SPEECH")
            log.info("Transcript saved (%d chars)", len(transcript))

        except Exception as e:
            log.error("Error processing %s: %s", url, e)
            upsert_transcript(url, "ERROR")
        finally:
            cleanup_audio(audio_path)
            cleanup_video(video_path)

        time.sleep(2)   # brief pause between downloads


def run_ai_step() -> None:
    """Step 5 — AI analysis for reels that don't have it yet."""
    log.info("═" * 60)
    log.info("STEP 5 — AI ANALYSIS")
    log.info("═" * 60)

    from database.mongo_client import reels_col
    reels = list(reels_col().find(
        {"$or": [
            {"ai_analysis": {"$exists": False}},
            {"ai_analysis": {}},
        ]},
        {"reel_url": 1, "caption": 1, "transcript": 1, "_id": 0}
    ).limit(100))

    log.info("%d reels need AI analysis", len(reels))

    for i, reel in enumerate(reels, 1):
        log.info("[%d/%d] Analysing: %s", i, len(reels), reel["reel_url"])
        analysis = analyse_reel(
            caption    = reel.get("caption",    ""),
            transcript = reel.get("transcript", ""),
        )
        if analysis:
            upsert_ai_analysis(reel["reel_url"], analysis)
        time.sleep(1.2)


def run_pipeline(
    skip_scrape:       bool = False,
    skip_transcribe:   bool = False,
    skip_ai:           bool = False,
) -> None:
    """Full pipeline run."""
    start = time.time()
    log.info("╔══════════════════════════════════════════╗")
    log.info("║   INSTA INTEL PIPELINE — START           ║")
    log.info("╚══════════════════════════════════════════╝")

    ensure_indexes()
    accounts = load_accounts()

    if not skip_scrape:
        run_scrape_step(accounts)

    if not skip_transcribe:
        run_transcription_step()

    if not skip_ai:
        run_ai_step()

    elapsed = int(time.time() - start)
    log.info("╔══════════════════════════════════════════╗")
    log.info("║   PIPELINE COMPLETE — %dm %ds elapsed", elapsed // 60, elapsed % 60)
    log.info("╚══════════════════════════════════════════╝")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="InstaIntel Pipeline")
    parser.add_argument("--skip-scrape",     action="store_true")
    parser.add_argument("--skip-transcribe", action="store_true")
    parser.add_argument("--skip-ai",         action="store_true")
    args = parser.parse_args()

    run_pipeline(
        skip_scrape     = args.skip_scrape,
        skip_transcribe = args.skip_transcribe,
        skip_ai         = args.skip_ai,
    )
