"""
insta_intel/pipeline/run_pipeline.py
Master pipeline orchestrator. Runs end-to-end:
  1. Scrape reels from all competitor accounts via Apify
  2. Analyse each reel with OpenAI GPT (hook, topic, CTA, format, summary)
  3. Store everything in MongoDB

Run manually:    python -m pipeline.run_pipeline
GitHub Actions:  triggered on schedule every 2 days (see .github/workflows/pipeline.yml)
"""

from __future__ import annotations
import json
import sys
import time
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.apify_scraper  import scrape_all_accounts
from processing.ai_analysis import analyse_reel
from database.mongo_client  import (
    ensure_indexes,
    bulk_upsert_reels,
    upsert_ai_analysis,
)
from utils.helpers import get_logger

log = get_logger("pipeline")

ACCOUNTS_PATH = Path(__file__).parent.parent / "config" / "accounts.json"


def load_accounts() -> dict:
    with open(ACCOUNTS_PATH) as f:
        return json.load(f)


def run_scrape_step(accounts: dict) -> list:
    """Step 1 — Scrape all accounts and store raw reels in MongoDB."""
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


def run_ai_step() -> None:
    """Step 2 — AI analysis for all reels that don't have it yet."""
    log.info("═" * 60)
    log.info("STEP 2 — AI ANALYSIS")
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
        log.info("[%d/%d] Analysing: %s", i, len(reels), reel.get("reel_url", ""))
        analysis = analyse_reel(
            caption    = reel.get("caption",    ""),
            transcript = reel.get("transcript", ""),
        )
        if analysis:
            upsert_ai_analysis(reel["reel_url"], analysis)
        time.sleep(1.2)   # stay within OpenAI rate limits


def run_pipeline(
    skip_scrape: bool = False,
    skip_ai:     bool = False,
) -> None:
    """Full pipeline run — scrape then analyse."""
    start = time.time()
    log.info("╔══════════════════════════════════════════╗")
    log.info("║   INSTA INTEL PIPELINE — START           ║")
    log.info("╚══════════════════════════════════════════╝")

    ensure_indexes()
    accounts = load_accounts()

    if not skip_scrape:
        run_scrape_step(accounts)

    if not skip_ai:
        run_ai_step()

    elapsed = int(time.time() - start)
    log.info("╔══════════════════════════════════════════╗")
    log.info("║   PIPELINE COMPLETE — %dm %ds elapsed    ║", elapsed // 60, elapsed % 60)
    log.info("╚══════════════════════════════════════════╝")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="InstaIntel Pipeline")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping step")
    parser.add_argument("--skip-ai",     action="store_true", help="Skip AI analysis step")
    args = parser.parse_args()

    run_pipeline(
        skip_scrape = args.skip_scrape,
        skip_ai     = args.skip_ai,
    )
