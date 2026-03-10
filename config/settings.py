"""
insta_intel/config/settings.py
Central configuration. All secrets loaded from environment variables.
Copy .env.example to .env and fill in your values.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Apify ────────────────────────────────────────────────────
APIFY_API_TOKEN        = os.getenv("APIFY_API_TOKEN", "")
APIFY_ACTOR_ID         = "apify~instagram-reel-scraper"   # official profile scraper
APIFY_RESULTS_PER_ACC  = int(os.getenv("APIFY_RESULTS_PER_ACC", "20"))

# ── OpenAI ───────────────────────────────────────────────────
OPENAI_API_KEY         = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL           = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
WHISPER_MODEL          = os.getenv("WHISPER_MODEL", "whisper-1")

# ── MongoDB ──────────────────────────────────────────────────
MONGO_URI              = os.getenv("MONGO_URI", "mongodb+srv://<user>:<pass>@cluster.mongodb.net/")
MONGO_DB               = os.getenv("MONGO_DB", "insta_intel")
MONGO_COLLECTION_REELS = "reels"
MONGO_COLLECTION_ACCS  = "accounts"

# ── Pipeline ─────────────────────────────────────────────────
PIPELINE_INTERVAL_DAYS = int(os.getenv("PIPELINE_INTERVAL_DAYS", "2"))
DOWNLOAD_DIR           = os.getenv("DOWNLOAD_DIR", "/tmp/insta_intel_videos")
AUDIO_DIR              = os.getenv("AUDIO_DIR",    "/tmp/insta_intel_audio")

# ── Dashboard ────────────────────────────────────────────────
DASHBOARD_TITLE        = "LeapScholar · Insta Intel"
DASHBOARD_ICON         = "🚀"
