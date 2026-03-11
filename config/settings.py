import os

def _get(key, default=""):
    try:
        import streamlit as st
        val = st.secrets.get(key)
        if val:
            return val
    except Exception:
        pass
    return os.getenv(key, default)

APIFY_API_TOKEN        = _get("APIFY_API_TOKEN")
APIFY_ACTOR_ID         = "apify~instagram-reel-scraper"
APIFY_RESULTS_PER_ACC  = int(_get("APIFY_RESULTS_PER_ACC", "10"))
OPENAI_API_KEY         = _get("OPENAI_API_KEY")
OPENAI_MODEL           = _get("OPENAI_MODEL", "gpt-4o-mini")
WHISPER_MODEL          = _get("WHISPER_MODEL", "whisper-1")
MONGO_URI              = _get("MONGO_URI")
MONGO_DB               = _get("MONGO_DB", "insta_intel")
MONGO_COLLECTION_REELS = "reels"
MONGO_COLLECTION_ACCS  = "accounts"
PIPELINE_INTERVAL_DAYS = int(_get("PIPELINE_INTERVAL_DAYS", "2"))
DOWNLOAD_DIR           = _get("DOWNLOAD_DIR", "/tmp/insta_intel_videos")
AUDIO_DIR              = _get("AUDIO_DIR", "/tmp/insta_intel_audio")
