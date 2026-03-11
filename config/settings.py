import os
try:
    import streamlit as st
    _secrets = st.secrets
except Exception:
    _secrets = {}

def _get(key, default=""):
    # Try Streamlit secrets first, then environment variables
    try:
        return _secrets[key]
    except Exception:
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
```

---

## Commit and wait 2 minutes
```
git commit message: "fix settings - read from st.secrets for Streamlit Cloud"
