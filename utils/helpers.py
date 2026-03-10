"""
insta_intel/utils/helpers.py
Shared utility functions used across the pipeline.
"""

import os
import re
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

def fmt_number(n: int) -> str:
    """Format large numbers: 1200000 → '1.2M', 45000 → '45K'"""
    if n is None:
        return "0"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)

def engagement_rate(likes: int, comments: int, views: int) -> float:
    """engagement_rate = (likes + comments) / views"""
    if not views or views == 0:
        return 0.0
    return round((likes + comments) / views, 4)

def clean_caption(caption: str) -> str:
    """Strip excessive whitespace and newlines from caption."""
    if not caption:
        return ""
    caption = re.sub(r"\s+", " ", caption).strip()
    return caption[:1000]

def ensure_dirs(*dirs: str) -> None:
    """Create directories if they don't exist."""
    for d in dirs:
        os.makedirs(d, exist_ok=True)

def safe_int(val, default: int = 0) -> int:
    try:
        return int(val or default)
    except (TypeError, ValueError):
        return default

def today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def shortcode_from_url(url: str) -> Optional[str]:
    """Extract Instagram shortcode from reel URL."""
    match = re.search(r"/reel/([A-Za-z0-9_-]+)", url)
    return match.group(1) if match else None

def truncate(text: str, max_len: int = 120) -> str:
    if not text:
        return ""
    return text if len(text) <= max_len else text[:max_len] + "…"
