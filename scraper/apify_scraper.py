"""
insta_intel/scraper/apify_scraper.py

FREE TIER OPTIMIZED — Apify $5/month credit manager.

Strategy:
  - Check remaining credit before every run
  - Calculate exactly how many results we can afford
  - Prioritize high-value accounts (never-scraped first, oldest-scraped next)
  - Skip accounts scraped recently (smart dedup)
  - Batch accounts to minimize actor run overhead
  - Hard stop when credit threshold is reached
  - Full usage report after every run
"""

from __future__ import annotations
import math
import time
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from config.settings import APIFY_API_TOKEN
from utils.helpers import get_logger, engagement_rate, safe_int, clean_caption, today_str

log = get_logger(__name__)
BASE = "https://api.apify.com/v2"

# ── Free tier budget constants ────────────────────────────────
COST_PER_RESULT_USD       = 0.004   # conservative estimate per reel result
SAFETY_BUFFER_USD         = 0.50    # always keep $0.50 in reserve
MIN_CREDIT_TO_RUN_USD     = 0.20    # don't start if less than $0.20 available
FREE_TIER_MAX_PER_ACCOUNT = 10      # max reels per account on free tier
FREE_TIER_MAX_PER_RUN     = 200     # hard cap on total results per pipeline run
MIN_HOURS_BETWEEN_SCRAPES = 48      # don't re-scrape same account within 48 hrs


# ═══════════════════════════════════════════════
#  CREDIT MONITOR
# ═══════════════════════════════════════════════

def get_credit_balance() -> Dict[str, Any]:
    """
    Fetch current Apify account usage and remaining credit.
    Returns dict with: available_usd, used_usd, limit_usd, can_run, max_results
    """
    try:
        resp = requests.get(
            f"{BASE}/users/me",
            params={"token": APIFY_API_TOKEN},
            timeout=10,
        )
        if resp.status_code != 200:
            log.error("Failed to fetch account info: %s", resp.status_code)
            return {"available_usd": 0, "can_run": False}

        data          = resp.json().get("data", {})
        plan          = data.get("plan", {})
        usage         = data.get("monthlyUsage", {})
        limit_usd     = float(plan.get("monthlyUsageCreditUsd", 5.0))
        used_usd      = float(usage.get("totalCreditUsd", 0.0))
        available_usd = max(0.0, limit_usd - used_usd - SAFETY_BUFFER_USD)
        can_run       = available_usd >= MIN_CREDIT_TO_RUN_USD

        result = {
            "limit_usd":     round(limit_usd,    4),
            "used_usd":      round(used_usd,      4),
            "available_usd": round(available_usd, 4),
            "can_run":       can_run,
            "max_results":   int(available_usd / COST_PER_RESULT_USD),
        }

        log.info(
            "💳 Credit: $%.4f used / $%.2f limit → $%.4f available → %d results max",
            used_usd, limit_usd, available_usd, result["max_results"],
        )
        if not can_run:
            log.warning("⚠️  Insufficient credit ($%.4f). Skipping run.", available_usd)

        return result

    except Exception as e:
        log.error("Credit check error: %s", e)
        return {"available_usd": 0, "can_run": False}


# ═══════════════════════════════════════════════
#  SMART ACCOUNT PRIORITIZER
# ═══════════════════════════════════════════════

def _get_last_scraped_map() -> Dict[str, str]:
    """Get most recent scraped_at date per account from MongoDB."""
    try:
        from database.mongo_client import reels_col
        pipeline = [{"$group": {"_id": "$competitor", "last": {"$max": "$scraped_at"}}}]
        return {r["_id"]: r["last"] for r in reels_col().aggregate(pipeline)}
    except Exception:
        return {}


def _hours_since_scraped(username: str, last_map: Dict[str, str]) -> float:
    last = last_map.get(username.lower())
    if not last:
        return float("inf")
    try:
        dt = datetime.strptime(last[:10], "%Y-%m-%d")
        return (datetime.utcnow() - dt).total_seconds() / 3600
    except Exception:
        return float("inf")


def _prioritize(
    usernames: List[str],
    acc_type:  str,
    last_map:  Dict[str, str],
    max_accs:  int,
) -> List[str]:
    """
    Return up to max_accs accounts, prioritized:
      1. Never scraped before
      2. Scraped longest ago (oldest first)
      3. Skip anything scraped within MIN_HOURS_BETWEEN_SCRAPES
    """
    scored = [
        (u, _hours_since_scraped(u, last_map))
        for u in usernames
        if _hours_since_scraped(u, last_map) >= MIN_HOURS_BETWEEN_SCRAPES
    ]
    scored.sort(key=lambda x: -x[1])   # most stale first
    selected = [u for u, _ in scored[:max_accs]]
    skipped  = len(usernames) - len(selected)
    log.info("[%s] Selected %d/%d accounts — %d skipped (scraped recently)",
             acc_type, len(selected), len(usernames), skipped)
    return selected


# ═══════════════════════════════════════════════
#  ACTOR HELPERS
# ═══════════════════════════════════════════════

def _run_actor(run_input: Dict) -> Optional[Tuple[str, str]]:
    resp = requests.post(
        f"{BASE}/acts/apify~instagram-profile-scraper/runs",
        params={"token": APIFY_API_TOKEN},
        json=run_input,
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        log.error("Actor start failed %s: %s", resp.status_code, resp.text[:200])
        return None
    data = resp.json().get("data", {})
    log.info("▶  Run started → %s", data.get("id"))
    return data.get("id"), data.get("defaultDatasetId")


def _poll_run(run_id: str, timeout_s: int = 600) -> bool:
    url, params, waited = f"{BASE}/actor-runs/{run_id}", {"token": APIFY_API_TOKEN}, 0
    while waited < timeout_s:
        time.sleep(15); waited += 15
        try:
            state = requests.get(url, params=params, timeout=10).json().get("data",{}).get("status","RUNNING")
            log.debug("Run %s → %s (%ds)", run_id, state, waited)
            if state == "SUCCEEDED": return True
            if state in ("FAILED","ABORTED","TIMED-OUT"):
                log.error("Run %s: %s", run_id, state); return False
        except Exception as e:
            log.warning("Poll error: %s", e)
    return False


def _fetch_dataset(dataset_id: str, limit: int = 500) -> List[Dict]:
    resp = requests.get(
        f"{BASE}/datasets/{dataset_id}/items",
        params={"token": APIFY_API_TOKEN, "limit": limit},
        timeout=60,
    )
    items = resp.json() if resp.status_code == 200 else []
    log.info("📦 Fetched %d raw items", len(items))
    return items


def _actual_cost(run_id: str) -> float:
    try:
        return round(
            requests.get(f"{BASE}/actor-runs/{run_id}",
                         params={"token": APIFY_API_TOKEN}, timeout=10)
            .json().get("data",{}).get("usageTotalUsd", 0.0), 6
        )
    except Exception:
        return 0.0


# ═══════════════════════════════════════════════
#  DATA NORMALISER
# ═══════════════════════════════════════════════

def _normalise(item: Dict, username: str, account_type: str) -> Optional[Dict]:
    is_video = (
        item.get("isVideo") is True
        or str(item.get("type",      "")).upper() in ("VIDEO","REEL")
        or str(item.get("mediaType", "")).upper() in ("VIDEO","REEL")
        or item.get("videoUrl")      is not None
        or item.get("videoDuration") is not None
    )
    if not is_video:
        return None

    views     = safe_int(item.get("videoViewCount") or item.get("playsCount") or item.get("videoPlayCount"))
    likes     = safe_int(item.get("likesCount")    or item.get("likes"))
    comments  = safe_int(item.get("commentsCount") or item.get("comments"))
    shortcode = item.get("shortCode") or item.get("id","")
    url       = item.get("url") or f"https://www.instagram.com/reel/{shortcode}/"
    caption   = clean_caption(item.get("caption") or "")
    ts        = item.get("timestamp") or item.get("takenAt") or ""
    music     = item.get("musicInfo") or {}
    audio     = (
        music.get("musicName") or music.get("musicArtistName") or
        item.get("audio") or "Original audio"
    ) if isinstance(music, dict) else item.get("audio","Original audio")

    return {
        "competitor":      username.lower(),
        "account_type":    account_type,
        "reel_url":        url,
        "shortcode":       shortcode,
        "caption":         caption,
        "views":           views,
        "likes":           likes,
        "comments":        comments,
        "engagement_rate": engagement_rate(likes, comments, views),
        "audio":           str(audio)[:200],
        "transcript":      "",
        "ai_analysis":     {},
        "date":            ts[:10] if ts else today_str(),
        "scraped_at":      today_str(),
    }


# ═══════════════════════════════════════════════
#  USAGE REPORT
# ═══════════════════════════════════════════════

def _usage_report(reels: int, cost: float, credit_before: float, accounts: int) -> None:
    remaining  = max(0.0, credit_before - cost - SAFETY_BUFFER_USD)
    runs_left  = int(remaining / max(cost, 0.001)) if cost > 0 else 999
    log.info("")
    log.info("╔══════════════════════════════════════════╗")
    log.info("║        APIFY USAGE REPORT                ║")
    log.info("╠══════════════════════════════════════════╣")
    log.info("║  Accounts scraped : %-4d                 ║", accounts)
    log.info("║  Reels collected  : %-4d                 ║", reels)
    log.info("║  This run cost    : $%-8.4f             ║", cost)
    log.info("║  Credit remaining : $%-8.4f             ║", remaining)
    log.info("║  Est. runs left   : %-4s                 ║", str(runs_left) if runs_left < 999 else "∞")
    log.info("╚══════════════════════════════════════════╝")
    if remaining < 1.0:
        log.warning("⚠️  Less than $1.00 remaining — consider upgrading to Starter ($29/mo)")
    if remaining < 0.50:
        log.error("🚨 Under $0.50 remaining — pipeline will auto-pause next run")


# ═══════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════

def scrape_all_accounts(
    company_accounts: List[str],
    creator_accounts: List[str],
) -> List[Dict]:
    """
    Main entry point. Fully free-tier-optimized scraping run.

    1. Check credit balance — abort if insufficient
    2. Calculate max affordable results
    3. Prioritize unscraped / stalest accounts
    4. Run actor for company accounts, then creator accounts
    5. Print usage report
    """
    log.info("═" * 55)
    log.info("  APIFY FREE TIER OPTIMIZED SCRAPER")
    log.info("═" * 55)

    # 1. Credit check
    credit = get_credit_balance()
    if not credit["can_run"]:
        return []

    available         = credit["available_usd"]
    max_results       = min(credit["max_results"], FREE_TIER_MAX_PER_RUN)
    per_acc           = FREE_TIER_MAX_PER_ACCOUNT

    # 2. Split budget 60% company / 40% creator
    company_slots = math.floor(max_results * 0.60) // per_acc
    creator_slots = math.floor(max_results * 0.40) // per_acc
    company_slots = max(1, company_slots)
    creator_slots = max(1, creator_slots)

    log.info("📊 Budget: $%.4f → %d results max | %d company slots | %d creator slots",
             available, max_results, company_slots * per_acc, creator_slots * per_acc)

    # 3. Prioritize accounts
    last_map = _get_last_scraped_map()
    sel_company = _prioritize(company_accounts, "company", last_map, company_slots)
    sel_creator = _prioritize(creator_accounts, "creator", last_map, creator_slots)

    if not sel_company and not sel_creator:
        log.warning("No accounts need scraping right now — all scraped within %dh", MIN_HOURS_BETWEEN_SCRAPES)
        return []

    # 4. Run scraping
    all_reels  = []
    total_cost = 0.0

    for accs, acc_type in [(sel_company, "company"), (sel_creator, "creator")]:
        if not accs:
            continue

        log.info("🔍 [%s] Scraping %d accounts × %d results = %d max reels",
                 acc_type, len(accs), per_acc, len(accs) * per_acc)

        result = _run_actor({"usernames": accs, "resultsLimit": per_acc, "resultsType": "posts"})
        if not result:
            continue

        run_id, dataset_id = result
        if not _poll_run(run_id):
            continue

        raw   = _fetch_dataset(dataset_id, limit=len(accs) * per_acc * 2)
        cost  = _actual_cost(run_id)
        total_cost += cost
        log.info("💸 [%s] Run cost: $%.6f", acc_type, cost)

        for item in raw:
            owner   = (item.get("ownerUsername") or (item.get("owner") or {}).get("username","") or "").lower()
            matched = next((u for u in accs if u.lower() == owner), accs[0])
            doc     = _normalise(item, matched, acc_type)
            if doc:
                all_reels.append(doc)

        log.info("✅ [%s] %d reels normalised", acc_type,
                 sum(1 for r in all_reels if r["account_type"] == acc_type))

    # 5. Usage report
    _usage_report(
        reels          = len(all_reels),
        cost           = total_cost,
        credit_before  = available + SAFETY_BUFFER_USD,
        accounts       = len(sel_company) + len(sel_creator),
    )

    return all_reels


# backwards-compat shim used by run_pipeline.py
def scrape_profiles(usernames: List[str], account_type: str, results_per_account: int = 10) -> List[Dict]:
    """Thin wrapper — routes to scrape_all_accounts."""
    return scrape_all_accounts(
        company_accounts = usernames if account_type == "company" else [],
        creator_accounts = usernames if account_type == "creator" else [],
    )
