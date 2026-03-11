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

COST_PER_RESULT_USD       = 0.004
SAFETY_BUFFER_USD         = 0.50
MIN_CREDIT_TO_RUN_USD     = 0.20
FREE_TIER_MAX_PER_ACCOUNT = 10
FREE_TIER_MAX_PER_RUN     = 200
MIN_HOURS_BETWEEN_SCRAPES = 48


def get_credit_balance():
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
            "Credit: $%.4f used / $%.2f limit -> $%.4f available -> %d results max",
            used_usd, limit_usd, available_usd, result["max_results"],
        )
        if not can_run:
            log.warning("Insufficient credit ($%.4f). Skipping run.", available_usd)

        return result

    except Exception as e:
        log.error("Credit check error: %s", e)
        return {"available_usd": 0, "can_run": False}


def _get_last_scraped_map():
    try:
        from database.mongo_client import reels_col
        pipeline = [{"$group": {"_id": "$competitor", "last": {"$max": "$scraped_at"}}}]
        return {r["_id"]: r["last"] for r in reels_col().aggregate(pipeline)}
    except Exception:
        return {}


def _hours_since_scraped(username, last_map):
    last = last_map.get(username.lower())
    if not last:
        return float("inf")
    try:
        dt = datetime.strptime(last[:10], "%Y-%m-%d")
        return (datetime.utcnow() - dt).total_seconds() / 3600
    except Exception:
        return float("inf")


def _prioritize(usernames, acc_type, last_map, max_accs):
    scored = [
        (u, _hours_since_scraped(u, last_map))
        for u in usernames
        if _hours_since_scraped(u, last_map) >= MIN_HOURS_BETWEEN_SCRAPES
    ]
    scored.sort(key=lambda x: -x[1])
    selected = [u for u, _ in scored[:max_accs]]
    skipped  = len(usernames) - len(selected)
    log.info(
        "[%s] Selected %d/%d accounts -- %d skipped (scraped recently)",
        acc_type, len(selected), len(usernames), skipped
    )
    return selected


def _run_actor(username, max_reels):
    """Run apify~instagram-reel-scraper for a single username."""
    run_input = {
        "username": [username],
        "maxReels": max_reels,
    }
    resp = requests.post(
        f"{BASE}/acts/apify~instagram-reel-scraper/runs",
        params={"token": APIFY_API_TOKEN},
        json=run_input,
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        log.error("Actor start failed %s: %s", resp.status_code, resp.text[:200])
        return None
    data = resp.json().get("data", {})
    log.info("Run started -> %s", data.get("id"))
    return data.get("id"), data.get("defaultDatasetId")


def _poll_run(run_id, timeout_s=300):
    url    = f"{BASE}/actor-runs/{run_id}"
    params = {"token": APIFY_API_TOKEN}
    waited = 0
    while waited < timeout_s:
        time.sleep(10)
        waited += 10
        try:
            state = (
                requests.get(url, params=params, timeout=10)
                .json().get("data", {}).get("status", "RUNNING")
            )
            log.debug("Run %s -> %s (%ds)", run_id, state, waited)
            if state == "SUCCEEDED":
                return True
            if state in ("FAILED", "ABORTED", "TIMED-OUT"):
                log.error("Run %s: %s", run_id, state)
                return False
        except Exception as e:
            log.warning("Poll error: %s", e)
    return False


def _fetch_dataset(dataset_id, limit=50):
    resp = requests.get(
        f"{BASE}/datasets/{dataset_id}/items",
        params={"token": APIFY_API_TOKEN, "limit": limit},
        timeout=60,
    )
    items = resp.json() if resp.status_code == 200 else []
    log.info("Fetched %d raw items", len(items))
    return items


def _actual_cost(run_id):
    try:
        return round(
            requests.get(
                f"{BASE}/actor-runs/{run_id}",
                params={"token": APIFY_API_TOKEN},
                timeout=10,
            ).json().get("data", {}).get("usageTotalUsd", 0.0), 6
        )
    except Exception:
        return 0.0


def _normalise(item, username, account_type):
    """
    Map raw apify~instagram-reel-scraper item to clean reel document.

    Field names verified against instagram-reel-scraper actor output:
    - views     -> videoPlayCount (primary), fallback to viewsCount, playsCount
    - likes     -> likesCount, fallback to diggCount
    - comments  -> commentsCount, fallback to commentCount
    - url       -> url, fallback to shortCode
    - caption   -> caption, fallback to text
    - timestamp -> timestamp, fallback to takenAt
    - audio     -> musicInfo.musicName, fallback to Original audio
    - videoUrl  -> videoUrl (stored for potential future download use)
    """

    # ── Views: try every known field name, skip 0-valued ones ──────────
    views = 0
    for field in ["videoPlayCount", "viewsCount", "playsCount", "videoViewCount", "plays"]:
        val = safe_int(item.get(field))
        if val and val > 0:
            views = val
            break

    # ── Likes ────────────────────────────────────────────────────────────
    likes = 0
    for field in ["likesCount", "likes", "diggCount"]:
        val = safe_int(item.get(field))
        if val and val > 0:
            likes = val
            break

    # ── Comments ─────────────────────────────────────────────────────────
    comments = 0
    for field in ["commentsCount", "comments", "commentCount"]:
        val = safe_int(item.get(field))
        if val and val > 0:
            comments = val
            break

    # ── Identifiers ──────────────────────────────────────────────────────
    shortcode = item.get("shortCode") or item.get("code") or item.get("id") or ""
    url = (
        item.get("url") or
        item.get("reelUrl") or
        item.get("permalinkUrl") or
        (f"https://www.instagram.com/reel/{shortcode}/" if shortcode else None)
    )

    if not url:
        log.debug("Skipping item with no URL")
        return None

    # ── Content ──────────────────────────────────────────────────────────
    caption   = clean_caption(item.get("caption") or item.get("text") or "")
    ts        = item.get("timestamp") or item.get("takenAt") or item.get("date") or ""
    video_url = item.get("videoUrl") or item.get("video_url") or ""

    # ── Audio ─────────────────────────────────────────────────────────────
    music = item.get("musicInfo") or item.get("music") or {}
    if isinstance(music, dict):
        audio = (
            music.get("musicName") or
            music.get("name") or
            music.get("musicArtistName") or
            item.get("audio") or
            "Original audio"
        )
    else:
        audio = item.get("audio") or "Original audio"

    # ── Log what we got for debugging ─────────────────────────────────────
    log.debug(
        "@%s | views=%d | likes=%d | comments=%d | shortcode=%s",
        username, views, likes, comments, shortcode
    )

    return {
        "competitor":      username.lower(),
        "account_type":    account_type,
        "reel_url":        url,
        "shortcode":       shortcode,
        "video_url":       video_url,
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


def _debug_raw_item(item, username):
    """Log all fields from a raw Apify item for debugging."""
    log.info("--- RAW ITEM DEBUG for @%s ---", username)
    for key, val in item.items():
        if key != "images":
            log.info("  %s: %s", key, str(val)[:100])
    log.info("--- END DEBUG ---")


def _usage_report(reels, cost, credit_before, accounts):
    remaining = max(0.0, credit_before - cost - SAFETY_BUFFER_USD)
    runs_left = int(remaining / max(cost, 0.001)) if cost > 0 else 999
    log.info("")
    log.info("╔══════════════════════════════════════════╗")
    log.info("║        APIFY USAGE REPORT                ║")
    log.info("╠══════════════════════════════════════════╣")
    log.info("║  Accounts scraped : %-4d                 ║", accounts)
    log.info("║  Reels collected  : %-4d                 ║", reels)
    log.info("║  This run cost    : $%-8.4f             ║", cost)
    log.info("║  Credit remaining : $%-8.4f             ║", remaining)
    log.info("║  Est. runs left   : %-4s                 ║", str(runs_left) if runs_left < 999 else "many")
    log.info("╚══════════════════════════════════════════╝")
    if remaining < 1.0:
        log.warning("Less than $1.00 remaining - consider upgrading to Starter ($29/mo)")
    if remaining < 0.50:
        log.error("Under $0.50 remaining - pipeline will auto-pause next run")


def scrape_all_accounts(company_accounts, creator_accounts):
    """
    Main entry point. Scrapes reels one account at a time
    using apify~instagram-reel-scraper.
    """
    log.info("=" * 55)
    log.info("  APIFY FREE TIER OPTIMIZED SCRAPER")
    log.info("=" * 55)

    # 1. Credit check
    credit = get_credit_balance()
    if not credit["can_run"]:
        return []

    available   = credit["available_usd"]
    max_results = min(credit["max_results"], FREE_TIER_MAX_PER_RUN)
    per_acc     = FREE_TIER_MAX_PER_ACCOUNT

    # 2. Split budget 60% company / 40% creator
    company_slots = max(1, math.floor(max_results * 0.60) // per_acc)
    creator_slots = max(1, math.floor(max_results * 0.40) // per_acc)

    log.info(
        "Budget: $%.4f -> %d results max | %d company slots | %d creator slots",
        available, max_results, company_slots * per_acc, creator_slots * per_acc
    )

    # 3. Prioritize accounts
    last_map    = _get_last_scraped_map()
    sel_company = _prioritize(company_accounts, "company", last_map, company_slots)
    sel_creator = _prioritize(creator_accounts, "creator", last_map, creator_slots)

    if not sel_company and not sel_creator:
        log.warning(
            "No accounts need scraping -- all scraped within %dh",
            MIN_HOURS_BETWEEN_SCRAPES
        )
        return []

    # 4. Scrape each account individually
    all_reels  = []
    total_cost = 0.0
    first_item_debugged = False

    for accs, acc_type in [(sel_company, "company"), (sel_creator, "creator")]:
        if not accs:
            continue

        log.info(
            "[%s] Scraping %d accounts x %d reels each",
            acc_type, len(accs), per_acc
        )

        for username in accs:
            log.info("[%s] Starting @%s ...", acc_type, username)

            result = _run_actor(username, per_acc)
            if not result:
                log.warning("[%s] Skipping @%s -- actor failed to start", acc_type, username)
                continue

            run_id, dataset_id = result

            if not _poll_run(run_id):
                log.warning("[%s] @%s run did not succeed", acc_type, username)
                continue

            raw  = _fetch_dataset(dataset_id, limit=per_acc * 2)
            cost = _actual_cost(run_id)
            total_cost += cost

            log.info("[%s] @%s -> %d items fetched, cost $%.6f", acc_type, username, len(raw), cost)

            # Debug first item of very first account so we can see all field names
            if raw and not first_item_debugged:
                _debug_raw_item(raw[0], username)
                first_item_debugged = True

            count_before = len(all_reels)
            for item in raw:
                doc = _normalise(item, username, acc_type)
                if doc:
                    all_reels.append(doc)

            added = len(all_reels) - count_before
            log.info("[%s] @%s -> %d reels added", acc_type, username, added)

            # Check if we are running low on credit
            remaining = available - total_cost
            if remaining < MIN_CREDIT_TO_RUN_USD:
                log.warning("Credit running low ($%.4f) -- stopping early", remaining)
                break

            time.sleep(3)

    # 5. Usage report
    _usage_report(
        reels         = len(all_reels),
        cost          = total_cost,
        credit_before = available + SAFETY_BUFFER_USD,
        accounts      = len(sel_company) + len(sel_creator),
    )

    return all_reels


def scrape_profiles(usernames, account_type, results_per_account=10):
    """Backwards-compat wrapper."""
    return scrape_all_accounts(
        company_accounts = usernames if account_type == "company" else [],
        creator_accounts = usernames if account_type == "creator" else [],
    )
