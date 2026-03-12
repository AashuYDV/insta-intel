from __future__ import annotations

import math
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

from config.settings import APIFY_API_TOKEN
from utils.helpers import get_logger, engagement_rate, safe_int, clean_caption, today_str

log  = get_logger(__name__)
BASE = "https://api.apify.com/v2"

# ── Cost constants ─────────────────────────────────────────────────────────────
# Actor: apify/instagram-scraper  |  resultsType: reels
# Pricing: $2.30 per 1,000 results = $0.0023 per result (confirmed from Apify docs)
COST_PER_RESULT_USD       = 0.0023   # confirmed — NOT 0.004 or 0.025
SAFETY_BUFFER_USD         = 0.50
MIN_CREDIT_TO_RUN_USD     = 0.10
MAX_COST_PER_ACCOUNT_USD  = 0.05     # 20 reels * $0.0023 = $0.046 — cap at $0.05
RESULTS_PER_ACCOUNT       = 10       # reels to fetch per account
MAX_RESULTS_PER_RUN       = 210      # 21 accounts * 10 reels = 210 total
MIN_HOURS_BETWEEN_SCRAPES = 48
POLL_TIMEOUT_S            = 90       # reduced from 300 — fast-fail timed-out accounts
POLL_INTERVAL_S           = 8        # check every 8 seconds


# ── Credit monitor ─────────────────────────────────────────────────────────────

def get_credit_balance() -> Dict[str, Any]:
    """
    Fetch current Apify usage and calculate available credits.
    WARNING: The Apify API can return stale data — treat this as approximate.
    Always check actual run costs via _actual_cost() after each run.
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
            "Credit: $%.4f used / $%.2f limit -> $%.4f available -> %d results max",
            used_usd, limit_usd, available_usd, result["max_results"],
        )
        if not can_run:
            log.warning("Insufficient credit ($%.4f). Skipping run.", available_usd)

        return result

    except Exception as e:
        log.error("Credit check error: %s", e)
        return {"available_usd": 0, "can_run": False}


# ── Account prioritizer ────────────────────────────────────────────────────────

def _get_last_scraped_map() -> Dict[str, str]:
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
    scored = [
        (u, _hours_since_scraped(u, last_map))
        for u in usernames
        if _hours_since_scraped(u, last_map) >= MIN_HOURS_BETWEEN_SCRAPES
    ]
    scored.sort(key=lambda x: -x[1])   # most stale first
    selected = [u for u, _ in scored[:max_accs]]
    skipped  = len(usernames) - len(selected)
    log.info(
        "[%s] Selected %d/%d accounts -- %d skipped (scraped recently)",
        acc_type, len(selected), len(usernames), skipped,
    )
    return selected


# ── Actor: apify/instagram-scraper ─────────────────────────────────────────────

def _build_actor_input(username: str, results_limit: int) -> Dict:
    """
    Build input for apify/instagram-scraper.

    Key decisions:
    - directUrls: full profile URL required (not just username string)
    - resultsType: "reels" — returns only Video/clips posts, no images or carousels
    - onlyPostsNewerThan: 60 days ago — prevents old pinned posts from wasting quota
    - addParentData: false — reduces response payload size
    """
    sixty_days_ago = (datetime.utcnow() - timedelta(days=60)).strftime("%Y-%m-%d")
    return {
        "directUrls":         [f"https://www.instagram.com/{username}/"],
        "resultsType":        "reels",
        "resultsLimit":       results_limit,
        "onlyPostsNewerThan": sixty_days_ago,
        "addParentData":      False,
    }


def _run_actor(username: str, results_limit: int) -> Optional[Tuple[str, str]]:
    """
    Start apify/instagram-scraper for a single username.
    Returns (run_id, dataset_id) on success, None on failure.
    Raises RuntimeError("apify_hard_limit_exceeded") if monthly cap is hit.
    """
    run_input = _build_actor_input(username, results_limit)

    log.debug(
        "Starting actor for @%s | limit=%d | newerThan=%s",
        username, results_limit, run_input["onlyPostsNewerThan"],
    )

    resp = requests.post(
        f"{BASE}/acts/apify~instagram-scraper/runs",
        params={"token": APIFY_API_TOKEN},
        json=run_input,
        timeout=30,
    )

    if resp.status_code == 403:
        try:
            err = resp.json().get("error", {})
        except Exception:
            err = {}
        err_type = err.get("type", "")
        if err_type in ("platform-feature-disabled", "monthly-usage-exceeded"):
            log.error(
                "APIFY HARD LIMIT HIT -- Monthly cap exceeded. "
                "Stopping pipeline immediately. Reset: 1st of next month."
            )
            raise RuntimeError("apify_hard_limit_exceeded")
        log.error("Actor start failed 403: %s", resp.text[:300])
        return None

    if resp.status_code not in (200, 201):
        log.error("Actor start failed %s: %s", resp.status_code, resp.text[:300])
        return None

    data       = resp.json().get("data", {})
    run_id     = data.get("id")
    dataset_id = data.get("defaultDatasetId")
    log.info("Run started for @%s -> run_id=%s", username, run_id)
    return run_id, dataset_id


def _poll_run(run_id: str, timeout_s: int = POLL_TIMEOUT_S) -> bool:
    """
    Poll until run succeeds, fails, or times out.
    Timeout is 90s — fast-fails accounts with bot-detection / compute hangs.
    On timeout, aborts the run to prevent further credit drain.
    """
    url    = f"{BASE}/actor-runs/{run_id}"
    params = {"token": APIFY_API_TOKEN}
    waited = 0

    while waited < timeout_s:
        time.sleep(POLL_INTERVAL_S)
        waited += POLL_INTERVAL_S
        try:
            state = (
                requests.get(url, params=params, timeout=10)
                .json().get("data", {}).get("status", "RUNNING")
            )
            log.debug("Run %s -> %s (%ds elapsed)", run_id, state, waited)

            if state == "SUCCEEDED":
                log.info("Run %s succeeded after %ds", run_id, waited)
                return True
            if state in ("FAILED", "ABORTED", "TIMED-OUT"):
                log.error("Run %s terminal state: %s", run_id, state)
                return False

        except Exception as e:
            log.warning("Poll error for run %s: %s", run_id, e)

    log.error(
        "Run %s timed out after %ds -- aborting to prevent credit drain.",
        run_id, timeout_s,
    )
    # Abort the hung run to stop it consuming more compute credits
    try:
        requests.post(
            f"{BASE}/actor-runs/{run_id}/abort",
            params={"token": APIFY_API_TOKEN},
            timeout=10,
        )
        log.info("Aborted hung run %s", run_id)
    except Exception:
        pass

    return False


def _fetch_dataset(dataset_id: str, limit: int) -> List[Dict]:
    resp = requests.get(
        f"{BASE}/datasets/{dataset_id}/items",
        params={"token": APIFY_API_TOKEN, "limit": limit},
        timeout=60,
    )
    items = resp.json() if resp.status_code == 200 else []
    log.info("Fetched %d raw items from dataset %s", len(items), dataset_id)
    return items


def _actual_cost(run_id: str) -> float:
    """Fetch the real cost of a completed run from the Apify API."""
    try:
        return round(
            requests.get(
                f"{BASE}/actor-runs/{run_id}",
                params={"token": APIFY_API_TOKEN},
                timeout=10,
            ).json().get("data", {}).get("usageTotalUsd", 0.0),
            6,
        )
    except Exception:
        return 0.0


# ── Data normaliser ────────────────────────────────────────────────────────────

def _normalise(item: Dict, username: str, account_type: str) -> Optional[Dict]:
    """
    Map raw apify/instagram-scraper (resultsType=reels) item to clean reel document.

    Field names verified against real API output (leverageedu, 2026-03-12):
        type            : "Video"          — always Video for reels endpoint
        productType     : "clips"          — confirms it's a Reel
        videoPlayCount  : total plays      — USE THIS for views (3-11x higher)
        videoViewCount  : unique viewers   — lower, less representative
        likesCount      : likes
        commentsCount   : comments
        timestamp       : ISO datetime
        shortCode       : unique ID        — used for MongoDB dedup
        videoUrl        : CDN mp4 link
        displayUrl      : thumbnail image
        isPinned        : bool             — skip these
        musicInfo       : {song_name, artist_name, uses_original_audio, audio_id}

    Filters applied:
        1. isPinned=True  -> skip (old evergreen posts pollute trend analysis)
        2. type != Video  -> skip (safety net, shouldn't appear with resultsType=reels)
        3. No URL         -> skip (can't store or link to reel)
    """
    # Filter 1: Skip pinned posts
    if item.get("isPinned") is True:
        log.debug("Skipping pinned post %s by @%s", item.get("shortCode"), username)
        return None

    # Filter 2: Skip non-video (safety net)
    post_type = item.get("type", "Video")
    if post_type and post_type != "Video":
        log.debug("Skipping non-video (type=%s) by @%s", post_type, username)
        return None

    # ── Metrics ──────────────────────────────────────────────────────────────
    views    = safe_int(item.get("videoPlayCount")) or safe_int(item.get("videoViewCount")) or 0
    likes    = safe_int(item.get("likesCount")) or 0
    comments = safe_int(item.get("commentsCount")) or 0

    # ── Identifiers ──────────────────────────────────────────────────────────
    shortcode = item.get("shortCode") or item.get("id") or ""
    url       = (
        item.get("url") or
        (f"https://www.instagram.com/p/{shortcode}/" if shortcode else None)
    )

    if not url:
        log.debug("Skipping item with no URL from @%s", username)
        return None

    # ── Content ───────────────────────────────────────────────────────────────
    caption   = clean_caption(item.get("caption") or "")
    ts        = item.get("timestamp") or ""
    video_url = item.get("videoUrl") or ""
    duration  = item.get("videoDuration") or 0

    # Thumbnail: prefer displayUrl, fall back to first image in images array
    images    = item.get("images") or []
    thumbnail = item.get("displayUrl") or (images[0] if images else "")

    # ── Audio ─────────────────────────────────────────────────────────────────
    music = item.get("musicInfo") or {}
    if isinstance(music, dict):
        song_name     = music.get("song_name") or "Original audio"
        artist_name   = music.get("artist_name") or username
        uses_original = bool(music.get("uses_original_audio", False))
        audio_id      = str(music.get("audio_id") or "")
        audio_label   = "Original audio" if uses_original else f"{song_name} - {artist_name}"
    else:
        song_name     = "Original audio"
        artist_name   = username
        uses_original = True
        audio_id      = ""
        audio_label   = "Original audio"

    log.debug(
        "@%s | views=%d | likes=%d | comments=%d | shortcode=%s",
        username, views, likes, comments, shortcode,
    )

    return {
        # Identity
        "competitor":          username.lower(),
        "account_type":        account_type,
        "shortcode":           shortcode,
        "reel_url":            url,
        "video_url":           video_url,
        "thumbnail_url":       thumbnail,

        # Content
        "caption":             caption,
        "duration_seconds":    round(float(duration), 1),

        # Metrics
        "views":               views,
        "likes":               likes,
        "comments":            comments,
        "engagement_rate":     engagement_rate(likes, comments, views),

        # Audio
        "audio":               audio_label[:200],
        "audio_song":          song_name[:200],
        "audio_artist":        artist_name[:200],
        "audio_id":            audio_id,
        "uses_original_audio": uses_original,

        # AI fields (populated later by ai_analysis.py)
        "transcript":          "",
        "ai_analysis":         {},

        # Timestamps
        "date":                ts[:10] if ts else today_str(),
        "scraped_at":          today_str(),
    }


def _debug_raw_item(item: Dict, username: str) -> None:
    """Log key fields from first raw item to verify field mapping is correct."""
    log.info("--- RAW ITEM DEBUG for @%s ---", username)
    skip_keys = {
        "images", "videoUrl", "audioUrl", "displayUrl",
        "latestComments", "childPosts", "coauthorProducers", "taggedUsers",
    }
    for key, val in item.items():
        if key not in skip_keys:
            log.info("  %-25s : %s", key, str(val)[:150])
    log.info("--- END DEBUG ---")


# ── Usage report ───────────────────────────────────────────────────────────────

def _usage_report(
    reels:         int,
    cost:          float,
    credit_before: float,
    accounts_done: int,
) -> None:
    remaining = max(0.0, credit_before - cost - SAFETY_BUFFER_USD)
    log.info("")
    log.info("╔══════════════════════════════════════════╗")
    log.info("║         APIFY USAGE REPORT               ║")
    log.info("╠══════════════════════════════════════════╣")
    log.info("║  Accounts scraped : %-4d                 ║", accounts_done)
    log.info("║  Reels collected  : %-4d                 ║", reels)
    log.info("║  This run cost    : $%-8.4f             ║", cost)
    log.info("║  Credit remaining : $%-8.4f             ║", remaining)
    log.info("╚══════════════════════════════════════════╝")
    if remaining < 1.0:
        log.warning("Under $1.00 remaining -- consider upgrading to Starter ($29/mo)")
    if remaining < 0.50:
        log.error("Under $0.50 remaining -- pipeline will auto-pause next run")


# ── Main entry point ───────────────────────────────────────────────────────────

def scrape_all_accounts(
    company_accounts: List[str],
    creator_accounts: List[str],
) -> List[Dict]:
    """
    Scrape reels for all accounts using apify/instagram-scraper with resultsType=reels.

    Actor:   apify/instagram-scraper  (correct — NOT apify~instagram-reel-scraper)
    Pricing: $0.0023/result
    Cost:    21 accounts x 10 reels = 210 results = ~$0.48 per full run
    Free tier ($5/mo): ~10 full pipeline runs per month

    Data quality guarantees:
    - Only reels (no images, no carousels)
    - Only non-pinned posts (no old viral content polluting trends)
    - Only posts from last 60 days
    - videoPlayCount used for views (most accurate metric)
    """
    log.info("=" * 55)
    log.info("  APIFY INSTAGRAM SCRAPER")
    log.info("  Actor : apify/instagram-scraper")
    log.info("  Type  : reels | Cost: $%.4f/result", COST_PER_RESULT_USD)
    log.info("=" * 55)

    # 1. Credit check
    credit = get_credit_balance()
    if not credit["can_run"]:
        return []

    available   = credit["available_usd"]
    max_results = min(credit["max_results"], MAX_RESULTS_PER_RUN)
    per_acc     = RESULTS_PER_ACCOUNT

    # 2. Split budget: 60% company, 40% creator
    total_slots   = max(1, max_results // per_acc)
    company_slots = max(1, math.floor(total_slots * 0.60))
    creator_slots = max(1, math.floor(total_slots * 0.40))

    log.info(
        "Budget: $%.4f -> %d result slots | %d company | %d creator",
        available, max_results, company_slots * per_acc, creator_slots * per_acc,
    )

    # 3. Prioritize (most stale accounts first)
    last_map    = _get_last_scraped_map()
    sel_company = _prioritize(company_accounts, "company", last_map, company_slots)
    sel_creator = _prioritize(creator_accounts, "creator", last_map, creator_slots)

    if not sel_company and not sel_creator:
        log.warning(
            "No accounts need scraping -- all scraped within %dh",
            MIN_HOURS_BETWEEN_SCRAPES,
        )
        return []

    # 4. Scrape each account individually
    all_reels           = []
    total_cost          = 0.0
    accounts_done       = 0
    first_item_debugged = False

    for accs, acc_type in [(sel_company, "company"), (sel_creator, "creator")]:
        if not accs:
            continue

        log.info(
            "[%s] Scraping %d accounts x %d reels each",
            acc_type, len(accs), per_acc,
        )

        for username in accs:

            # Re-check budget before each account
            remaining_budget = available - total_cost
            if remaining_budget < MIN_CREDIT_TO_RUN_USD:
                log.warning(
                    "Budget running low ($%.4f remaining) -- stopping early",
                    remaining_budget,
                )
                break

            log.info("[%s] -- Scraping @%s --", acc_type, username)

            # Start actor run
            try:
                result = _run_actor(username, per_acc)
            except RuntimeError as e:
                if "apify_hard_limit_exceeded" in str(e):
                    log.error("Hard limit hit -- aborting all remaining accounts")
                    _usage_report(
                        reels         = len(all_reels),
                        cost          = total_cost,
                        credit_before = available + SAFETY_BUFFER_USD,
                        accounts_done = accounts_done,
                    )
                    return all_reels
                raise

            if not result:
                log.warning("[%s] Skipping @%s -- actor failed to start", acc_type, username)
                continue

            run_id, dataset_id = result

            # Poll for completion (90s timeout with auto-abort)
            succeeded = _poll_run(run_id)

            # Always record cost — compute is consumed even on failure/timeout
            cost        = _actual_cost(run_id)
            total_cost += cost

            if not succeeded:
                log.warning(
                    "[%s] @%s run did not succeed | cost $%.5f | total $%.4f",
                    acc_type, username, cost, total_cost,
                )
                continue

            # Fetch and normalise results
            raw = _fetch_dataset(dataset_id, limit=per_acc + 5)

            if cost > MAX_COST_PER_ACCOUNT_USD:
                log.warning(
                    "[%s] @%s cost $%.4f -- over per-account cap of $%.3f",
                    acc_type, username, cost, MAX_COST_PER_ACCOUNT_USD,
                )

            accounts_done += 1
            log.info(
                "[%s] @%s -> %d raw items | cost $%.5f | total $%.4f",
                acc_type, username, len(raw), cost, total_cost,
            )

            # Debug field names on first successful item (run once per pipeline execution)
            if raw and not first_item_debugged:
                _debug_raw_item(raw[0], username)
                first_item_debugged = True

            count_before = len(all_reels)
            for item in raw:
                doc = _normalise(item, username, acc_type)
                if doc:
                    all_reels.append(doc)

            added    = len(all_reels) - count_before
            filtered = len(raw) - added
            log.info(
                "[%s] @%s -> %d added | %d filtered (pinned/non-video/no-url)",
                acc_type, username, added, filtered,
            )

            time.sleep(3)  # polite pause between accounts

    # 5. Final usage report
    _usage_report(
        reels         = len(all_reels),
        cost          = total_cost,
        credit_before = available + SAFETY_BUFFER_USD,
        accounts_done = accounts_done,
    )

    log.info("Scrape complete -- %d total reels collected", len(all_reels))
    return all_reels


def scrape_profiles(
    usernames:           List[str],
    account_type:        str,
    results_per_account: int = RESULTS_PER_ACCOUNT,
) -> List[Dict]:
    """Backwards-compatible wrapper used by run_pipeline.py."""
    return scrape_all_accounts(
        company_accounts=usernames if account_type == "company" else [],
        creator_accounts=usernames if account_type == "creator" else [],
    )
