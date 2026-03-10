"""
insta_intel/database/mongo_client.py
All MongoDB interactions.  One connection pool shared across the pipeline.
"""

from __future__ import annotations
from typing import Optional, List, Dict, Any

from pymongo import MongoClient, DESCENDING, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from config.settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_REELS,
    MONGO_COLLECTION_ACCS,
)
from utils.helpers import get_logger

log = get_logger(__name__)

# ── Singleton client ─────────────────────────────────────────
_client: Optional[MongoClient] = None

def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        log.info("MongoDB connected → %s", MONGO_DB)
    return _client

def get_db():
    return get_client()[MONGO_DB]

def reels_col() -> Collection:
    return get_db()[MONGO_COLLECTION_REELS]

def accounts_col() -> Collection:
    return get_db()[MONGO_COLLECTION_ACCS]

# ── Setup indexes ─────────────────────────────────────────────
def ensure_indexes() -> None:
    reels_col().create_index("reel_url", unique=True)
    reels_col().create_index([("views", DESCENDING)])
    reels_col().create_index("competitor")
    reels_col().create_index("date")
    log.info("MongoDB indexes ensured")

# ── Reel CRUD ─────────────────────────────────────────────────
def reel_exists(reel_url: str) -> bool:
    return reels_col().count_documents({"reel_url": reel_url}, limit=1) > 0

def insert_reel(doc: Dict[str, Any]) -> bool:
    """Insert reel. Returns True if inserted, False if duplicate."""
    try:
        reels_col().insert_one(doc)
        return True
    except DuplicateKeyError:
        log.debug("Duplicate reel skipped: %s", doc.get("reel_url"))
        return False

def upsert_ai_analysis(reel_url: str, analysis: Dict[str, Any]) -> None:
    reels_col().update_one(
        {"reel_url": reel_url},
        {"$set": {"ai_analysis": analysis}},
    )

def upsert_transcript(reel_url: str, transcript: str) -> None:
    reels_col().update_one(
        {"reel_url": reel_url},
        {"$set": {"transcript": transcript}},
    )

def bulk_upsert_reels(docs: List[Dict[str, Any]]) -> Dict[str, int]:
    """Bulk upsert — skip duplicates, update existing."""
    inserted = skipped = 0
    ops = []
    for doc in docs:
        ops.append(
            UpdateOne(
                {"reel_url": doc["reel_url"]},
                {"$setOnInsert": doc},
                upsert=True,
            )
        )
    if ops:
        result = reels_col().bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        skipped  = len(ops) - inserted
    log.info("Bulk upsert: %d inserted, %d skipped", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}

# ── Query helpers ─────────────────────────────────────────────
def get_all_reels(
    competitor: Optional[str]   = None,
    account_type: Optional[str] = None,
    min_views: int              = 0,
    min_eng: float              = 0.0,
    topic: Optional[str]        = None,
    audio: Optional[str]        = None,
    date_from: Optional[str]    = None,
    date_to: Optional[str]      = None,
    limit: int                  = 500,
) -> List[Dict[str, Any]]:
    query: Dict[str, Any] = {}
    if competitor:    query["competitor"]    = competitor
    if account_type:  query["account_type"] = account_type
    if min_views > 0: query["views"]        = {"$gte": min_views}
    if min_eng   > 0: query["engagement_rate"] = {"$gte": min_eng}
    if topic:         query["ai_analysis.topic"] = {"$regex": topic, "$options": "i"}
    if audio:         query["audio"]        = {"$regex": audio,  "$options": "i"}
    if date_from or date_to:
        date_q: Dict[str, str] = {}
        if date_from: date_q["$gte"] = date_from
        if date_to:   date_q["$lte"] = date_to
        query["date"] = date_q

    cursor = reels_col().find(query, {"_id": 0}).sort("views", DESCENDING).limit(limit)
    return list(cursor)

def get_trend_data() -> List[Dict[str, Any]]:
    """Return all reels that have ai_analysis for trend aggregation."""
    return list(
        reels_col().find(
            {"ai_analysis": {"$exists": True}},
            {"_id": 0, "ai_analysis": 1, "audio": 1, "views": 1, "engagement_rate": 1}
        ).limit(1000)
    )

def get_stats() -> Dict[str, Any]:
    col = reels_col()
    pipeline = [
        {"$group": {
            "_id": None,
            "total":     {"$sum": 1},
            "avg_views": {"$avg": "$views"},
            "avg_eng":   {"$avg": "$engagement_rate"},
            "total_views": {"$sum": "$views"},
        }}
    ]
    result = list(col.aggregate(pipeline))
    if result:
        r = result[0]
        return {
            "total_reels":  r["total"],
            "avg_views":    int(r["avg_views"] or 0),
            "avg_eng":      round(r["avg_eng"] or 0, 4),
            "total_views":  int(r["total_views"] or 0),
            "competitors":  col.distinct("competitor"),
        }
    return {"total_reels": 0, "avg_views": 0, "avg_eng": 0, "total_views": 0, "competitors": []}
