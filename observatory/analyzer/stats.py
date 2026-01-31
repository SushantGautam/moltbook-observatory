"""Aggregate statistics and snapshots."""

import json
from datetime import datetime, timedelta
from functools import lru_cache
from observatory.database.connection import get_db, execute_query

# Cache stats for 5 minutes to reduce query load
_stats_cache = None
_stats_cache_time = None
STATS_CACHE_TTL = 300  # 5 minutes


async def get_stats() -> dict:
    """Get current platform statistics with caching."""
    global _stats_cache, _stats_cache_time
    
    # Return cached result if still valid
    now = datetime.utcnow()
    if _stats_cache is not None and _stats_cache_time is not None:
        if (now - _stats_cache_time).total_seconds() < STATS_CACHE_TTL:
            return _stats_cache
    
    # Get all counts in a single query
    now_iso = now.isoformat()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    one_hour_ago = (now - timedelta(hours=1)).isoformat()
    one_day_ago = (now - timedelta(hours=24)).isoformat()
    
    result = await execute_query("""
        SELECT
            (SELECT COUNT(*) FROM agents) as total_agents,
            (SELECT COUNT(*) FROM posts) as total_posts,
            (SELECT COUNT(*) FROM comments) as total_comments,
            (SELECT COUNT(*) FROM submolts) as total_submolts,
            (SELECT COUNT(*) FROM posts WHERE created_at >= ?) as posts_today,
            (SELECT COUNT(DISTINCT agent_name) FROM posts WHERE created_at >= ?) as active_agents_1h,
            (SELECT COUNT(DISTINCT agent_name) FROM posts WHERE created_at >= ?) as active_agents_24h
    """, (today_start, one_hour_ago, one_day_ago))
    
    if result:
        _stats_cache = {
            "total_agents": result[0]["total_agents"],
            "total_posts": result[0]["total_posts"],
            "total_comments": result[0]["total_comments"],
            "total_submolts": result[0]["total_submolts"],
            "posts_today": result[0]["posts_today"],
            "active_agents_1h": result[0]["active_agents_1h"],
            "active_agents_24h": result[0]["active_agents_24h"],
        }
        _stats_cache_time = now
        return _stats_cache
    
    return {
        "total_agents": 0,
        "total_posts": 0,
        "total_comments": 0,
        "total_submolts": 0,
        "posts_today": 0,
        "active_agents_1h": 0,
        "active_agents_24h": 0,
    }


def invalidate_stats_cache() -> None:
    """Invalidate the stats cache."""
    global _stats_cache, _stats_cache_time
    _stats_cache = None
    _stats_cache_time = None


async def get_new_agents_today() -> list[dict]:
    """Get agents first seen today."""
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    
    return await execute_query("""
        SELECT name, description, karma, first_seen_at
        FROM agents
        WHERE first_seen_at >= ?
        ORDER BY first_seen_at DESC
        LIMIT 10
    """, (today_start,))


async def create_snapshot() -> None:
    """Create an hourly snapshot of platform metrics."""
    from observatory.analyzer.trends import get_top_words
    from observatory.analyzer.sentiment import get_recent_sentiment
    
    db = await get_db()
    stats = await get_stats()
    sentiment = await get_recent_sentiment(hours=1)
    top_words = await get_top_words(hours=1, limit=10)
    
    await db.execute("""
        INSERT INTO snapshots (
            timestamp, total_agents, total_posts, total_comments,
            active_agents_24h, avg_sentiment, top_words
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        stats["total_agents"],
        stats["total_posts"],
        stats["total_comments"],
        stats["active_agents_24h"],
        sentiment["polarity"],
        json.dumps([w["word"] for w in top_words]),
    ))
    
    await db.commit()


async def get_snapshot_history(hours: int = 168) -> list[dict]:
    """Get snapshot history for the given number of hours."""
    start = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    
    snapshots = await execute_query("""
        SELECT timestamp, total_agents, total_posts, total_comments,
               active_agents_24h, avg_sentiment, top_words
        FROM snapshots
        WHERE timestamp >= ?
        ORDER BY timestamp ASC
    """, (start,))
    
    # Parse top_words JSON
    for s in snapshots:
        if s.get("top_words"):
            try:
                s["top_words"] = json.loads(s["top_words"])
            except json.JSONDecodeError:
                s["top_words"] = []
    
    return snapshots


async def get_top_posters(limit: int = 20) -> list[dict]:
    """Get agents with the most posts."""
    return await execute_query("""
        SELECT 
            agent_name as name,
            COUNT(*) as post_count,
            SUM(score) as total_score,
            AVG(score) as avg_score,
            MAX(created_at) as last_post
        FROM posts
        WHERE agent_name IS NOT NULL AND agent_name != ''
        GROUP BY agent_name
        ORDER BY post_count DESC
        LIMIT ?
    """, (limit,))


async def get_activity_by_hour() -> list[dict]:
    """Get post activity grouped by hour of day (UTC)."""
    return await execute_query("""
        SELECT 
            CAST(strftime('%H', created_at) AS INTEGER) as hour,
            COUNT(*) as post_count
        FROM posts
        WHERE created_at IS NOT NULL
        GROUP BY hour
        ORDER BY hour ASC
    """)


async def get_submolt_activity(limit: int = 20) -> list[dict]:
    """Get submolts ranked by post activity."""
    return await execute_query("""
        SELECT 
            submolt as name,
            COUNT(*) as post_count,
            COUNT(DISTINCT agent_name) as unique_posters,
            SUM(score) as total_score,
            AVG(score) as avg_score,
            MAX(created_at) as last_post
        FROM posts
        WHERE submolt IS NOT NULL AND submolt != ''
        GROUP BY submolt
        ORDER BY post_count DESC
        LIMIT ?
    """, (limit,))

