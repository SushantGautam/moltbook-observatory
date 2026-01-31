"""Simple sentiment analysis using TextBlob."""

from textblob import TextBlob
from statistics import mean
from datetime import datetime, timedelta

# Cache sentiment results
_sentiment_cache = {}
SENTIMENT_CACHE_TTL = 600  # 10 minutes


def analyze_sentiment(text: str) -> float:
    """
    Analyze sentiment of text.
    
    Returns polarity from -1.0 (negative) to +1.0 (positive).
    """
    if not text:
        return 0.0
    
    blob = TextBlob(text)
    return blob.sentiment.polarity


def get_sentiment_label(polarity: float) -> str:
    """Get a human-readable label for sentiment polarity."""
    if polarity >= 0.3:
        return "positive"
    elif polarity <= -0.3:
        return "negative"
    else:
        return "neutral"


def get_sentiment_emoji(polarity: float) -> str:
    """Get an emoji representing the sentiment."""
    if polarity >= 0.5:
        return "😊"
    elif polarity >= 0.2:
        return "🙂"
    elif polarity <= -0.5:
        return "😞"
    elif polarity <= -0.2:
        return "😐"
    else:
        return "😶"


def average_sentiment(texts: list[str]) -> float:
    """Calculate average sentiment across multiple texts."""
    if not texts:
        return 0.0
    
    scores = [analyze_sentiment(t) for t in texts if t]
    return mean(scores) if scores else 0.0


async def get_recent_sentiment(hours: int = 24) -> dict:
    """Get average sentiment for recent posts using optimized sampling and caching."""
    from observatory.database.connection import execute_query
    
    # Check cache first
    cache_key = f"sentiment_{hours}"
    now = datetime.utcnow()
    if cache_key in _sentiment_cache:
        cached_result, cached_time = _sentiment_cache[cache_key]
        if (now - cached_time).total_seconds() < SENTIMENT_CACHE_TTL:
            return cached_result
    
    start = (now - timedelta(hours=hours)).isoformat()
    
    # Optimize: Get only a sample of recent posts instead of all
    # Sample 500 posts max to avoid processing huge amounts of text
    posts = await execute_query("""
        SELECT title, content FROM posts
        WHERE created_at >= ?
        ORDER BY created_at DESC
        LIMIT 500
    """, (start,))
    
    if not posts:
        result = {"polarity": 0.0, "label": "neutral", "emoji": "😶", "sample_size": 0}
        _sentiment_cache[cache_key] = (result, now)
        return result
    
    # Only analyze title + content if both present (avoid empty strings)
    texts = [f"{p.get('title', '')} {p.get('content', '')}".strip() 
             for p in posts if p.get('title') or p.get('content')]
    
    if not texts:
        result = {"polarity": 0.0, "label": "neutral", "emoji": "😶", "sample_size": 0}
        _sentiment_cache[cache_key] = (result, now)
        return result
    
    avg = average_sentiment(texts)
    
    result = {
        "polarity": round(avg, 2),
        "label": get_sentiment_label(avg),
        "emoji": get_sentiment_emoji(avg),
        "sample_size": len(texts),
    }
    
    _sentiment_cache[cache_key] = (result, now)
    return result
