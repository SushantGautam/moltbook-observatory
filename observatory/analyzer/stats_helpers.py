"""Helper functions to get stats from database."""

from observatory.database.connection import execute_query


async def get_agent_stats(agent_name: str) -> dict:
    """
    Get agent stats from database (populated by polling).
    
    Args:
        agent_name: The agent name to get stats for
    
    Returns:
        Dict with post_count calculated from posts table
    """
    # Get post count from database
    post_result = await execute_query("""
        SELECT COUNT(*) as post_count FROM posts WHERE agent_name = ?
    """, (agent_name,))
    
    return {
        "post_count": post_result[0]["post_count"] if post_result else 0,
    }

