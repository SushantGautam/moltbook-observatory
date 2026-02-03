"""Process API responses into database records."""

from datetime import datetime
from observatory.database.connection import get_db


async def process_posts(posts_data: dict) -> int:
    """
    Process posts from API response and store in database.
    Also extracts and updates agent/submolt data from post metadata.
    
    Returns number of new posts inserted.
    """
    db = await get_db()
    posts = posts_data.get("posts", [])
    if not posts:
        return 0
    
    new_count = 0
    now = datetime.utcnow().isoformat()
    
    for post in posts:
        post_id = post.get("id")
        if not post_id:
            continue
        
        # Calculate score from upvotes/downvotes
        upvotes = post.get("upvotes", 0) or 0
        downvotes = post.get("downvotes", 0) or 0
        score = upvotes - downvotes
        
        # Check if post exists
        async with db.execute("SELECT id FROM posts WHERE id = ?", (post_id,)) as cursor:
            exists = await cursor.fetchone()
        
        if exists:
            # Update existing post (score might have changed)
            await db.execute("""
                UPDATE posts SET
                    score = ?,
                    comment_count = ?,
                    is_pinned = ?
                WHERE id = ?
            """, (
                score,
                post.get("comment_count", 0) or 0,
                post.get("is_pinned", False),
                post_id,
            ))
        else:
            # Get author info - API uses "author" not "agent"
            author = post.get("author") or post.get("agent") or {}
            if isinstance(author, str):
                author = {"name": author}
            author_name = author.get("name", "") if author else ""
            
            # Handle submolt being a dict or string
            submolt_data = post.get("submolt", "")
            submolt_name = ""
            if isinstance(submolt_data, dict):
                submolt_name = submolt_data.get("name", "")
                # Ensure submolt exists with its data
                if submolt_name:
                    await ensure_submolt(submolt_name, submolt_data)
            else:
                submolt_name = submolt_data
                # Ensure submolt exists (will be minimal data)
                if submolt_name:
                    await ensure_submolt(submolt_name)
            
            # Ensure agent exists BEFORE inserting post (for foreign key constraint)
            if author_name:
                await ensure_agent(author_name, author if isinstance(author, dict) else None)
            
            await db.execute("""
                INSERT INTO posts (id, agent_id, agent_name, submolt, title, content, url, score, comment_count, created_at, fetched_at, is_pinned)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                post_id,
                author.get("id") if isinstance(author, dict) else None,
                author_name,
                submolt_name,
                post.get("title", "") or "",
                post.get("content", "") or "",
                post.get("url"),
                score,
                post.get("comment_count", 0) or 0,
                post.get("created_at"),
                now,
                post.get("is_pinned", False),
            ))
            new_count += 1
    
    await db.commit()
    return new_count


async def ensure_agent(name: str, agent_data: dict = None) -> None:
    """Ensure an agent exists in the database and update with latest available data."""
    db = await get_db()
    
    async with db.execute("SELECT name FROM agents WHERE name = ?", (name,)) as cursor:
        exists = await cursor.fetchone()
    
    now = datetime.utcnow().isoformat()
    
    if exists:
        # Update existing agent with fresh data if available
        if agent_data:
            await db.execute("""
                UPDATE agents SET
                    description = ?,
                    karma = ?,
                    follower_count = ?,
                    following_count = ?,
                    is_claimed = ?,
                    owner_x_handle = ?,
                    avatar_url = ?,
                    last_seen_at = ?
                WHERE name = ?
            """, (
                agent_data.get("description", ""),
                agent_data.get("karma", 0) or 0,
                agent_data.get("follower_count", 0) or 0,
                agent_data.get("following_count", 0) or 0,
                agent_data.get("is_claimed", False),
                agent_data.get("owner", {}).get("x_handle") if isinstance(agent_data.get("owner"), dict) else None,
                agent_data.get("avatar_url"),
                now,
                name,
            ))
        else:
            # Just update last_seen
            await db.execute("UPDATE agents SET last_seen_at = ? WHERE name = ?", (now, name))
    else:
        # Insert new agent
        await db.execute("""
            INSERT INTO agents (id, name, description, karma, follower_count, following_count, is_claimed, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            agent_data.get("id", name) if agent_data else name,
            name,
            agent_data.get("description", "") if agent_data else "",
            agent_data.get("karma", 0) if agent_data else 0,
            agent_data.get("follower_count", 0) if agent_data else 0,
            agent_data.get("following_count", 0) if agent_data else 0,
            agent_data.get("is_claimed", False) if agent_data else False,
            now,
            now,
        ))
    
    await db.commit()


async def process_agent_profile(profile_data: dict) -> None:
    """Process and store agent profile data."""
    db = await get_db()
    
    agent = profile_data.get("agent", {})
    if not agent:
        return
    
    name = agent.get("name")
    if not name:
        return
    
    now = datetime.utcnow().isoformat()
    owner = agent.get("owner", {})
    
    async with db.execute("SELECT name FROM agents WHERE name = ?", (name,)) as cursor:
        exists = await cursor.fetchone()
    
    if exists:
        await db.execute("""
            UPDATE agents SET
                description = ?,
                karma = ?,
                follower_count = ?,
                following_count = ?,
                is_claimed = ?,
                owner_x_handle = ?,
                last_seen_at = ?,
                created_at = ?,
                avatar_url = ?
            WHERE name = ?
        """, (
            agent.get("description", ""),
            agent.get("karma", 0),
            agent.get("follower_count", 0),
            agent.get("following_count", 0),
            agent.get("is_claimed", False),
            owner.get("x_handle") if owner else None,
            now,
            agent.get("created_at"),
            agent.get("avatar_url"),
            name,
        ))
    else:
        await db.execute("""
            INSERT INTO agents (id, name, description, karma, follower_count, following_count, is_claimed, owner_x_handle, first_seen_at, last_seen_at, created_at, avatar_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            agent.get("id", name),
            name,
            agent.get("description", ""),
            agent.get("karma", 0),
            agent.get("follower_count", 0),
            agent.get("following_count", 0),
            agent.get("is_claimed", False),
            owner.get("x_handle") if owner else None,
            now,
            now,
            agent.get("created_at"),
            agent.get("avatar_url"),
        ))
    
    await db.commit()


async def process_agents(agents_list: list[str]) -> int:
    """
    Process a list of agent names and fetch their profiles.
    
    Returns number of agents updated.
    """
    from observatory.poller.client import get_client
    
    client = await get_client()
    updated = 0
    
    for name in agents_list:
        try:
            profile = await client.get_agent_profile(name)
            await process_agent_profile(profile)
            updated += 1
        except Exception as e:
            print(f"Error fetching profile for {name}: {e}")
    
    return updated


async def ensure_submolt(name: str, submolt_data: dict = None) -> None:
    """Ensure a submolt exists in the database and update with latest available data."""
    db = await get_db()
    
    async with db.execute("SELECT name FROM submolts WHERE name = ?", (name,)) as cursor:
        exists = await cursor.fetchone()
    
    now = datetime.utcnow().isoformat()
    
    if exists:
        # Update existing submolt with fresh data if available
        if submolt_data:
            await db.execute("""
                UPDATE submolts SET
                    display_name = ?,
                    description = ?,
                    subscriber_count = ?,
                    post_count = ?,
                    avatar_url = ?,
                    banner_url = ?
                WHERE name = ?
            """, (
                submolt_data.get("display_name", name),
                submolt_data.get("description", ""),
                submolt_data.get("subscriber_count", 0) or 0,
                submolt_data.get("post_count", 0) or 0,
                submolt_data.get("avatar_url"),
                submolt_data.get("banner_url"),
                name,
            ))
    else:
        # Insert new submolt
        await db.execute("""
            INSERT INTO submolts (name, display_name, description, subscriber_count, post_count, created_at, first_seen_at, avatar_url, banner_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name,
            submolt_data.get("display_name", name) if submolt_data else name,
            submolt_data.get("description", "") if submolt_data else "",
            submolt_data.get("subscriber_count", 0) if submolt_data else 0,
            0,  # Will be calculated from posts
            submolt_data.get("created_at") if submolt_data else now,
            now,
            submolt_data.get("avatar_url") if submolt_data else None,
            submolt_data.get("banner_url") if submolt_data else None,
        ))
    
    await db.commit()


async def process_submolts(submolts_data: dict) -> int:
    """
    Process submolts from API response and store in database.
    
    Returns number of submolts processed.
    """
    db = await get_db()
    submolts = submolts_data.get("submolts", [])
    if not submolts:
        return 0
    
    count = 0
    
    for submolt in submolts:
        name = submolt.get("name")
        if not name:
            continue
        
        await ensure_submolt(name, submolt)
        count += 1
    
    return count


async def process_comments(post_id: str, comments_data: dict) -> int:
    """
    Process comments from API response and store in database.
    
    Returns number of new comments inserted.
    """
    db = await get_db()
    comments = comments_data.get("comments", [])
    if not comments:
        return 0
    
    new_count = 0
    now = datetime.utcnow().isoformat()
    
    async def process_comment(comment: dict, parent_id: str = None) -> None:
        nonlocal new_count
        
        comment_id = comment.get("id")
        if not comment_id:
            return
        
        async with db.execute("SELECT id FROM comments WHERE id = ?", (comment_id,)) as cursor:
            exists = await cursor.fetchone()
        
        # API uses "author" not "agent"
        author = comment.get("author") or comment.get("agent") or {}
        author_name = author.get("name", "") if author else ""
        
        # Calculate score from upvotes/downvotes
        upvotes = comment.get("upvotes", 0) or 0
        downvotes = comment.get("downvotes", 0) or 0
        score = upvotes - downvotes
        
        if not exists:
            # Ensure agent exists BEFORE inserting comment (FK constraint)
            if author_name:
                await ensure_agent(author_name, author)
            
            await db.execute("""
                INSERT INTO comments (id, post_id, agent_id, agent_name, parent_id, content, score, created_at, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                comment_id,
                post_id,
                author.get("id") if author else None,
                author_name,
                parent_id,
                comment.get("content", ""),
                score,
                comment.get("created_at"),
                now,
            ))
            new_count += 1
        
        # Process replies
        for reply in comment.get("replies", []):
            await process_comment(reply, comment_id)
    
    for comment in comments:
        await process_comment(comment)
    
    await db.commit()
    return new_count
