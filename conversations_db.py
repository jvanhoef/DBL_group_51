import os
import json
import pyodbc
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
import multiprocessing as mp
import db_repository as repo

# General configuration
BATCH_SIZE = 1000  # Process this many tweets at once
NUM_PROCESSES = max(1, mp.cpu_count() - 1)  # Use all but one CPU core

def mine_conversations_batch(airline_screen_name, batch_start=0, batch_size=BATCH_SIZE):
    """Process conversations in efficient batches."""
    conn = repo.get_connection()
    airline_id = repo.get_airline_id(conn, airline_screen_name)
    
    # 1. Get only airline tweets that are replies (filtering at DB level)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.id, t.in_reply_to_status_id, t.user_id
        FROM tweet t
        WHERE t.user_id = ?
          AND t.in_reply_to_status_id IS NOT NULL
        ORDER BY t.created_at
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """, (airline_id, batch_start, batch_size))
    
    airline_replies = cursor.fetchall()
    if not airline_replies:
        return []
    
    # 2. Build a set of all tweet IDs we need to fetch (parent tweets and reply tweets)
    needed_tweet_ids = set()
    parent_ids = set()
    
    for reply in airline_replies:
        needed_tweet_ids.add(reply[0])  # Reply ID
        parent_ids.add(reply[1])        # Parent ID
    
    # 3. Get all parent tweets in one query
    parent_placeholders = ','.join(['?' for _ in parent_ids])
    cursor.execute(f"""
        SELECT t.id, t.user_id, t.in_reply_to_status_id 
        FROM tweet t
        WHERE t.id IN ({parent_placeholders})
    """, tuple(parent_ids))
    
    parent_tweets = {row[0]: row for row in cursor.fetchall()}
    for tweet_id, tweet_data in parent_tweets.items():
        needed_tweet_ids.add(tweet_id)
    
    # 4. Find all potential replies in one query
    reply_cursor = conn.cursor()
    reply_cursor.execute(f"""
        SELECT t.id, t.in_reply_to_status_id, t.user_id
        FROM tweet t
        WHERE t.in_reply_to_status_id IN ({','.join(['?' for _ in needed_tweet_ids])})
    """, tuple(needed_tweet_ids))
    
    replies = defaultdict(list)
    for row in reply_cursor.fetchall():
        reply_id, parent_id, user_id = row
        replies[parent_id].append((reply_id, user_id))
    
    # 5. Process each airline reply
    conversations = []
    seen_ids = set()
    
    for airline_reply in airline_replies:
        reply_id, parent_id, _ = airline_reply
        
        if reply_id in seen_ids or parent_id not in parent_tweets:
            continue
        
        parent_user_id = parent_tweets[parent_id][1]
        allowed_user_ids = {airline_id, parent_user_id}
        
        # Get conversation context recursively but with DB caching
        convo_ids = get_conversation_context(conn, parent_id, allowed_user_ids)
        convo_ids.append(parent_id)
        convo_ids.append(reply_id)
        
        # Get conversation replies recursively but with DB caching
        reply_ids = get_conversation_replies(reply_id, replies, allowed_user_ids)
        convo_ids.extend(reply_ids)
        
        # Check if this is a new conversation with just the two participants
        if not seen_ids.intersection(convo_ids):
            seen_ids.update(convo_ids)
            conversations.append((parent_user_id, parent_id, convo_ids))
            
            # Store in DB immediately to avoid memory buildup
            repo.insert_conversation(conn, parent_user_id, airline_id, parent_id)
            repo.insert_conversation_tweets(conn, cursor.lastrowid, convo_ids)
    
    conn.commit()
    conn.close()
    return conversations

def get_conversation_context(conn, tweet_id, allowed_user_ids, _cache=None):
    """Recursively get context with DB caching."""
    if _cache is None:
        _cache = {}
        
    if tweet_id in _cache:
        return _cache[tweet_id]
    
    cursor = conn.cursor()
    context = []
    
    # Get parent tweet info
    cursor.execute("""
        SELECT t.id, t.user_id, t.in_reply_to_status_id
        FROM tweet t
        WHERE t.id = ?
    """, (tweet_id,))
    
    row = cursor.fetchone()
    if not row or not row[2]:  # No parent or root tweet
        _cache[tweet_id] = []
        return []
        
    parent_id = row[2]
    
    # Get parent tweet
    cursor.execute("""
        SELECT t.id, t.user_id
        FROM tweet t
        WHERE t.id = ?
    """, (parent_id,))
    
    parent = cursor.fetchone()
    if not parent or parent[1] not in allowed_user_ids:
        _cache[tweet_id] = []
        return []
    
    # Add parent and get its context
    context.append(parent_id)
    parent_context = get_conversation_context(conn, parent_id, allowed_user_ids, _cache)
    context = parent_context + context
    
    _cache[tweet_id] = context
    return context

def get_conversation_replies(tweet_id, replies_map, allowed_user_ids, _visited=None):
    """Get all replies using pre-fetched replies map."""
    if _visited is None:
        _visited = set()
    
    if tweet_id in _visited:
        return []
    
    _visited.add(tweet_id)
    all_replies = []
    
    for reply_id, user_id in replies_map.get(tweet_id, []):
        if user_id in allowed_user_ids:
            all_replies.append(reply_id)
            all_replies.extend(get_conversation_replies(reply_id, replies_map, allowed_user_ids, _visited))
    
    return all_replies

def process_airline_parallel(airline_screen_name):
    """Process an airline's conversations using multiple processes."""
    conn = repo.get_connection()
    cursor = conn.cursor()
    
    # Get total count of airline replies
    airline_id = repo.get_airline_id(conn, airline_screen_name)
    cursor.execute("""
        SELECT COUNT(*)
        FROM tweet t
        WHERE t.user_id = ?
          AND t.in_reply_to_status_id IS NOT NULL
    """, (airline_id,))
    
    total_tweets = cursor.fetchone()[0]
    conn.close()
    
    # Create batch ranges
    batches = [(airline_screen_name, i, BATCH_SIZE) 
              for i in range(0, total_tweets, BATCH_SIZE)]
    
    # Process in parallel
    with mp.Pool(NUM_PROCESSES) as pool:
        results = list(tqdm(
            pool.starmap(mine_conversations_batch, batches),
            total=len(batches),
            desc=f"Mining {airline_screen_name} conversations"
        ))
    
    # Count total conversations
    total_conversations = sum(len(batch) for batch in results)
    print(f"Found {total_conversations} conversations for {airline_screen_name}")
    return total_conversations

if __name__ == "__main__":
    # Create indexes if they don't exist (one-time operation)
    conn = repo.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweet_user_id ON tweet(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweet_in_reply ON tweet(in_reply_to_status_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweet_created ON tweet(created_at)")
        conn.commit()
    except:
        pass  # Indexes might already exist or not supported
    conn.close()
    
    # Process airlines
    airlines = ["AmericanAir", "united", "SouthwestAir", "JetBlue", "Delta"]
    total = 0
    
    for airline in airlines:
        count = process_airline_parallel(airline)
        total += count
        
    print(f"Total conversations stored: {total}")
    