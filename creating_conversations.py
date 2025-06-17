import logging
from datetime import datetime
from db_repository import (
    get_connection,
    get_airline_id,
    insert_conversation,
    insert_conversation_tweets,
    truncate_tables,
    get_screen_name_by_id
)
from tqdm import tqdm
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Dictionary of known airlines with their IDs
KNOWN_AIRLINES = {
    'KLM': 106062176,
    'AirFrance': 106062176,
    'British_Airways': 18332190,
    'AmericanAir': 22536055,
    'Lufthansa': 124476322,
    'AirBerlin': 26223583,
    'AirBerlin_assist': 2182373406,
    'easyJet': 38676903,
    'RyanAir': 1542862735,
    'SingaporeAir': 253340062,
    'Qantas': 218730857,
    'EtihadAirways': 45621423,
    'VirginAtlantic': 20626359
}

def fetch_conversation_components(conn, airline_id):
    """
    Efficiently fetch all potential conversation components in a single query.
    Returns tweets organized by their relationships.
    """
    logger.info("Fetching potential conversation components...")
    
    # Base query for conversation components
    base_query = """
    WITH conversation_tweets AS (
        -- Start with airline replies (these are conversation starters)
        SELECT id, user_id, in_reply_to_status_id, created_at
        FROM tweet
        WHERE user_id = ? 
        AND in_reply_to_status_id IS NOT NULL

        UNION

        -- Add tweets that airline replied to
        SELECT t.id, t.user_id, t.in_reply_to_status_id, t.created_at
        FROM tweet t
        INNER JOIN tweet airline_replies ON airline_replies.in_reply_to_status_id = t.id
        WHERE airline_replies.user_id = ?

        UNION

        -- Add context tweets (tweets before the ones airline replied to)
        SELECT t.id, t.user_id, t.in_reply_to_status_id, t.created_at
        FROM tweet t
        INNER JOIN tweet parent_tweets ON t.id = parent_tweets.in_reply_to_status_id
        WHERE parent_tweets.id IN (
            SELECT in_reply_to_status_id 
            FROM tweet 
            WHERE user_id = ? 
            AND in_reply_to_status_id IS NOT NULL
        )

        UNION

        -- Add follow-up replies to airline tweets
        SELECT t.id, t.user_id, t.in_reply_to_status_id, t.created_at
        FROM tweet t
        INNER JOIN tweet airline_tweets ON t.in_reply_to_status_id = airline_tweets.id
        WHERE airline_tweets.user_id = ?
    )
    """
    
    # First get the count for progress bar
    count_sql = base_query + "\nSELECT COUNT(*) as total FROM conversation_tweets"
    cur = conn.execute(count_sql, (airline_id, airline_id, airline_id, airline_id))
    total_tweets = cur.fetchone()[0]
    
    # Now get the actual data
    data_sql = base_query + """
    SELECT 
        id,
        user_id,
        in_reply_to_status_id,
        created_at
    FROM conversation_tweets
    ORDER BY created_at
    """
    
    cur = conn.execute(data_sql, (airline_id, airline_id, airline_id, airline_id))
    
    # Organize tweets into useful data structures with progress bar
    tweets_by_id = {}
    replies_to = defaultdict(list)
    
    with tqdm(total=total_tweets, desc="Loading tweets") as pbar:
        for row in cur:
            tweets_by_id[row.id] = row
            if row.in_reply_to_status_id:
                replies_to[row.in_reply_to_status_id].append(row.id)
            pbar.update(1)
    
    return tweets_by_id, replies_to

def build_conversation(tweet_id, tweets_by_id, replies_to, airline_id, seen_ids):
    """
    Build a valid conversation starting from an airline reply tweet.
    Returns None if the conversation is invalid or uses already seen tweets.
    """
    tweet = tweets_by_id.get(tweet_id)
    if not tweet or tweet.user_id != airline_id or tweet.id in seen_ids:
        return None
        
    # Get the parent tweet
    parent = tweets_by_id.get(tweet.in_reply_to_status_id)
    if not parent:
        return None
        
    original_user_id = parent.user_id
    allowed_user_ids = {airline_id, original_user_id}
    
    # Build conversation chain
    convo_ids = []
    
    # 1. Get context chain before parent
    current = parent
    while current and current.in_reply_to_status_id:
        prev = tweets_by_id.get(current.in_reply_to_status_id)
        if not prev or prev.user_id not in allowed_user_ids:
            break
        convo_ids.insert(0, prev.id)
        current = prev
    
    # 2. Add parent tweet
    convo_ids.append(parent.id)
    
    # 3. Add airline's reply
    convo_ids.append(tweet.id)
    
    # 4. Add follow-up replies (breadth-first to maintain conversation flow)
    to_process = [tweet.id]
    while to_process:
        current_id = to_process.pop(0)
        for reply_id in replies_to.get(current_id, []):
            reply = tweets_by_id.get(reply_id)
            if reply and reply.user_id in allowed_user_ids:
                convo_ids.append(reply.id)
                to_process.append(reply.id)
    
    # Verify no overlap with seen tweets
    if seen_ids.intersection(convo_ids):
        return None
        
    # Verify all participants are allowed
    participants = {tweets_by_id[tid].user_id for tid in convo_ids}
    if not participants <= allowed_user_ids:
        return None
        
    return original_user_id, parent.id, convo_ids

def mine_conversations(conn, airline_id):
    """
    Extract valid conversations using optimized batch processing.
    """
    # Fetch all potential conversation components efficiently
    tweets_by_id, replies_to = fetch_conversation_components(conn, airline_id)
    
    # Find airline replies that could start conversations
    potential_starts = [
        tweet_id for tweet_id, tweet in tweets_by_id.items()
        if tweet.user_id == airline_id and tweet.in_reply_to_status_id
    ]
    
    logger.info(f"Processing {len(potential_starts)} potential conversation starts")
    
    seen_ids = set()
    conversations = []
    
    # Process each potential conversation with progress bar
    with tqdm(total=len(potential_starts), desc="Mining conversations") as pbar:
        for tweet_id in potential_starts:
            result = build_conversation(tweet_id, tweets_by_id, replies_to, airline_id, seen_ids)
            if result:
                user_id, root_id, convo_ids = result
                seen_ids.update(convo_ids)
                conversations.append((user_id, root_id, convo_ids))
            pbar.update(1)
    
    return conversations

def format_conversation(conn, user_id, root_id, tweet_ids):
    """
    Format a single conversation in a readable way.
    Returns a list of strings representing each line of the conversation.
    """
    lines = []
    
    # Get all tweets in this conversation with their details
    placeholders = ','.join(['?' for _ in tweet_ids])
    cur = conn.execute(f"""
        SELECT 
            t.id,
            t.created_at,
            t.text,
            u.screen_name,
            t.in_reply_to_status_id
        FROM tweet t
        JOIN [user] u ON t.user_id = u.id
        WHERE t.id IN ({placeholders})
        ORDER BY t.created_at
    """, tweet_ids)
    
    tweets = cur.fetchall()
    
    # Convert timestamps and format each tweet
    formatted_tweets = []
    for tweet in tweets:
        # Convert timestamp to milliseconds since epoch
        try:
            if isinstance(tweet.created_at, str):
                timestamp = int(datetime.strptime(tweet.created_at, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
            else:
                timestamp = int(tweet.created_at.timestamp() * 1000)
        except (ValueError, AttributeError):
            timestamp = 0
            
        formatted_tweets.append({
            'timestamp': timestamp,
            'screen_name': tweet.screen_name,
            'text': tweet.text,
            'id': tweet.id,
            'in_reply_to_status_id': tweet.in_reply_to_status_id
        })
    
    # Sort by timestamp
    formatted_tweets.sort(key=lambda x: x['timestamp'])
    
    return formatted_tweets

def print_conversations(conn, conversations, airline_screen_name, output_file=None):
    """
    Print all conversations in a readable format.
    If output_file is provided, write to that file instead of printing to console.
    """
    def write_line(line):
        if output_file:
            output_file.write(line + '\n')
        else:
            print(line)
    
    total = len(conversations)
    logger.info(f"Formatting {total} conversations...")
    
    # Add progress bar for conversation formatting
    with tqdm(total=total, desc="Formatting conversations") as pbar:
        for i, (user_id, root_id, tweet_ids) in enumerate(conversations, 1):
            try:
                formatted_tweets = format_conversation(conn, user_id, root_id, tweet_ids)
                
                write_line(f"\n--- Conversation {i}/{total} Start ({airline_screen_name}) ---")
                
                for tweet in formatted_tweets:
                    text = tweet['text'].replace('\n', ' ').strip()
                    line = f"(Time: {tweet['timestamp']}) @{tweet['screen_name']}: {text}"
                    write_line(line)
                
                write_line(f"--- Conversation End ---")
                
            except Exception as e:
                logger.error(f"Error formatting conversation {i}: {str(e)}")
                continue
                
            pbar.update(1)

def mine_and_store_conversations(airline_screen_name, output_path=None):
    """
    Main function to extract, store, and display conversations for a given airline.
    Args:
        airline_screen_name: The screen name of the airline (e.g., "AmericanAir")
        output_path: Optional path to save formatted conversations to a file
    """
    conn = get_connection()
    try:
        airline_id = get_airline_id(conn, airline_screen_name)
        if not airline_id:
            logger.error(f"Could not find airline ID for {airline_screen_name}")
            return
            
        logger.info(f"Processing conversations for {airline_screen_name} (ID: {airline_id})")
        
        # Mine conversations
        conversations = mine_conversations(conn, airline_id)
        logger.info(f"Found {len(conversations)} valid conversations")
        
        # Store conversations in database with progress bar
        with tqdm(total=len(conversations), desc="Storing conversations") as pbar:
            for user_id, root_id, tweet_ids in conversations:
                try:
                    conv_id = insert_conversation(conn, user_id, airline_id, root_id)
                    insert_conversation_tweets(conn, conv_id, tweet_ids)
                except Exception as e:
                    logger.error(f"Error storing conversation: {str(e)}")
                    continue
                pbar.update(1)
        
        # Print or save formatted conversations
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                print_conversations(conn, conversations, airline_screen_name, f)
            logger.info(f"Conversations saved to {output_path}")
        else:
            print_conversations(conn, conversations, airline_screen_name)
                
        logger.info(f"Successfully processed all conversations for {airline_screen_name}")
        
    except Exception as e:
        logger.error(f"Error processing conversations: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        # Get list of available airlines
        print("\nAvailable airlines:")
        for airline, airline_id in KNOWN_AIRLINES.items():
            print(f"- {airline}")

        # Check existing conversations
        conn = get_connection()
        cur = conn.execute("""
            SELECT u.screen_name, COUNT(*) as conv_count 
            FROM conversation c 
            JOIN [user] u ON c.airline_id = u.id 
            GROUP BY u.screen_name
        """)
        existing = {row[0]: row[1] for row in cur.fetchall()}
        
        if existing:
            print("\nExisting conversations in database:")
            for airline, count in existing.items():
                print(f"- {airline}: {count} conversations")
        
        airline_screen_name = input("\nEnter the airline screen name to analyze: ")
        
        if airline_screen_name not in KNOWN_AIRLINES:
            print(f"Warning: {airline_screen_name} is not a known airline. Available airlines are: {list(KNOWN_AIRLINES.keys())}")
            exit()
        
        if airline_screen_name in existing:
            clear = input(f"\nFound {existing[airline_screen_name]} existing conversations for {airline_screen_name}. Clear them? (y/n): ").lower()
            if clear == 'y':
                print("Clearing existing conversations...")
                tables = ['conversation_tweet', 'conversation']
                with tqdm(total=len(tables), desc="Truncating tables") as pbar:
                    for table in tables:
                        truncate_tables([table])
                        pbar.update(1)
            else:
                print("Keeping existing conversations. New conversations will be added to the existing ones.")
            
        output_file = f"conversations_{airline_screen_name}_output.txt"
        mine_and_store_conversations(airline_screen_name, output_file)
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
