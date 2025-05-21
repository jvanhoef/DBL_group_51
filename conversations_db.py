import pandas as pd
from db_repository import (
    get_connection,
    get_airline_id,
    get_relevant_tweets,
    insert_conversation,
    insert_conversation_tweets,
    truncate_tables
)
from tqdm import tqdm

def get_context_from(conn, tweet_id, allowed_user_ids):
    context = []
    current_id = tweet_id
    while True:
        cur = conn.execute(
            "SELECT id, user_id, in_reply_to_status_id FROM tweets WHERE id = ?", (current_id,)
        )
        row = cur.fetchone()
        if not row or row["in_reply_to_status_id"] is None:
            break
        parent_id = row["in_reply_to_status_id"]
        cur2 = conn.execute(
            "SELECT id, user_id FROM tweets WHERE id = ?", (parent_id,)
        )
        parent = cur2.fetchone()
        if not parent or parent["user_id"] not in allowed_user_ids:
            break
        context.insert(0, parent_id)
        current_id = parent_id
    return context

def get_replies_for(conn, tweet_id, allowed_user_ids):
    replies = []
    queue = [tweet_id]
    while queue:
        current_id = queue.pop(0)
        cur = conn.execute(
            "SELECT id, user_id FROM tweets WHERE in_reply_to_status_id = ?", (current_id,)
        )
        for row in cur.fetchall():
            if row["user_id"] in allowed_user_ids:
                replies.append(row["id"])
                queue.append(row["id"])
    return replies

def mine_conversations(conn, tweets_df, airline_id):
    tweets_df = tweets_df.sort_values("created_at")
    tweet_map = tweets_df.set_index("id").to_dict("index")
    replies_to = {}
    for idx, tweet in tweets_df.iterrows():
        reply_id = tweet.get('in_reply_to_status_id')
        if reply_id:
            replies_to.setdefault(reply_id, []).append(tweet['id'])

    seen_ids = set()
    conversations = []

    for idx, tweet in tqdm(tweets_df.iterrows(), total=len(tweets_df), desc="Mining conversations"):
        screen_name = tweet.get('screen_name')
        user_id = tweet['user_id']
        tweet_id = tweet['id']
        if not screen_name or not user_id:
            continue

        # Only consider airline replies to a user
        if user_id == airline_id and tweet.get('in_reply_to_status_id'):
            parent_id = tweet.get('in_reply_to_status_id')
            parent = tweet_map.get(parent_id)
            if not parent:
                continue
            original_user_id = parent['user_id']
            if not original_user_id:
                continue

            allowed_user_ids = {airline_id, original_user_id}

            # Build conversation: context, parent, tweet, replies
            convo_ids = []
            context_ids = get_context_from(conn, parent_id, allowed_user_ids)
            convo_ids.extend(context_ids)
            convo_ids.append(parent_id)
            convo_ids.append(tweet_id)
            reply_ids = get_replies_for(conn, tweet_id, allowed_user_ids)
            convo_ids.extend(reply_ids)

            # Avoid duplicates and third-party participants
            if not seen_ids.intersection(convo_ids):
                seen_ids.update(convo_ids)
                participants = {tweet_map[tid]['user_id'] for tid in convo_ids}
                if participants <= allowed_user_ids:
                    conversations.append((original_user_id, parent_id, convo_ids))

    return conversations

def collect_reply_thread(tweets_df, root_id):
    tweet_map = tweets_df.set_index("id").to_dict("index")
    children = tweets_df.groupby("in_reply_to_status_id")

    thread = []
    stack = [root_id]

    while stack:
        current_id = stack.pop()
        tweet = tweet_map.get(current_id)
        if not tweet:
            continue

        tweet["id"] = current_id
        thread.append(tweet)

        for _, reply in children.get_group(current_id).iterrows() if current_id in children.groups else []:
            stack.append(reply["id"])

    return sorted(thread, key=lambda t: t["created_at"])

def mine_and_store_conversations(airline_screen_name):
    conn = get_connection()
    airline_id = get_airline_id(conn, airline_screen_name)
    print(f"Airline ID for {airline_screen_name}: {airline_id}")
    tweets_df = get_relevant_tweets(conn, airline_id)
    print(f"Number of relevant tweets: {len(tweets_df)}")

    conversations = mine_conversations(conn, tweets_df, airline_id)
    print(f"Number of conversations mined: {len(conversations)}")

    for user_id, root_id, tweet_ids in conversations:
        conv_id = insert_conversation(conn, user_id, airline_id, root_id)
        insert_conversation_tweets(conn, conv_id, tweet_ids)

    print(f"{len(conversations)} conversations mined and stored for {airline_screen_name}")

if __name__ == "__main__":
    truncate_tables(['conversation_tweet', 'conversation'])
    mine_and_store_conversations("AmericanAir")
