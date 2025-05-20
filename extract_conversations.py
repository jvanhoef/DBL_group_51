import os
import json
from collections import defaultdict
from tqdm import tqdm

# Path to JSON files
folder_path = r"C:\Users\evanb\Downloads\data"
output_txt_path = r"conversations_output_filtered.txt"

# Airline screen names (can expand as needed)
airline_users = {
    'AmericanAir': 'AmericanAir',
}
airline_usernames_set = set(airline_users.values())

def get_full_text(tweet):
    """Extract full text from tweet object."""
    return (
        tweet.get('extended_tweet', {}).get('full_text') or
        tweet.get('full_text') or
        tweet.get('text', '')
    ).replace('\n', ' ').strip()

def get_context_from(tweet, tweets_by_id, allowed_user_ids):
    """Get context (ancestors) of a tweet limited to allowed users."""
    context = []
    parent_id = tweet.get('in_reply_to_status_id_str')
    while parent_id and parent_id in tweets_by_id:
        parent = tweets_by_id[parent_id]
        if parent.get('user', {}).get('id_str') not in allowed_user_ids:
            break
        context.insert(0, parent)
        parent_id = parent.get('in_reply_to_status_id_str')
    return context

def get_replies_for(tweet, replies_to, allowed_user_ids):
    """Recursively get all replies from allowed users."""
    replies = []
    queue = replies_to.get(tweet['id_str'], [])
    while queue:
        reply = queue.pop(0)
        user_id = reply.get('user', {}).get('id_str')
        if user_id in allowed_user_ids:
            replies.append(reply)
            queue.extend(replies_to.get(reply['id_str'], []))
    return replies

def format_conversation(convo):
    """Format the conversation with proper time, user, and text output."""
    id_to_tweet = {t['id_str']: t for t in convo}
    id_to_reply = {t['id_str']: t.get('in_reply_to_status_id_str') for t in convo}

    roots = []
    children = defaultdict(list)
    for tweet_id, parent_id in id_to_reply.items():
        if parent_id and parent_id in id_to_tweet:
            children[parent_id].append(tweet_id)
        else:
            roots.append(tweet_id)

    def render(tweet_id, depth=0):
        tweet = id_to_tweet[tweet_id]
        user = tweet.get('user', {}).get('screen_name', 'UnknownUser')
        text = get_full_text(tweet)
        timestamp = tweet.get('timestamp_ms') or tweet.get('created_at', 'UnknownTime')
        return f"(Time: {timestamp}) @{user}: {text}\n" + ''.join(
            render(child_id, depth + 1) for child_id in children.get(tweet_id, [])
        )

    return ''.join(render(root) for root in roots)

# Process all JSON files
json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]

with open(output_txt_path, "w", encoding="utf-8") as output_file:
    conversations_by_airline = defaultdict(int)

    for filename in tqdm(json_files, desc="Processing files"):
        file_path = os.path.join(folder_path, filename)

        tweets = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                    if isinstance(tweet, dict):
                        tweets.append(tweet)
                except json.JSONDecodeError:
                    continue

        tweets_by_id = {tweet['id_str']: tweet for tweet in tweets if 'id_str' in tweet}
        replies_to = defaultdict(list)
        for tweet in tweets:
            reply_id = tweet.get('in_reply_to_status_id_str')
            if reply_id:
                replies_to[reply_id].append(tweet)

        seen_ids = set()

        for tweet in tweets:
            screen_name = tweet.get('user', {}).get('screen_name')
            user_id = tweet.get('user', {}).get('id_str')
            if not screen_name or not user_id:
                continue

            if screen_name in airline_usernames_set and tweet.get('in_reply_to_status_id_str'):
                parent_id = tweet.get('in_reply_to_status_id_str')
                parent = tweets_by_id.get(parent_id)
                if not parent:
                    continue

                original_user_id = parent.get('user', {}).get('id_str')
                if not original_user_id:
                    continue

                allowed_user_ids = {user_id, original_user_id}

                convo = []
                context = get_context_from(parent, tweets_by_id, allowed_user_ids)
                convo.extend(context)
                convo.append(parent)
                convo.append(tweet)
                replies = get_replies_for(tweet, replies_to, allowed_user_ids)
                convo.extend(replies)

                convo_ids = [t['id_str'] for t in convo]
                if not seen_ids.intersection(convo_ids):
                    seen_ids.update(convo_ids)
                    participants = {t['user']['id_str'] for t in convo}
                    if participants <= allowed_user_ids:
                        conversations_by_airline[screen_name] += 1
                        output_file.write(f"--- Conversation Start ({screen_name}) ---\n")
                        output_file.write(format_conversation(convo))
                        output_file.write(f"--- Conversation End ---\n\n")

# Summary
print("\nConversations per airline:")
for airline, count in conversations_by_airline.items():
    print(f"{airline}: {count} conversations")
print(f"\nTotal conversations found: {sum(conversations_by_airline.values())}")
print(f"\nAll conversations saved to: {output_txt_path}")