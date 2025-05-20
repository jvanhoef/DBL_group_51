import os
import json
from collections import defaultdict
from tqdm import tqdm

# Path to the directory containing your .json files
folder_path = r"clean_data"
output_txt_path = r"conversations_output.txt"

# Airline screen names (must match 'screen_name' of the airline accounts)
airline_users = {
    'AmericanAir': 'AmericanAir',
}

def get_context_from(tweet, tweets_by_id, max_depth=5):
    context = []
    current = tweet
    depth = 0
    while current and current.get('in_reply_to_status_id_str') and depth < max_depth:
        parent_id = current['in_reply_to_status_id_str']
        parent = tweets_by_id.get(parent_id)
        if parent:
            context.insert(0, parent)
            current = parent
            depth += 1
        else:
            break
    return context

def format_conversation(convo):
    """Return formatted conversation text showing reply hierarchy."""
    id_to_tweet = {t['id_str']: t for t in convo if 'id_str' in t}
    id_to_reply = {t['id_str']: t.get('in_reply_to_status_id_str') for t in convo}

    # Build tree structure
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
        text = tweet.get('text', '').replace('\n', ' ')
        timestamp = tweet.get('created_at', 'UnknownTime')
        reply_to = id_to_reply.get(tweet_id)
        prefix = "↳ " * depth
        header = f"{prefix}@{user} (ID: {tweet_id}, Time: {timestamp})"
        if reply_to and reply_to in id_to_tweet:
            header += f" (in reply to @{id_to_tweet[reply_to]['user']['screen_name']})"
        return f"{header}:\n{prefix}{text}\n\n" + ''.join(
            render(child_id, depth + 1) for child_id in children.get(tweet_id, [])
        )

    result = ""
    for root in roots:
        result += render(root)
    return result

conversations_by_airline = defaultdict(int)

# Collect all JSON files
json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]

# Open output file
with open(output_txt_path, "w", encoding="utf-8") as output_file:
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
            if not screen_name:
                continue

            if screen_name in airline_users.values() and tweet.get('in_reply_to_status_id_str'):
                parent_id = tweet['in_reply_to_status_id_str']
                parent = tweets_by_id.get(parent_id)
                if not parent:
                    continue

                convo = []
                context = get_context_from(parent, tweets_by_id)
                convo.extend(context)
                convo.append(parent)
                convo.append(tweet)
                after = replies_to.get(tweet['id_str'], [])[:3]
                convo.extend(after)

                participants = {t['user']['screen_name'] for t in convo if 'user' in t}
                if (participants & set(airline_users.values())) and (participants - set(airline_users.values())):
                    convo_ids = [t['id_str'] for t in convo if 'id_str' in t]
                    if not seen_ids.intersection(convo_ids):
                        seen_ids.update(convo_ids)
                        conversations_by_airline[screen_name] += 1
                        # Write conversation to file
                        output_file.write(f"--- Conversation Start ({screen_name}) ---\n")
                        output_file.write(format_conversation(convo))
                        output_file.write(f"--- Conversation End ---\n\n")

# Summary
print("\nConversations per airline:")
for airline, count in conversations_by_airline.items():
    print(f"{airline}: {count} conversations")
print(f"\nTotal conversations found: {sum(conversations_by_airline.values())}")
print(f"\nAll conversations saved to: {output_txt_path}")
