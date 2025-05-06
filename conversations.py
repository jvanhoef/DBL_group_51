import json
import os

script_directory = os.path.dirname(__file__)
file_path = os.path.join(script_directory, 'data', 'airlines-1558527599826.json')

airline_ids = {
    56377143, 106062176, 18332190, 22536055, 124476322, 26223583,
    2182373406, 38676903, 1542862735, 253340062, 218730857, 45621423, 20626359
}

tweets = []
with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        try:
            tweet = json.loads(line)
            tweets.append(tweet)
        except json.JSONDecodeError:
            continue

print(f"Loaded {len(tweets)} tweets")

tweets_by_id = {tweet['id']: tweet for tweet in tweets if 'id' in tweet}
replies_to = {}
for tweet in tweets:
    reply_id = tweet.get('in_reply_to_status_id')
    if reply_id:
        replies_to.setdefault(reply_id, []).append(tweet)

def get_context_from(tweet, max_depth=5):
    context = []
    current = tweet
    depth = 0
    while current and current.get('in_reply_to_status_id') and depth < max_depth:
        parent_id = current['in_reply_to_status_id']
        parent = tweets_by_id.get(parent_id)
        if parent:
            context.insert(0, parent)
            current = parent
            depth += 1
        else:
            break
    return context

conversations = []
seen_ids = set()

for tweet in tweets:
    if tweet.get('user', {}).get('id') in airline_ids and tweet.get('in_reply_to_status_id'):
        parent_id = tweet['in_reply_to_status_id']
        parent = tweets_by_id.get(parent_id)

        if not parent:
            continue 

        convo = []
        context = get_context_from(parent)
        convo.extend(context)
        convo.append(parent)
        convo.append(tweet)

        after = replies_to.get(tweet['id'], [])[:3]
        convo.extend(after)

        participants = {t['user']['id'] for t in convo if 'user' in t and 'id' in t['user']}
        if (participants & airline_ids) and (participants - airline_ids):
            convo_ids = [t['id'] for t in convo if 'id' in t]
            if not seen_ids.intersection(convo_ids):
                conversations.append(convo)
                seen_ids.update(convo_ids)

print(f"Found {len(conversations)} conversations.")

if conversations:
    print("Sample Conversation:\n")
    for i, tweet in enumerate(conversations[360]):
        screen_name = tweet['user']['screen_name']
        text = tweet['text']
        print(f"{i+1}. @{screen_name}: {text}")