import os
import json

# Path to the directory containing your .json files
folder_path = r"C:\Users\User\Documents\DBL\data"

airline_ids = {
    #56377143, # KLM
    #106062176, # AirFrance
    #18332190, # British Airways
    22536055, # AmericanAir
    #124476322, # Lufthansa
    #26223583, # AirBerlin
    #2182373406, # AirBerlin assist
    #38676903, # easyJet
    #1542862735, # RyanAir
    #253340062, # SingaporeAir
    #218730857, # Qantas
    #45621423, # EtihadAirways
    #20626359 #  VirginAtlantic
}

def get_context_from(tweet, tweets_by_id, max_depth=5):
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

total_conversations = 0

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".json"):
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

        tweets_by_id = {tweet['id']: tweet for tweet in tweets if 'id' in tweet}
        replies_to = {}
        for tweet in tweets:
            reply_id = tweet.get('in_reply_to_status_id')
            if reply_id:
                replies_to.setdefault(reply_id, []).append(tweet)

        conversations = []
        seen_ids = set()

        for tweet in tweets:
            if tweet.get('user', {}).get('id') in airline_ids and tweet.get('in_reply_to_status_id'):
                parent_id = tweet['in_reply_to_status_id']
                parent = tweets_by_id.get(parent_id)
                if not parent:
                    continue
                convo = []
                context = get_context_from(parent, tweets_by_id)
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

        print(f"{filename}: {len(conversations)} conversations")
        total_conversations += len(conversations)

print(f"\nTotal conversations found: {total_conversations}")
