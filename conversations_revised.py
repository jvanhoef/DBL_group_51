import os
import json
import os

script_directory = os.path.dirname(__file__)
file_path = os.path.join(script_directory, 'data', 'airlines-1558527599826.json')

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

total_conversations = 0

folder_path = r"C:\Users\evanb\Downloads\data"

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
                current = tweet

                # find the tweet where the airline is first mentioned
                while parent and parent.get('user', {}).get('id') not in airline_ids:
                    current = parent
                    parent_id = current.get('in_reply_to_status_id')
                    parent = tweets_by_id.get(parent_id)

                # Add the tweet where the airline is mentioned
                if parent and parent.get('user', {}).get('id') in airline_ids:
                    convo.append(parent)

                # Include all back-and-forth replies between the user and the airline
                while current:
                    convo.append(current)
                    replies = replies_to.get(current['id'], [])
                    current = None
                    for reply in replies:
                        # Ensure parent is not None before accessing its attributes
                        if parent and (reply.get('user', {}).get('id') in airline_ids or reply.get('user', {}).get('id') == parent.get('user', {}).get('id')):
                            current = reply
                            break

                # Validate the conversation
                participants = {t['user']['id'] for t in convo if 'user' in t and 'id' in t['user']}
                if (participants & airline_ids) and (participants - airline_ids):
                    convo_ids = [t['id'] for t in convo if 'id' in t]
                    if not seen_ids.intersection(convo_ids):
                        conversations.append(convo)
                        seen_ids.update(convo_ids)

        print(f"{filename}: {len(conversations)} conversations")
        total_conversations += len(conversations)

print(f"\nTotal conversations found: {total_conversations}")



