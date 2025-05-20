import os
import json
import os

script_directory = os.path.dirname(__file__)
file_path = os.path.join(script_directory, 'data', 'airlines-1558527599826.json')

airline_ids = {
    #56377143, # KLM
    #106062176, # AirFrance
    #18332190, # British Airways
    "22536055", # AmericanAir
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

server = 'S20203142'
database = 'airline_tweets'

# Connect to SQL Server using Microsoft Authentication
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

folder_path = r"C:\Users\evanb\Downloads\data"

output_file_path = os.path.join(script_directory, 'conversations.txt')

# Open the output file in write mode
with open(output_file_path, 'w', encoding='utf-8') as output_file:
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

            tweets_by_id = {tweet['id_str']: tweet for tweet in tweets if 'id_str' in tweet}
            replies_to = {}
            for tweet in tweets:
                reply_id = tweet.get('in_reply_to_status_id_str')
                if reply_id:
                    replies_to.setdefault(reply_id, []).append(tweet)

            conversations = []
            seen_ids = set()

            for tweet in tweets:
                if tweet.get('user', {}).get('id_str') in airline_ids and tweet.get('in_reply_to_status_id_str'):
                    parent_id = tweet['in_reply_to_status_id_str']
                    parent = tweets_by_id.get(parent_id)
                    if not parent:
                        continue
                    convo = []
                    current = tweet

                    # find the tweet where the airline is first mentioned
                    while parent and parent.get('user', {}).get('id_str') not in airline_ids:
                        current = parent
                        parent_id = current.get('in_reply_to_status_id_str')
                        parent = tweets_by_id.get(parent_id)

                    # Add the tweet where the airline is mentioned
                    if parent and parent.get('user', {}).get('id_str') in airline_ids:
                        convo.append(parent)

                    # Include all back-and-forth replies between the user and the airline
                    while current:
                        convo.append(current)
                        replies = replies_to.get(current['id_str'], [])
                        current = None
                        for reply in replies:
                            # Ensure parent is not None before accessing its attributes
                            if parent and (reply.get('user', {}).get('id_str') in airline_ids or reply.get('user', {}).get('id_str') == parent.get('user', {}).get('id_str')):
                                current = reply
                                break

                    # Validate the conversation
                    participants = {t['user']['id_str'] for t in convo if 'user' in t and 'id_str' in t['user']}
                    if (participants & airline_ids) and (participants - airline_ids):
                        convo_ids = [t['id_str'] for t in convo if 'id_str' in t]
                        if not seen_ids.intersection(convo_ids):
                            conversations.append(convo)
                            seen_ids.update(convo_ids)

            # Write conversations to the text file
            for convo in conversations:
                output_file.write(f"Filename: {filename}\n")
                output_file.write("Conversation:\n")
                for tweet in convo:
                    output_file.write(f"  - {tweet.get('text', 'No text available')}\n")
                output_file.write("\n")  # Add a blank line between conversations

            print(f"{filename}: {len(conversations)} conversations")
            total_conversations += len(conversations)

print(f"\nTotal conversations found: {total_conversations}")



