import db_repository

airline_ids = [
    22536055, # AmericanAir
    # ... add other airline IDs as needed
]

total_conversations = 0
seen_ids = set()

# Fetch all tweets by airlines
airline_tweets = db_repository.fetch_airline_tweets(airline_ids)

for tweet in airline_tweets:
    if tweet.get('in_reply_to_status_id'):
        conversation = []
        current = tweet
        parent_id = tweet['in_reply_to_status_id']

        # Traverse up to find the root of the conversation
        while parent_id:
            parent = db_repository.fetch_tweet_by_id(parent_id)
            if not parent:
                break
            conversation.insert(0, parent)  # Insert at the beginning
            parent_id = parent.get('in_reply_to_status_id')

        # Add the original airline tweet
        conversation.append(tweet)

        # Traverse down to find replies to this tweet
        replies = db_repository.fetch_replies_to_tweet(tweet['id'])
        for reply in replies:
            conversation.append(reply)
            # Optionally, you can recursively fetch replies to replies

        # Validate and store conversation
        convo_ids = [t['id'] for t in conversation if 'id' in t]
        if not seen_ids.intersection(convo_ids):
            seen_ids.update(convo_ids)
            total_conversations += 1
            print(f"Conversation with root tweet {conversation[0]['id']} has {len(conversation)} tweets.")

print(f"\nTotal conversations found: {total_conversations}")