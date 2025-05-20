import db_repository

airline_ids = {
    22536055, # AmericanAir
    # ... add other airline IDs as needed
}

total_conversations = 0
seen_ids = set()
conversations = []

# 1. Get all tweets where the user is an airline and it's a reply (potential airline reply)
airline_reply_tweets = db_repository.fetch_airline_reply_tweets(list(airline_ids))
print(airline_reply_tweets)
for tweet in airline_reply_tweets:
    parent_id = tweet.get('in_reply_to_status_id')
    if not parent_id:
        continue
    parent = db_repository.fetch_tweet_by_id(parent_id)
    if not parent:
        continue

    convo = []
    current = tweet

    # 2. Walk up to the root user tweet (where the airline is first mentioned)
    while parent and parent.get('user_id') not in airline_ids:
        current = parent
        parent_id = current.get('in_reply_to_status_id')
        parent = db_repository.fetch_tweet_by_id(parent_id) if parent_id else None

    # 3. Add the tweet where the airline is mentioned (root)
    if parent and parent.get('user_id') in airline_ids:
        convo.append(parent)

    # 4. Collect all back-and-forth replies between the user and the airline
    current = tweet
    while current:
        convo.append(current)
        replies = db_repository.fetch_replies_to_tweet(current['id'])
        next_reply = None
        for reply in replies:
            # Only continue the chain if reply is from the airline or the same user as parent
            if parent and (reply.get('user_id') in airline_ids or reply.get('user_id') == parent.get('user_id')):
                next_reply = reply
                break
        current = next_reply

    # 5. Validate the conversation
    participants = {t['user_id'] for t in convo if t.get('user_id')}
    if (participants & airline_ids) and (participants - airline_ids):
        convo_ids = [t['id'] for t in convo if t.get('id')]
        if not seen_ids.intersection(convo_ids):
            conversations.append(convo)
            seen_ids.update(convo_ids)

print(f"Total conversations found: {len(conversations)}")