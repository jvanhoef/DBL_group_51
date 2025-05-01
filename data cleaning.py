import os
import json
import pandas as pd
from glob import glob

def clean_text(text):
    # Fix encoding and remove problematic characters
    try:
        return text.encode('latin1').decode('utf-8', errors='ignore')
    except Exception:
        return text

def clean_tweet(raw):
    try:
        tweet = {}

        # Basic info
        tweet['created_at'] = pd.to_datetime(raw.get('created_at'))
        tweet['id'] = raw.get('id_str')
        tweet['lang'] = raw.get('lang')

        # Get full text if available, otherwise regular text
        full_text = raw.get('extended_tweet', {}).get('full_text', raw.get('text'))
        tweet['text'] = clean_text(full_text).strip()

        # Tweet structure info
        tweet['is_retweet'] = 'retweeted_status' in raw
        tweet['is_reply'] = raw.get('in_reply_to_status_id') is not None

        # User info
        user = raw.get('user', {})
        tweet['user_id'] = user.get('id_str')
        tweet['user_screen_name'] = user.get('screen_name')
        tweet['user_verified'] = user.get('verified', False)
        tweet['user_followers'] = user.get('followers_count', 0)

        # Mentions
        mentions = raw.get('entities', {}).get('user_mentions', [])
        tweet['mentioned_airlines'] = [m['screen_name'] for m in mentions]
        tweet['mention_count'] = len(tweet['mentioned_airlines'])

        return tweet
    except Exception as e:
        print(f"Skipping tweet due to error: {e}")
        return None

def load_and_clean_all(folder_path):
    all_tweets = []
    print(f"📂 Scanning folder: {folder_path}")

    for file_path in glob(os.path.join(folder_path, '*.json')):
        print(f"📄 Processing file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    raw = json.loads(line)
                    cleaned = clean_tweet(raw)
                    if cleaned:
                        all_tweets.append(cleaned)
                except json.JSONDecodeError:
                    continue

    df = pd.DataFrame(all_tweets)

    # Optional: Convert timezone-aware to naive (remove +00:00)
    df['created_at'] = pd.to_datetime(df['created_at']).dt.tz_localize(None)

    return df

# 🛠 Usage
folder = "c:/Users/User/Documents/DBL/data"  # Update if needed
tweets_df = load_and_clean_all(folder)

# 🧪 Show and save
print(f"\n✅ Loaded {len(tweets_df)} tweets.")
print(tweets_df.head())
tweets_df.to_csv("cleaned_tweets_final.csv", index=False)
