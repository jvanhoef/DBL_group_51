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




import json
import pandas as pd
import re
import os
from datetime import datetime

# Define paths
input_folder = r"data"
output_folder = r"clean_data"

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# Columns to remove
columns_to_remove = [
    'id', 'source', 'in_reply_to_status_id', 'in_reply_to_user_id', 'in_reply_to_screen_name',
    'place', 'quoted_status_id_str', 'is_quote_status', 'geo', 'extended_entities', 'favorited',
    'retweeted', 'possibly_sensitive', 'truncated', 'matching_rules', 'withheld_copyright', 
    'withheld_in_countries', 'withheld_scope', 'current_user_retweet', 'display_text_range'
]

user_keys_to_remove = [
    'id', 'url', 'time_zone', 'geo_enabled', 'is_translator', 'profile_background_color', 
    'profile_background_image_url', 'profile_background_image_url_https', 'profile_background_tile', 
    'profile_link_color', 'profile_sidebar_border_color', 'profile_sidebar_fill_color', 'profile_text_color', 
    'profile_use_background_image', 'profile_image_url', 'profile_image_url_https', 'profile_banner_url', 
    'utc_offset'
]

def convert_timestamp(ts):
    try:
        # Check if it's a Unix timestamp (in milliseconds)
        if isinstance(ts, int):
            dt = datetime.fromtimestamp(ts / 1000.0)
            return dt.strftime('%Y-%m-%d %H:%M:%S')  # Return formatted string
        # Check if it's a Twitter datetime string
        elif isinstance(ts, str):
            dt = datetime.strptime(ts, "%a %b %d %H:%M:%S +0000 %Y")
            return dt.strftime('%Y-%m-%d %H:%M:%S')  # Return formatted string
        return None
    except Exception as e:
        print(f"Error parsing timestamp: {e}")
        return None

# Process each file
for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        file_path = os.path.join(input_folder, filename)
        cleaned_tweets = []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f, 1):
                    try:
                        tweet = json.loads(line)

                        # Convert created_at if it's in Unix timestamp or string format
                        if 'created_at' in tweet:
                            tweet['created_at'] = convert_timestamp(tweet['created_at'])

                        # Clean user object
                        if 'user' in tweet and isinstance(tweet['user'], dict):
                            for key in user_keys_to_remove:
                                tweet['user'].pop(key, None)

                        cleaned_tweets.append(tweet)
                    except json.JSONDecodeError:
                        print(f"Skipping malformed line {i} in {filename}")

            df = pd.DataFrame(cleaned_tweets)

            # Ensure created_at is datetime
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            df['date'] = df['created_at'].dt.date
            df['hour'] = df['created_at'].dt.hour
            df['weekday'] = df['created_at'].dt.weekday

            # Drop rows with missing essential values
            df = df.dropna(subset=['created_at', 'text', 'id'])

            # Remove duplicates
            df = df.drop_duplicates(subset=['id'])

            # Drop unnecessary columns
            df = df.drop(columns=columns_to_remove, errors='ignore')

            # Filter out rows with null language
            df = df[df['lang'].notna()]

            # Save cleaned data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"{os.path.splitext(filename)[0]}-cleaned_{timestamp}.json"
            output_file = os.path.join(output_folder, output_filename)

            df.to_json(output_file, orient='records', lines=True)
            print(f"Cleaned data saved to: {output_file}")

        except Exception as e:
            print(f"An error occurred while processing {filename}: {e}")









