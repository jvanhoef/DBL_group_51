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









