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







import pandas as pd
import numpy as np
import re
import os
from datetime import datetime

# define the input folder and output folder paths
input_folder_path = "/Users/niahayrabedian/Documents/quartile 4/DBL Challenge/data"  # Folder containing JSON files
output_folder_path = "/Users/niahayrabedian/Documents/quartile 4/DBL Challenge/cleaned data"  # Folder to save cleaned files

# ensure the output folder exists
if not os.path.exists(output_folder_path):
    os.makedirs(output_folder_path)

# for loop over all files in the input folder
for filename in os.listdir(input_folder_path):
    if filename.endswith(".json"):  # process only .json files
        file_path = os.path.join(input_folder_path, filename)  # full path of the current file
        
        try:
            # read the JSON file
            df = pd.read_json(file_path, encoding="utf-8", lines=True)

            # convert 'created_at' to datetime
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

            # extract date, hour, and weekday
            df['date'] = df['created_at'].dt.date
            df['hour'] = df['created_at'].dt.hour
            df['weekday'] = df['created_at'].dt.weekday

            # convert 'text' from string to a list (to extract hashtags, keywords)
            def extract_hashtags(text):
                if isinstance(text, str):  # check if text is a string
                    hashtags = re.findall(r'#\w+', text)
                    return hashtags
                return []  # return an empty list if text is not a string

            df['hashtags'] = df['text'].apply(extract_hashtags)

            # remove emojis from 'text'
            def remove_emojis(text):
                if isinstance(text, str):  # check if text is a string
                    emoji_pattern = re.compile("[\U00010000-\U0010ffff]", flags=re.UNICODE)
                    return emoji_pattern.sub(r'', text)
                return text  # return the original value if text is not a string 

            df['text'] = df['text'].apply(remove_emojis)

            # remove rows with missing values in essential columns
            df = df.dropna(subset=['created_at', 'text', 'id'])

            # remove duplicate tweets (based on 'id')
            df = df.drop_duplicates(subset=['id'])

            # remove unnecessary columns based on the data cleaning report
            columns_to_remove = [
                'id_str', 'source', 'in_reply_to_status_id_str', 'in_reply_to_user_id_str', 'in_reply_to_screen_name',
                'place', 'quoted_status_id_str', 'is_quote_status', 'geo', 'extended_entities', 'favorited',
                'retweeted', 'possibly_sensitive', 'truncated', 'matching_rules', 'withheld_copyright', 
                'withheld_in_countries', 'withheld_scope', 'current_user_retweet'
            ]

            df = df.drop(columns=columns_to_remove, errors='ignore')

            # remove tweets with undetectable or null languages
            df = df[df['lang'].notna()]

            # generate a unique output filename based on the original filename
            # use timestamp to avoid overwriting files
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"{os.path.splitext(filename)[0]}-new_{timestamp}.json"  # add the timestamp to avoid overwriting
            output_file = os.path.join(output_folder_path, output_filename)
            
            # save the cleaned data to a new JSON file inside the specified folder
            df.to_json(output_file, orient='records', lines=True)  # save as JSON

            # final cleaned data 
            print(f"Cleaned data saved to: {output_file}")

        except ValueError as e:
            print(f"Error processing file {filename}: {e}")
            # log or handle this error differently (e.g., skip this file)
        except Exception as e:
            print(f"An unexpected error occurred while processing {filename}: {e}")
            # handle other unexpected errors









