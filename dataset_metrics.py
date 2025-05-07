import os
import json
from tqdm import tqdm

# -------------------- Metric Functions --------------------

def count_mentions_in_text(text, airline_name):
    if not isinstance(text, str):
        return 0
    return airline_name.lower() in text.lower()

# -------------------- Metric Calculation --------------------

def calculate_metrics(folder_path, airline_name):
    file_list = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    total_files = len(file_list)

    total_unique_tweets = set()
    total_mentions = 0
    total_accounts = set()
    total_length = 0
    total_tweets = 0
    total_lines = 0
    tweet_attributes = set()
    user_attributes = set()
    total_size_bytes = 0

    for filename in tqdm(file_list, desc="Processing files", unit="file"):
        file_path = os.path.join(folder_path, filename)
        total_size_bytes += os.path.getsize(file_path)

        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                total_lines += 1
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    obj = json.loads(stripped)

                    # Collect tweet attributes
                    tweet_attributes.update(obj.keys())

                    # Unique tweets (use id_str for precision)
                    tweet_id_str = obj.get('id_str')
                    if tweet_id_str:
                        total_unique_tweets.add(tweet_id_str)

                    # Mentions of airline
                    if count_mentions_in_text(obj.get('text'), airline_name):
                        total_mentions += 1

                    # Unique user accounts (use id_str) and collect user attributes
                    user = obj.get('user')
                    if isinstance(user, dict):
                        user_id_str = user.get('id_str')
                        if user_id_str:
                            total_accounts.add(user_id_str)
                        user_attributes.update(user.keys())

                    # Tweet length
                    text = obj.get('text')
                    if isinstance(text, str):
                        total_length += len(text)
                        total_tweets += 1
                except json.JSONDecodeError:
                    continue

    average_length = total_length / total_tweets if total_tweets else 0
    file_size_mb = round(total_size_bytes / (1024 ** 2), 4)

    return {
        "unique_tweets": len(total_unique_tweets),
        "mentions_of_airline": total_mentions,
        "file_size_mb": file_size_mb,
        "unique_accounts": len(total_accounts),
        "avg_tweet_length": round(average_length, 2),
        "total_lines": total_lines,
        "total_files": total_files,
        "criteria_columns_per_tweet": len(tweet_attributes),
        "criteria_columns_per_user": len(user_attributes),
        "tweet_attributes": sorted(tweet_attributes),
        "user_attributes": sorted(user_attributes)
    }

# -------------------- Use --------------------
folder_path = "test_data"  # Set your folder path here
airline_name = "AmericanAir"     # Airline name to detect in text

metrics = calculate_metrics(folder_path, airline_name)
print("\nMetrics Summary:")
for k, v in metrics.items():
    if k in ["tweet_attributes", "user_attributes"]:
        print(f"{k} ({len(v)} total):")
        print(", ".join(v))
    else:
        print(f"{k}: {v}")
