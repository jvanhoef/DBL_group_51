import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os
from collections import Counter, defaultdict

# Streamed processing setup
data_folder = r"C:\\Users\\User\\Documents\\DBL\\data"
file_list = [f for f in os.listdir(data_folder) if f.endswith('.json')]

print(f"Streaming from {len(file_list)} files in {data_folder}...")

non_reply_count = 0
total_tweets = 0
language_counter = Counter()
date_counter = defaultdict(int)

for idx, filename in enumerate(file_list, 1):
    print(f"[{idx}/{len(file_list)}] Processing {filename}...")
    file_path = os.path.join(data_folder, filename)
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                tweet = json.loads(line)
                total_tweets += 1
                
                # Count non-reply tweets
                if tweet.get('in_reply_to_status_id') is None:
                    non_reply_count += 1

                # Count languages
                lang = tweet.get('lang')
                if lang:
                    language_counter[lang] += 1

                # Count dates
                created_at = tweet.get('created_at')
                if created_at:
                    dt = pd.to_datetime(created_at, errors='coerce')
                    if pd.notnull(dt):
                        date_counter[dt.date()] += 1

            except (json.JSONDecodeError, TypeError):
                continue  # skip malformed lines

# Print summary stats
print("\n--- Summary ---")
print("Total tweets:", total_tweets)
print("Number of non-reply tweets:", non_reply_count)
print("Top 10 languages:")
for lang, count in language_counter.most_common(10):
    print(f"{lang}: {count}")

# --- Visualizations ---

# a. Language Distribution
lang_df = pd.DataFrame(language_counter.most_common(10), columns=['Language', 'Count'])
plt.figure(figsize=(10, 5))
sns.barplot(x='Language', y='Count', data=lang_df)
plt.title('Top 10 Languages Used in Tweets')
plt.xlabel('Language')
plt.ylabel('Number of Tweets')
plt.tight_layout()
plt.show()

# b. Tweet Volume Over Time
date_df = pd.DataFrame(sorted(date_counter.items()), columns=['Date', 'Tweet Count'])
plt.figure(figsize=(12, 6))
plt.plot(date_df['Date'], date_df['Tweet Count'])
plt.title('Tweet Volume Over Time')
plt.xlabel('Date')
plt.ylabel('Number of Tweets')
plt.tight_layout()
plt.show()
