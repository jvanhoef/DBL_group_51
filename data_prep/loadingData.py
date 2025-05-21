import os
import json
import pyodbc
import sys
from datetime import datetime, timezone

# Ensure the terminal uses UTF-8 encoding for output
sys.stdout.reconfigure(encoding='utf-8')

# Define the directory containing the JSON files dynamically
script_directory = os.path.dirname(__file__)
data_directory = os.path.join(script_directory, '..', 'clean_data')

server = 'S20203142'
database = 'airline_tweets'

# Connect to SQL Server using Microsoft Authentication
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

#Function to truncate(empty tables
def truncate_tables(cursor):
    """
    Delete all rows from the specified tables while respecting foreign key constraints.
    """
    tables = ['mention', 'hashtag']
    for table in tables:
        try:
            cursor.execute(f"DELETE FROM dbo.[{table}]")
            print(f"Table '{table}' cleared successfully.")
        except pyodbc.Error as e:
            print(f"Error clearing table '{table}': {e}")

# Function to insert or update user data
def load_users_batch(cursor, users):
    """
    Insert or update user data in the database in batches.
    """
    if not users:
        return

    cursor.executemany("""
        MERGE dbo.[user] AS target
        USING (SELECT ? AS id, ? AS name, ? AS screen_name, ? AS description, ? AS verified, ? AS followers_count, ? AS friends_count, ? AS listed_count, ? AS favorites_count, ? AS status_count) AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET 
                name = source.name,
                screen_name = source.screen_name,
                description = source.description,
                verified = source.verified,
                followers_count = source.followers_count,
                friends_count = source.friends_count,
                listed_count = source.listed_count,
                favorites_count = source.favorites_count,
                status_count = source.status_count
        WHEN NOT MATCHED THEN
            INSERT (id, name, screen_name, description, verified, followers_count, friends_count, listed_count, favorites_count, status_count)
            VALUES (source.id, source.name, source.screen_name, source.description, source.verified, source.followers_count, source.friends_count, source.listed_count, source.favorites_count, source.status_count);
    """, users)
    
def load_tweets_batch(cursor, tweets):
    """
    Insert or update tweet data in the database in batches.
    """
    if not tweets:
        return

    cursor.executemany("""
        MERGE dbo.tweet AS target
        USING (SELECT ? AS id, ? AS text, ? AS created_at, ? AS in_reply_to_status_id, ? AS in_reply_to_user, 
                      ? AS user_id, ? AS quoted_status_id, ? AS retweeted_id, 
                      ? AS quote_count, ? AS reply_count, ? AS retweet_count, ? AS favorite_count, 
                      ? AS possibly_sensitive, ? AS language, ? AS sentiment) AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET 
                text = source.text,
                created_at = source.created_at,
                in_reply_to_status_id = source.in_reply_to_status_id,
                in_reply_to_user = source.in_reply_to_user,
                user_id = source.user_id,
                quoted_status_id = source.quoted_status_id,
                retweeted_id = source.retweeted_id,
                quote_count = source.quote_count,
                reply_count = source.reply_count,
                retweet_count = source.retweet_count,
                favorite_count = source.favorite_count,
                possibly_sensitive = source.possibly_sensitive,
                language = source.language,
                sentiment = source.sentiment
        WHEN NOT MATCHED THEN
            INSERT (id, text, created_at, in_reply_to_status_id, in_reply_to_user, 
                    user_id, quoted_status_id, retweeted_id, quote_count, reply_count, retweet_count, 
                    favorite_count, possibly_sensitive, language, sentiment)
            VALUES (source.id, source.text, source.created_at, source.in_reply_to_status_id, source.in_reply_to_user, 
                    source.user_id, source.quoted_status_id, source.retweeted_id, source.quote_count, source.reply_count, 
                    source.retweet_count, source.favorite_count, source.possibly_sensitive, source.language, source.sentiment);
    """, tweets)
    
def load_hashtags_batch(cursor, hashtags):
    """
    Insert hashtag data in the database in batches without checking for duplicates.
    """
    if not hashtags:
        return

    cursor.executemany("""
        INSERT INTO dbo.hashtag (text, indices, tweet_id)
        VALUES (?, ?, ?);
    """, hashtags)
    
def load_mentions_batch(cursor, mentions):
    """
    Insert mention data in the database in batches without checking for duplicates.
    """
    if not mentions:
        return

    cursor.executemany("""
        INSERT INTO dbo.mention (indices, tweet_id, name)
        VALUES (?, ?, ?);
    """, mentions)
    
def load_polls_batch(cursor, polls):
    """
    Insert poll data in the database in batches without checking for duplicates.
    """
    if not polls:
        return

    cursor.executemany("""
        INSERT INTO dbo.poll (end_datetime, duration_minutes, id, tweet_id)
        VALUES (?, ?, ?, ?);
    """, polls)

def load_poll_options_batch(cursor, options):
    """
    Insert poll option data in the database in batches without checking for duplicates.
    """
    if not options:
        return

    cursor.executemany("""
        INSERT INTO dbo.options (poll_id, position, text)
        VALUES (?, ?, ?);
    """, options)
    
def update_tweet_text_batch(cursor, tweets):
    """
    Update tweet data in the database in batches.
    """
    if not tweets:
        return

    cursor.executemany("""
        UPDATE dbo.tweet
        SET text = ?
        WHERE id = ?;
    """, tweets)
    
# Get a list of all files in the directory
files = os.listdir(data_directory)

# Filter the list to include only JSON files
json_files = [file for file in files if file.endswith('.json')]

# Check if there are any JSON files
if json_files:
    # Truncate all tables before inserting new data
    # truncate_tables(cursor)

    # First iteration: Add all users
    total_files = len(json_files)
    
    processed_files_path = os.path.join(data_directory, 'processed_files.log')

processed_lines_path = os.path.join(data_directory, 'processed_lines.log')

# Load the last processed line for each file
if os.path.exists(processed_lines_path):
    with open(processed_lines_path, 'r') as log_file:
        processed_lines = {line.split(':')[0]: int(line.split(':')[1]) for line in log_file.read().splitlines()}
else:
    processed_lines = {}

# Process files
for index, json_file in enumerate(json_files, start=1):
    json_path = os.path.join(data_directory, json_file)
    last_processed_line = processed_lines.get(json_file, 0)
    users = []
    with open(json_path, 'r', encoding='utf8') as file:
        for line_number, line in enumerate(file, start=1):
            if line_number <= last_processed_line:
                continue  # Skip already processed lines

            try:
                data = json.loads(line)
                user = data.get('user', {})
                if user and 'id_str' in user:
                    try:
                        user_id = int(user['id_str'])  # Convert id_str to BIGINT
                    except ValueError as e:
                        print(f"Error converting id_str to BIGINT in file {json_file}: {e}")
                        continue  # Skip this user if conversion fails

                    users.append((
                        user_id,
                        user.get('name', " ") or " ",
                        user.get('screen_name', None),
                        user.get('description', None),
                        user.get('verified', False) or False,
                        user.get('followers_count', 0) or -1,
                        user.get('friends_count', 0) or -1,
                        user.get('listed_count', 0) or -1,
                        user.get('statuses_count', 0) or -1,
                        user.get('statuses_count', 0) or -1
                    ))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in file {json_file}: {e}")
    # Insert all users from the current file
    load_users_batch(cursor, users)
    connection.commit()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Users from {json_file} inserted successfully. ({index}/{total_files})")

    # Log the last processed line
    with open(processed_lines_path, 'w') as log_file:
        processed_lines[json_file] = line_number
        for file_name, last_line in processed_lines.items():
            log_file.write(f"{file_name}:{last_line}\n")

print("All users inserted successfully.")
# Second iteration: Add all tweets
for index, json_file in enumerate(json_files, start=1):
    json_path = os.path.join(data_directory, json_file)
    last_processed_line = processed_lines.get(json_file, 0)
    tweets = []
    with open(json_path, 'r', encoding='utf8') as file:
        for line_number, line in enumerate(file, start=1):
            if line_number <= last_processed_line:
                continue  # Skip already processed lines
            try:
                data = json.loads(line)
                tweet_id_str = data.get('id_str')
                if tweet_id_str:
                    try:
                        tweet_id = int(tweet_id_str)  # Convert id_str to BIGINT
                    except ValueError as e:
                        print(f"Error converting id_str to BIGINT in file {json_file}: {e}")
                        continue  # Skip this tweet if conversion fails
                    
                  # Convert all relevant fields to BIGINT or INT as needed
                def safe_int(val):
                    try:
                        return int(val)
                    except (TypeError, ValueError):
                        return None
                
                # Safely get full_text if available, otherwise fallback
                extended_tweet = data.get('extended_tweet')
                if extended_tweet and isinstance(extended_tweet, dict):
                    tweet_text = extended_tweet.get('full_text')
                else:
                    tweet_text = None

                if not tweet_text:
                    tweet_text = data.get('full_text') or data.get('text', '')

                tweet_text = tweet_text.replace('\n', ' ').strip()
                    
                in_reply_to_status_id = safe_int(data.get('in_reply_to_status_id_str'))
                in_reply_to_user = safe_int(data.get('in_reply_to_user_id_str'))
                user = data.get('user', {})
                user_id = safe_int(user.get('id_str'))
                retweeted_status = data.get('retweeted_status', {})
                retweeted_id = safe_int(retweeted_status.get('id_str')) if retweeted_status else None
                    
                created_at_int = data.get('created_at')
                if isinstance(created_at_int, int):  # Ensure it's a valid integer
                    created_at = datetime.fromtimestamp(created_at_int / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    created_at = None  # Handle missing or invalid created_at

                try:
                    user = data.get('user', {})
                    user_id = user['id_str']  # Convert id_str to BIGINT
                except ValueError as e:
                    print(f"Error converting id_str to BIGINT in file {json_file}: {e}")
                    continue  # Skip this user if conversion fails

                # Safely handle retweeted_status
                retweeted_status = data.get('retweeted_status', {})
                retweeted_id = retweeted_status.get('id') if retweeted_status else None

                tweets.append((
                    tweet_id,
                    extended_tweet,
                    created_at,
                    in_reply_to_status_id,
                    in_reply_to_user,
                    user_id,
                    data.get('quoted_status_id'),
                    retweeted_id,
                    data.get('quote_count', 0),
                    data.get('reply_count', 0),
                    data.get('retweet_count', 0),
                    data.get('favorite_count', 0),
                    data.get('possibly_sensitive', False),
                    data.get('lang'),
                    0
                ))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in file {json_file}: {e}")
        # Insert all tweets from the current file
        load_tweets_batch(cursor, tweets)
        connection.commit()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Tweets from {json_file} inserted successfully. ({index}/{total_files})")
        # Log the last processed line for this file
        processed_lines[json_file] = line_number
        with open(processed_lines_path, 'w') as log_file:
            for file_name, last_line in processed_lines.items():
                log_file.write(f"{file_name}:{last_line}\n")
print("All tweets inserted successfully.")

for index, json_file in enumerate(json_files, start=1):
    json_path = os.path.join(data_directory, json_file)
    hashtags = []
    mentions = []
    polls = []
    poll_options = []
    with open(json_path, 'r', encoding='utf8') as file:
        for line in file:
            try:
                data = json.loads(line)
                tweet_id_str = data.get('id_str')
                tweet_id = None
                if tweet_id_str:
                    try:
                        tweet_id = int(tweet_id_str)
                    except ValueError as e:
                        print(f"Error converting id_str to BIGINT in file {json_file}: {e}")
                        continue  # Skip this tweet if conversion fails

                if tweet_id is not None:
                    hashtags_data = data.get('entities', {}).get('hashtags', [])
                    for hashtag in hashtags_data:
                        hashtags.append((
                            hashtag.get('text', None),
                            str(hashtag.get('indices', None)),
                            tweet_id
                        ))

                    mentions_data = data.get('entities', {}).get('user_mentions', [])
                    for mention in mentions_data:
                        mentions.append((
                            str(mention.get('indices', None)),
                            tweet_id,
                            mention.get('screen_name', None)
                        ))
                        
                poll_data = data.get('entities', {}).get('polls', [])
                if poll_data and tweet_id is not None:
                    for poll in poll_data:
                        print(poll)
                        poll_id = poll.get('id')
                        end_datetime = poll.get('end_datetime')
                        duration_minutes = poll.get('duration_minutes')
                        # Store poll
                        polls.append((
                            end_datetime,
                            duration_minutes,
                            poll_id,
                            tweet_id
                        ))
                        # Store poll options
                        for option in poll.get('options', []):
                            poll_options.append((
                                poll_id,
                                option.get('position'),
                                option.get('text')
                            ))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in file {json_file}: {e}")
    # Insert all tweets from the current file
    load_hashtags_batch(cursor, hashtags)
    load_mentions_batch(cursor, mentions)
    load_polls_batch(cursor, polls)
    load_poll_options_batch(cursor, poll_options)
    connection.commit()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] entities from {json_file} inserted successfully. ({index}/{total_files})")
print("All tweet entities inserted successfully.")
                    
# Close the connection
cursor.close()
connection.close()