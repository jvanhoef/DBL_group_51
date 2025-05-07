import os
import json
import pyodbc
import sys
from datetime import datetime

# Ensure the terminal uses UTF-8 encoding for output
sys.stdout.reconfigure(encoding='utf-8')

# Define the directory containing the JSON files dynamically
script_directory = os.path.dirname(__file__)
data_directory = os.path.join(script_directory, '..', 'data')

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
    tables = ['conversations', 'hashtag', 'mention', 'options', 'poll', 'tweet', 'user']
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
    Insert tweet data in the database in batches without checking for existing records.
    """
    if not tweets:
        return

    cursor.executemany("""
        INSERT INTO dbo.tweet (
            id, text, created_at, in_reply_to_status_id, in_reply_to_user, in_reply_to_screen_name, 
            user_id, quoted_status_id, retweeted_id, quote_count, reply_count, retweet_count, 
            favorite_count, possibly_sensitive, language
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, tweets)
    
# Get a list of all files in the directory
files = os.listdir(data_directory)

# Filter the list to include only JSON files
json_files = [file for file in files if file.endswith('.json')]

# Check if there are any JSON files
if json_files:
    # Truncate all tables before inserting new data
    truncate_tables(cursor)

    # First iteration: Add all users
    total_files = len(json_files)
    for index, json_file in enumerate(json_files, start=1):
        json_path = os.path.join(data_directory, json_file)
        users = []
        with open(json_path, 'r', encoding='utf8') as file:
            for line in file:
                try:
                    data = json.loads(line)
                    user = data.get('user', {})
                    if user and 'id' in user:
                        users.append((
                            user.get('id'),
                            user.get('name', None),
                            user.get('screen_name', None),
                            user.get('description', None),
                            user.get('verified', False),
                            user.get('followers_count', 0),
                            user.get('friends_count', 0),
                            user.get('listed_count', 0),
                            user.get('statuses_count', 0),
                            user.get('statuses_count', 0)
                        ))
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in file {json_file}: {e}")
        # Insert all users from the current file
        load_users_batch(cursor, users)
        connection.commit()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Users from {json_file} inserted successfully. ({index}/{total_files})")
    print("All users inserted successfully.")

    # Second iteration: Add all tweets
    for index, json_file in enumerate(json_files, start=1):
        json_path = os.path.join(data_directory, json_file)
        tweets = []
        with open(json_path, 'r', encoding='utf8') as file:
            for line in file:
                try:
                    data = json.loads(line)
                    tweet_id = data.get('id')
                    if tweet_id:
                        tweets.append((
                            tweet_id,
                            data.get('text'),
                            datetime.strptime(data.get('created_at'), '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%d %H:%M:%S'),
                            data.get('in_reply_to_status_id'),
                            data.get('in_reply_to_user_id'),
                            data.get('in_reply_to_screen_name'),
                            data.get('user', {}).get('id'),
                            data.get('quoted_status_id'),
                            data.get('retweeted_status', {}).get('id'),
                            data.get('quote_count', 0),
                            data.get('reply_count', 0),
                            data.get('retweet_count', 0),
                            data.get('favorite_count', 0),
                            data.get('possibly_sensitive', False),
                            data.get('lang')
                        ))
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in file {json_file}: {e}")
        # Insert all tweets from the current file
        load_tweets_batch(cursor, tweets)
        connection.commit()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Tweets from {json_file} inserted successfully. ({index}/{total_files})")
    print("All tweets inserted successfully.")

# Close the connection
cursor.close()
connection.close()