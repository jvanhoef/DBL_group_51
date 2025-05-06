import os
import json
import pyodbc
import sys

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

# Function to insert or update user data
def load_users(cursor, data):
    user = data.get('user', {})
    if (user):
        user_id = user.get('id')
        user_name = user.get('name')
        user_screen_name = user.get('screen_name')
        user_description = user.get('description')
        user_verified = user.get('verified', False)
        user_followers_count = user.get('followers_count', 0)
        user_friends_count = user.get('friends_count', 0)
        user_listed_count = user.get('listed_count', 0)
        user_statuses_count = user.get('statuses_count', 0)
    cursor.execute("""
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
    """, (user_id, user_name, user_screen_name, user_description, user_verified, user_followers_count, user_friends_count, user_listed_count, user_statuses_count, user_statuses_count))

def load_tweets(cursor, data):
    tweet_id = data.get('id')
    tweet_text = data.get('text')
    tweet_created_at = data.get('created_at')
    tweet_user_id = data.get('user', {}).get('id')
    tweet_in_reply_to_status_id = data.get('in_reply_to_status_id')
    tweet_in_reply_to_user_id = data.get('in_reply_to_user_id')
    tweet_retweet_count = data.get('retweet_count', 0)
    tweet_favorite_count = data.get('favorite_count', 0)

    cursor.execute("""
        MERGE dbo.tweets AS target
        USING (SELECT ? AS id, ? AS text, ? AS created_at, ? AS user_id, ? AS in_reply_to_status_id, ? AS in_reply_to_user_id, ? AS retweet_count, ? AS favorite_count) AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET 
                text = source.text,
                created_at = source.created_at,
                user_id = source.user_id,
                in_reply_to_status_id = source.in_reply_to_status_id,
                in_reply_to_user_id = source.in_reply_to_user_id,
                retweet_count = source.retweet_count,
                favorite_count = source.favorite_count
        WHEN NOT MATCHED THEN
            INSERT (id, text, created_at, user_id, in_reply_to_status_id, in_reply_to_user_id, retweet_count, favorite_count)
            VALUES (source.id, source.text, source.created_at, source.user_id, source.in_reply_to_status_id, source.in_reply_to_user_id, source.retweet_count, source.favorite_count);
    """, (tweet_id, tweet_text, tweet_created_at, tweet_user_id, tweet_in_reply_to_status_id, tweet_in_reply_to_user_id, tweet_retweet_count, tweet_favorite_count))
    
# Get a list of all files in the directory
files = os.listdir(data_directory)

# Filter the list to include only JSON files
json_files = [file for file in files if file.endswith('.json')]

# Check if there are any JSON files
if json_files:
    # Get the first JSON file
    first_json_file = json_files[0]
    first_json_path = os.path.join(data_directory, first_json_file)
    
    # Load the JSON file into a Python object
    # Load the JSON file into Python objects
    with open(first_json_path, 'r', encoding='utf8') as file:
        data_objects = []
        for line in file:
            try:
                data = json.loads(line)
                
                load_users(cursor, data)
                
                connection.commit()
                print("Data inserted succesfully")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
    
# Close the connection
cursor.close()
connection.close()