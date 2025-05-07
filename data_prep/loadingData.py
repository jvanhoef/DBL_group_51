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
    """
    Insert or update tweet data in the database.
    """
    tweet_id = data.get('id')
    tweet_text = data.get('text')
    tweet_created_at = datetime.strptime(data.get('created_at'), '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%d %H:%M:%S')
    tweet_in_reply_to_status_id = data.get('in_reply_to_status_id')
    tweet_in_reply_to_user = data.get('in_reply_to_user_id')
    tweet_in_reply_to_screen_name = data.get('in_reply_to_screen_name')
    tweet_user_id = data.get('user', {}).get('id')
    tweet_quoted_status_id = data.get('quoted_status_id')
    tweet_retweeted_id = data.get('retweeted_status', {}).get('id')
    tweet_quote_count = data.get('quote_count', 0)
    tweet_reply_count = data.get('reply_count', 0)
    tweet_retweet_count = data.get('retweet_count', 0)
    tweet_favorite_count = data.get('favorite_count', 0)
    tweet_possibly_sensitive = data.get('possibly_sensitive', False)
    tweet_language = data.get('lang')

    cursor.execute("""
        MERGE dbo.tweet AS target
        USING (SELECT ? AS id, ? AS text, ? AS created_at, ? AS in_reply_to_status_id, ? AS in_reply_to_user, 
                      ? AS in_reply_to_screen_name, ? AS user_id, ? AS quoted_status_id, ? AS retweeted_id, 
                      ? AS quote_count, ? AS reply_count, ? AS retweet_count, ? AS favorite_count, 
                      ? AS possibly_sensitive, ? AS language) AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET 
                text = source.text,
                created_at = source.created_at,
                in_reply_to_status_id = source.in_reply_to_status_id,
                in_reply_to_user = source.in_reply_to_user,
                in_reply_to_screen_name = source.in_reply_to_screen_name,
                user_id = source.user_id,
                quoted_status_id = source.quoted_status_id,
                retweeted_id = source.retweeted_id,
                quote_count = source.quote_count,
                reply_count = source.reply_count,
                retweet_count = source.retweet_count,
                favorite_count = source.favorite_count,
                possibly_sensitive = source.possibly_sensitive,
                language = source.language
        WHEN NOT MATCHED THEN
            INSERT (id, text, created_at, in_reply_to_status_id, in_reply_to_user, in_reply_to_screen_name, 
                    user_id, quoted_status_id, retweeted_id, quote_count, reply_count, retweet_count, 
                    favorite_count, possibly_sensitive, language)
            VALUES (source.id, source.text, source.created_at, source.in_reply_to_status_id, source.in_reply_to_user, 
                    source.in_reply_to_screen_name, source.user_id, source.quoted_status_id, source.retweeted_id, 
                    source.quote_count, source.reply_count, source.retweet_count, source.favorite_count, 
                    source.possibly_sensitive, source.language);
    """, (tweet_id, tweet_text, tweet_created_at, tweet_in_reply_to_status_id, tweet_in_reply_to_user, 
          tweet_in_reply_to_screen_name, tweet_user_id, tweet_quoted_status_id, tweet_retweeted_id, 
          tweet_quote_count, tweet_reply_count, tweet_retweet_count, tweet_favorite_count, 
          tweet_possibly_sensitive, tweet_language))
    
# Get a list of all files in the directory
files = os.listdir(data_directory)

# Filter the list to include only JSON files
json_files = [file for file in files if file.endswith('.json')]

# Check if there are any JSON files
if json_files:
    # Truncate all tables before inserting new data 
    truncate_tables(cursor)
    
    # Get the first JSON file
    first_json_file = json_files[0]
    first_json_path = os.path.join(data_directory, first_json_file)
    
    # Load the JSON file into a Python object
    with open(first_json_path, 'r', encoding='utf8') as file:
        data_objects = []
        for line in file:
            try:
                data = json.loads(line)
                data_objects.append(data)
                
                load_users(cursor, data)
                load_tweets(cursor, data)
                
                connection.commit()
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                
print("Data inserted succesfully")
    
# Close the connection
cursor.close()
connection.close()