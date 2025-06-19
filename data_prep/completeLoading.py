import os
import json
import pyodbc
import sys
from datetime import datetime, timezone
from tqdm import tqdm
from collections import defaultdict

# Ensure the terminal uses UTF-8 encoding for output
sys.stdout.reconfigure(encoding='utf-8')

# Define the directory containing the JSON files dynamically
script_directory = os.path.dirname(__file__)
data_directory = os.path.join(script_directory, '..', 'data')
log_directory = os.path.join(script_directory, '..', 'logs')
os.makedirs(log_directory, exist_ok=True)

server = 'S20203142'
database = 'airline_tweets'
BATCH_SIZE = 1000  # Process tweets in batches for memory efficiency

# Connect to SQL Server using Microsoft Authentication
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

# Progress tracking log paths
progress_log_path = os.path.join(log_directory, 'loading_progress.log')
summary_log_path = os.path.join(log_directory, 'loading_summary.log')

# Initialize progress tracking dictionary with three stages per file:
# stage 1: users, stage 2: tweets, stage 3: entities
progress = defaultdict(lambda: {1: 0, 2: 0, 3: 0})

# Load the existing progress if the log exists
if os.path.exists(progress_log_path):
    with open(progress_log_path, 'r') as log_file:
        for line in log_file:
            parts = line.strip().split(':')
            if len(parts) == 3:
                filename, stage, line_number = parts
                progress[filename][int(stage)] = int(line_number)
else:
    # Create the log file
    with open(progress_log_path, 'w') as log_file:
        pass

# Helper function to update progress log
def update_progress(filename, stage, line_number):
    """Update the progress log for a specific file and stage."""
    progress[filename][stage] = line_number
    
    # Write the full progress log
    with open(progress_log_path, 'w') as log_file:
        for fname, stages in progress.items():
            for stage_num, line_num in stages.items():
                log_file.write(f"{fname}:{stage_num}:{line_num}\n")

# Helper function to append to summary log
def log_summary(message):
    """Append a message to the summary log with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(summary_log_path, 'a') as log_file:
        log_file.write(f"[{timestamp}] {message}\n")

# Helper functions
def convert_timestamp(ts):
    """Convert timestamp to standard format."""
    try:
        # Check if it's a Unix timestamp (in milliseconds)
        if isinstance(ts, int):
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        # Check if it's a Twitter datetime string
        elif isinstance(ts, str):
            dt = datetime.strptime(ts, "%a %b %d %H:%M:%S +0000 %Y")
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        return None
    except Exception as e:
        print(f"Error parsing timestamp: {e}")
        return None

def safe_int(val):
    """Safely convert value to integer."""
    try:
        if val is not None:
            return int(val)
        return None
    except (TypeError, ValueError):
        return None

def clean_text(text):
    """Clean text by removing newlines and trimming."""
    if text:
        return text.replace('\n', ' ').strip()
    return None

def clean_user_object(user):
    """Clean and validate user object."""
    if not user or not isinstance(user, dict):
        return None
    
    # Clean user data
    user_id = safe_int(user.get('id_str'))
    if not user_id:
        return None
        
    return {
        'id': user_id,
        'name': (user.get('name') or "").strip() or " ",  # Ensure name is never empty
        'screen_name': user.get('screen_name'),
        'description': user.get('description'),
        'verified': user.get('verified', False) or False,
        'followers_count': user.get('followers_count', 0) or 0,
        'friends_count': user.get('friends_count', 0) or 0,
        'listed_count': user.get('listed_count', 0) or 0,
        'favorites_count': user.get('favorites_count', 0) or 0,
        'status_count': user.get('statuses_count', 0) or 0
    }

def clean_tweet_object(data):
    """Clean and validate tweet object."""
    if not data:
        return None
    
    tweet_id = safe_int(data.get('id_str'))
    if not tweet_id:
        return None
    
    # Get the full text from the appropriate field
    extended_tweet = data.get('extended_tweet', {})
    tweet_text = None
    if isinstance(extended_tweet, dict):
        tweet_text = extended_tweet.get('full_text')
    if not tweet_text:
        tweet_text = data.get('full_text') or data.get('text', '')
    
    tweet_text = clean_text(tweet_text)
    
    # Get created_at timestamp
    created_at = convert_timestamp(data.get('created_at'))
    if not created_at:
        return None
        
    # Get user ID
    user = data.get('user', {})
    user_id = safe_int(user.get('id_str'))
    if not user_id:
        return None
        
    # Handle reply information
    in_reply_to_status_id = safe_int(data.get('in_reply_to_status_id_str'))
    in_reply_to_user = safe_int(data.get('in_reply_to_user_id_str'))
    
    # Handle retweet and quote information
    retweeted_status = data.get('retweeted_status', {})
    retweeted_id = safe_int(retweeted_status.get('id_str')) if retweeted_status else None
    quoted_status_id = safe_int(data.get('quoted_status_id_str'))
    
    return {
        'id': tweet_id,
        'text': tweet_text,
        'created_at': created_at,
        'in_reply_to_status_id': in_reply_to_status_id,
        'in_reply_to_user': in_reply_to_user,
        'user_id': user_id,
        'quoted_status_id': quoted_status_id,
        'retweeted_id': retweeted_id,
        'quote_count': data.get('quote_count', 0) or 0,
        'reply_count': data.get('reply_count', 0) or 0,
        'retweet_count': data.get('retweet_count', 0) or 0,
        'favorite_count': data.get('favorite_count', 0) or 0,
        'possibly_sensitive': data.get('possibly_sensitive', False) or False,
        'language': data.get('lang'),
        'sentiment': 0  # Default sentiment value
    }

def extract_entities(data, tweet_id):
    """Extract hashtags and mentions from tweet."""
    if not data or not tweet_id:
        return [], []
    
    hashtags = []
    mentions = []
    
    entities = data.get('entities', {})
    
    # Extract hashtags
    for hashtag in entities.get('hashtags', []):
        if hashtag.get('text'):
            hashtags.append((
                hashtag.get('text'),
                str(hashtag.get('indices', [])),
                tweet_id
            ))
    
    # Extract mentions
    for mention in entities.get('user_mentions', []):
        if mention.get('screen_name'):
            mentions.append((
                str(mention.get('indices', [])),
                tweet_id,
                mention.get('screen_name')
            ))
            
    return hashtags, mentions

# Database functions
def setup_database_tables():
    """Create all necessary database tables if they don't exist."""
    try:
        # Create user table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'user')
        CREATE TABLE [user] (
            id BIGINT PRIMARY KEY,
            name NVARCHAR(100) NOT NULL,
            screen_name NVARCHAR(50),
            description NVARCHAR(MAX),
            verified BIT DEFAULT 0,
            followers_count INT DEFAULT 0,
            friends_count INT DEFAULT 0,
            listed_count INT DEFAULT 0,
            favorites_count INT DEFAULT 0,
            status_count INT DEFAULT 0
        )
        """)
        
        # Create tweet table with foreign keys
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'tweet')
        CREATE TABLE tweet (
            id BIGINT PRIMARY KEY,
            text NVARCHAR(MAX),
            created_at DATETIME,
            in_reply_to_status_id BIGINT,
            in_reply_to_user BIGINT,
            user_id BIGINT NOT NULL,
            quoted_status_id BIGINT,
            retweeted_id BIGINT,
            quote_count INT DEFAULT 0,
            reply_count INT DEFAULT 0,
            retweet_count INT DEFAULT 0,
            favorite_count INT DEFAULT 0,
            possibly_sensitive BIT DEFAULT 0,
            language NVARCHAR(10),
            sentiment FLOAT DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES [user](id),
        )
        """)
        
        # Create hashtag table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'hashtag')
        CREATE TABLE hashtag (
            id INT IDENTITY(1,1) PRIMARY KEY,
            text NVARCHAR(280) NOT NULL,
            indices NVARCHAR(50),
            tweet_id BIGINT NOT NULL,
            FOREIGN KEY (tweet_id) REFERENCES tweet(id)
        )
        """)
        
        # Create mention table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'mention')
        CREATE TABLE mention (
            id INT IDENTITY(1,1) PRIMARY KEY,
            indices NVARCHAR(50),
            tweet_id BIGINT NOT NULL,
            name NVARCHAR(100),
            FOREIGN KEY (tweet_id) REFERENCES tweet(id)
        )
        """)
        
        connection.commit()
        print("✓ Database tables created successfully")
        log_summary("Database tables created successfully")
        return True
    except Exception as e:
        print(f"! Error creating database tables: {e}")
        log_summary(f"Error creating database tables: {e}")
        return False
    
    # Replace your current index creation code with this:
def create_indexes():
    """Create indexes if they don't exist using SQL Server compatible syntax."""
    try:
        # Check and create user_id index
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_tweet_user_id' AND object_id = OBJECT_ID('dbo.tweet'))
        BEGIN
            CREATE INDEX idx_tweet_user_id ON tweet(user_id)
        END
        """)
        
        # Check and create reply index
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_tweet_reply' AND object_id = OBJECT_ID('dbo.tweet'))
        BEGIN
            CREATE INDEX idx_tweet_reply ON tweet(in_reply_to_status_id)
        END
        """)
        
        # Check and create created_at index
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_tweet_created_at' AND object_id = OBJECT_ID('dbo.tweet'))
        BEGIN
            CREATE INDEX idx_tweet_created_at ON tweet(created_at)
        END
        """)
        
        # Check and create screen_name index
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_user_screen_name' AND object_id = OBJECT_ID('dbo.[user]'))
        BEGIN
            CREATE INDEX idx_user_screen_name ON [user](screen_name)
        END
        """)
        
        connection.commit()
        print("✓ Database indexes created or already exist")
        log_summary("Database indexes created or already exist")
        return True
    except Exception as e:
        print(f"! Could not create indexes - performance might be affected: {e}")
        log_summary(f"Could not create indexes - performance might be affected: {e}")
        return False

def load_users_batch(cursor, users):
    """Insert or update user data in the database in batches."""
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
    """Insert or update tweet data in the database in batches."""
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
    """Insert hashtag data in the database in batches without checking for duplicates."""
    if not hashtags:
        return

    cursor.executemany("""
        INSERT INTO dbo.hashtag (text, indices, tweet_id)
        VALUES (?, ?, ?);
    """, hashtags)
    
def load_mentions_batch(cursor, mentions):
    """Insert mention data in the database in batches without checking for duplicates."""
    if not mentions:
        return

    cursor.executemany("""
        INSERT INTO dbo.mention (indices, tweet_id, name)
        VALUES (?, ?, ?);
    """, mentions)

# Process a single file for a specific stage
def process_file(json_file, stage):
    """Process a single JSON file for a specific stage (1=users, 2=tweets, 3=entities)."""
    json_path = os.path.join(data_directory, json_file)
    last_processed_line = progress[json_file][stage]
    
    users_batch = []
    tweets_batch = []
    all_hashtags = []
    all_mentions = []
    
    line_number = 0
    valid_lines = 0
    invalid_lines = 0
    
    stage_name = {1: "users", 2: "tweets", 3: "entities"}[stage]
    
    with open(json_path, 'r', encoding='utf8') as file:
        # Count total lines for progress bar
        if last_processed_line == 0:  # Only count if starting from beginning
            total_lines = sum(1 for _ in open(json_path, 'r', encoding='utf8'))
        else:
            # Estimate remaining lines
            total_lines = sum(1 for _ in open(json_path, 'r', encoding='utf8')) - last_processed_line
        
        # Create progress bar
        pbar = tqdm(total=total_lines, 
                   desc=f"[{stage_name.upper()}] {json_file}", 
                   unit="lines",
                   position=0)
        
        # Skip to last processed line
        if last_processed_line > 0:
            for _ in range(last_processed_line):
                next(file, None)
                
        for line_number, line in enumerate(file, start=last_processed_line+1):
            pbar.update(1)
            
            try:
                # Parse and clean data
                data = json.loads(line)
                
                # Process users (stage 1)
                if stage == 1:
                    clean_user = clean_user_object(data.get('user'))
                    if clean_user:
                        users_batch.append((
                            clean_user['id'],
                            clean_user['name'],
                            clean_user['screen_name'],
                            clean_user['description'],
                            clean_user['verified'],
                            clean_user['followers_count'],
                            clean_user['friends_count'],
                            clean_user['listed_count'],
                            clean_user['favorites_count'],
                            clean_user['status_count']
                        ))
                
                # Process tweets (stage 2)
                elif stage == 2:
                    clean_tweet = clean_tweet_object(data)
                    if clean_tweet:
                        tweets_batch.append((
                            clean_tweet['id'],
                            clean_tweet['text'],
                            clean_tweet['created_at'],
                            clean_tweet['in_reply_to_status_id'],
                            clean_tweet['in_reply_to_user'],
                            clean_tweet['user_id'],
                            clean_tweet['quoted_status_id'],
                            clean_tweet['retweeted_id'],
                            clean_tweet['quote_count'],
                            clean_tweet['reply_count'],
                            clean_tweet['retweet_count'],
                            clean_tweet['favorite_count'],
                            clean_tweet['possibly_sensitive'],
                            clean_tweet['language'],
                            clean_tweet['sentiment']
                        ))
                
                # Process entities (stage 3)
                elif stage == 3:
                    clean_tweet = clean_tweet_object(data)
                    if clean_tweet:
                        hashtags, mentions = extract_entities(data, clean_tweet['id'])
                        all_hashtags.extend(hashtags)
                        all_mentions.extend(mentions)
                
                valid_lines += 1
                
                # Process in batches to save memory
                if len(users_batch) >= BATCH_SIZE and stage == 1:
                    load_users_batch(cursor, users_batch)
                    users_batch = []
                        
                if len(tweets_batch) >= BATCH_SIZE and stage == 2:
                    load_tweets_batch(cursor, tweets_batch)
                    tweets_batch = []
                        
                if stage == 3:
                    if len(all_hashtags) >= BATCH_SIZE:
                        load_hashtags_batch(cursor, all_hashtags)
                        all_hashtags = []
                        
                    if len(all_mentions) >= BATCH_SIZE:
                        load_mentions_batch(cursor, all_mentions)
                        all_mentions = []
                        
                # Commit periodically and update progress
                if line_number % (BATCH_SIZE * 5) == 0:
                    connection.commit()
                    update_progress(json_file, stage, line_number)
                    
            except json.JSONDecodeError:
                invalid_lines += 1
                continue  # Skip invalid JSON lines
                
        pbar.close()
    
    # Process any remaining items in batches
    if stage == 1 and users_batch:
        load_users_batch(cursor, users_batch)
        
    if stage == 2 and tweets_batch:
        load_tweets_batch(cursor, tweets_batch)
        
    if stage == 3:
        if all_hashtags:
            load_hashtags_batch(cursor, all_hashtags)
        if all_mentions:
            load_mentions_batch(cursor, all_mentions)
    
    # Commit changes
    connection.commit()
    
    # Update final progress
    update_progress(json_file, stage, line_number)
    
    # Log summary for this file and stage
    log_summary(f"Completed {stage_name} for {json_file}: {valid_lines} valid, {invalid_lines} invalid lines")
            
    return valid_lines, invalid_lines

def process_stage(stage_number, json_files, description=None):
    """
    Process all files for a specific stage.
    
    Args:
        stage_number: 1 for users, 2 for tweets, 3 for entities
        json_files: List of JSON files to process
        description: Optional description for logging
    
    Returns:
        tuple: (valid_count, invalid_count)
    """
    stage_name = {1: "users", 2: "tweets", 3: "entities"}[stage_number]
    description = description or f"Processing {stage_name}"
    
    print(f"\n--- STEP {stage_number}: {description} ---")
    log_summary(f"Starting STEP {stage_number}: {description}")
    
    total_valid = 0
    total_invalid = 0
    
    for json_file in json_files:
        # Check if this file has already been completed for this stage
        if progress[json_file][stage_number] > 0:
            json_path = os.path.join(data_directory, json_file)
            with open(json_path, 'r', encoding='utf8') as file:
                file_lines = sum(1 for _ in file)
            
            if progress[json_file][stage_number] >= file_lines:
                print(f"Skipping {stage_name} for {json_file} - already processed")
                continue
                
        valid, invalid = process_file(json_file, stage=stage_number)
        total_valid += valid
        total_invalid += invalid
    
    print(f"Completed {stage_name} stage: {total_valid} valid, {total_invalid} invalid")
    return total_valid, total_invalid

# Main execution
if __name__ == "__main__":
    # Get a list of all JSON files in the directory
    files = os.listdir(data_directory)
    json_files = [file for file in files if file.endswith('.json')]
    
    if not json_files:
        print("No JSON files found in the directory.")
        sys.exit(1)
        
    total_files = len(json_files)
    print(f"Found {total_files} JSON files to process.")
    log_summary(f"Starting processing of {total_files} JSON files")
    
    # Set up database tables
    print("\n--- Setting up database structure ---")
    if not setup_database_tables():
        print("Failed to set up database tables. Exiting.")
        sys.exit(1)
    
    # Create database indexes
    if not create_indexes():
        print("Warning: Failed to create some indexes. Processing will continue but might be slower.")
    
    # Process each stage sequentially for all files
    total_valid_users, total_invalid_users = process_stage(1, json_files, "Processing Users")
    
    total_valid_tweets, total_invalid_tweets = process_stage(2, json_files, "Processing Tweets")
    
    total_valid_entities, total_invalid_entities = process_stage(3, json_files, "Processing Entities")
    
    # Final summary
    total_valid = total_valid_users + total_valid_tweets + total_valid_entities
    total_invalid = total_invalid_users + total_invalid_tweets + total_invalid_entities
    
    print("\n=== Processing Summary ===")
    print(f"Users processed: {total_valid_users} valid, {total_invalid_users} invalid")
    print(f"Tweets processed: {total_valid_tweets} valid, {total_invalid_tweets} invalid")
    print(f"Entities processed: {total_valid_entities} valid, {total_invalid_entities} invalid")
    print(f"Total: {total_valid} valid, {total_invalid} invalid lines")
    
    # Log final summary
    log_summary("=== Processing Complete ===")
    log_summary(f"Users processed: {total_valid_users} valid, {total_invalid_users} invalid")
    log_summary(f"Tweets processed: {total_valid_tweets} valid, {total_invalid_tweets} invalid")
    log_summary(f"Entities processed: {total_valid_entities} valid, {total_invalid_entities} invalid")
    log_summary(f"Total: {total_valid} valid, {total_invalid} invalid lines")
    
    # Close the connection
    cursor.close()
    connection.close()
    
    print("\nAll data processed successfully!")