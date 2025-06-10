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

# Convert timestamp function
def convert_timestamp(ts):
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

# Safe integer conversion
def safe_int(val):
    """Safely convert value to integer."""
    try:
        if val is not None:
            return int(val)
        return None
    except (TypeError, ValueError):
        return None

# Clean text by removing newlines and trimming
def clean_text(text):
    """Clean text by removing newlines and trimming."""
    if text:
        return text.replace('\n', ' ').strip()
    return None

# Data processing functions
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
    
    # Create database indexes if they don't exist
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweet_user_id ON tweet(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweet_reply ON tweet(in_reply_to_status_id)")
        connection.commit()
        print("✓ Database indexes created or already exist")
        log_summary("Database indexes created or already exist")
    except:
        print("! Could not create indexes - performance might be affected")
        log_summary("Could not create indexes - performance might be affected")
    
    # Step 1: Load all users
    print("\n--- STEP 1: Processing Users ---")
    log_summary("Starting STEP 1: Processing Users")
    total_valid_users = 0
    total_invalid_users = 0
    
    for json_file in json_files:
        # Check if this file has already been completed for this stage
        if progress[json_file][1] > 0:
            with open(json_path, 'r', encoding='utf8') as file:
                file_lines = sum(1 for _ in file)
            
            if progress[json_file][1] >= file_lines:
                print(f"Skipping users for {json_file} - already processed")
                continue
                
        valid, invalid = process_file(json_file, stage=1)
        total_valid_users += valid
        total_invalid_users += invalid
    
    # Step 2: Load all tweets
    print("\n--- STEP 2: Processing Tweets ---")
    log_summary("Starting STEP 2: Processing Tweets")
    total_valid_tweets = 0
    total_invalid_tweets = 0
    
    for json_file in json_files:
        # Check if this file has already been completed for this stage
        if progress[json_file][2] > 0:
            with open(os.path.join(data_directory, json_file), 'r', encoding='utf8') as file:
                file_lines = sum(1 for _ in file)
            
            if progress[json_file][2] >= file_lines:
                print(f"Skipping tweets for {json_file} - already processed")
                continue
                
        valid, invalid = process_file(json_file, stage=2)
        total_valid_tweets += valid
        total_invalid_tweets += invalid
    
    # Step 3: Load all entities
    print("\n--- STEP 3: Processing Entities ---")
    log_summary("Starting STEP 3: Processing Entities")
    total_valid_entities = 0
    total_invalid_entities = 0
    
    for json_file in json_files:
        # Check if this file has already been completed for this stage
        if progress[json_file][3] > 0:
            with open(os.path.join(data_directory, json_file), 'r', encoding='utf8') as file:
                file_lines = sum(1 for _ in file)
            
            if progress[json_file][3] >= file_lines:
                print(f"Skipping entities for {json_file} - already processed")
                continue
                
        valid, invalid = process_file(json_file, stage=3)
        total_valid_entities += valid
        total_invalid_entities += invalid
    
    # Final summary
    total_valid = total_valid_users + total_valid_tweets + total_valid_entities
    total_invalid = total_invalid_users + total_invalid_tweets + total_invalid_entities
    
    print("\n=== Processing Summary ===")
    print(f"Users processed: {total_valid_users} valid, {total_invalid_users} invalid")
    print(f"Tweets processed: {total_valid_tweets} valid, {total_invalid_tweets} invalid")
    print(f"Entities processed: {total_valid_entities} valid, {total_invalid_entities} invalid")
    print(f"Total: {total_valid} valid, {total_invalid} invalid lines")
    print("\n✅ All data processed successfully!")
    
    # Log final summary
    log_summary("=== Processing Complete ===")
    log_summary(f"Users processed: {total_valid_users} valid, {total_invalid_users} invalid")
    log_summary(f"Tweets processed: {total_valid_tweets} valid, {total_invalid_tweets} invalid")
    log_summary(f"Entities processed: {total_valid_entities} valid, {total_invalid_entities} invalid")
    log_summary(f"Total: {total_valid} valid, {total_invalid} invalid lines")
    
    # Close the connection
    cursor.close()
    connection.close()
    