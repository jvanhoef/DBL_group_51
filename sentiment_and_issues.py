# run_sentiment_and_issues.py
"""
Script to create and populate tables for sentiment analysis and issue detection.
- Creates required tables with proper relationships
- Analyzes tweet sentiment
- Detects issues in conversations
- Tracks sentiment changes and response patterns
"""
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import emoji
from datetime import datetime
import re
from db_repository import get_connection
import pyodbc
from tqdm import tqdm
import logging
import numpy as np
from langdetect import detect

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize device
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Initialize models and tokenizers
english_model = AutoModelForSequenceClassification.from_pretrained("finiteautomata/bertweet-base-sentiment-analysis")
english_tokenizer = AutoTokenizer.from_pretrained("finiteautomata/bertweet-base-sentiment-analysis")
english_model.to(device)
english_model.eval()

multilingual_model = AutoModelForSequenceClassification.from_pretrained("cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual")
multilingual_tokenizer = AutoTokenizer.from_pretrained("cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual")
multilingual_model.to(device)
multilingual_model.eval()

# Label mapping for sentiment scores
label_mapping = {'NEG': -1, 'NEU': 0, 'POS': 1}

ISSUE_KEYWORDS = {
    'delay': ['delay', 'delayed', 'late', 'cancelled', 'cancellation', 'rescheduled', 'no show', 'missed connection', 'standby', 'overbooked', 'missed flight', 'missed flights'],
    'luggage': ['lost luggage', 'lost baggage', 'damaged luggage', 'broken suitcase', 'missing bag', 'baggage fee', 'overweight bag', 'delayed baggage', 'baggage claim', 'luggage handling'],
    'customer_service': ['rude staff', 'unhelpful', 'no response', 'ignored', 'bad service', 'disrespectful', 'poor communication', 'agent', 'support', 'customer care', 'complaint handling', 'worst customer service', 'attitude', 'retraining', 'role change'],
    'booking': ['booking error', 'ticket problem', 'reservation', 'seat assignment', 'check-in', 'boarding pass', 'upgrade denied', 'cancel my booking', 'refund', 'confirmation'],
    'pricing': ['extra charges', 'hidden fees', 'ticket price', 'refund', 'cancellation fee', 'change fee', 'baggage fee', 'overcharge', 'expensive', 'no compensation'],
    'flight_experience': [
        'seat comfort', 'legroom', 'dirty', 'smelly', 'broken seat', 'temperature', 'noisy', 'air conditioning', 
        'food quality', 'inflight meal', 'entertainment system', 'underseat storage', 'personal item', 'small storage',
        'crowded', 'uncomfortable', 'flight experience', 'inflight', 'entertainment', 'meal', 'food', 'drink', 'wifi'
    ],
    'safety': ['safety', 'emergency', 'security check', 'scary', 'dangerous', 'turbulence', 'emergency landing', 'staff negligence', 'unprofessional'],
    'communication': ['no updates', 'lack of information', 'missed announcements', 'confusing', 'wrong info', 'not informed', 'app failure', 'website down', 'lost boarding pass'],
    'accessibility': ['wheelchair', 'special assistance', 'disability', 'elderly', 'medical help', 'service animal', 'no help', 'unaccommodating'],
    'refunds': ['refund delayed', 'no refund', 'compensation', 'voucher', 'claim denied', 'delay compensation', 'poor handling']
}



def create_analysis_tables(conn):
    """Create tables for sentiment analysis with proper relationships"""
    try:
        cursor = conn.cursor()
        
        print("Creating/recreating sentiment analysis tables...")
        
        # First drop existing tables in correct order
        drop_tables = [
            'conversation_sentiment',
            'sentiment_log',
            'detected_issues',
            'tweet_sentiment'
        ]
        
        for table in drop_tables:
            print(f"Dropping {table} if exists...")
            cursor.execute(f"""
                IF OBJECT_ID('dbo.{table}', 'U') IS NOT NULL
                DROP TABLE dbo.{table}
            """)
        
        # Create tweet sentiment table
        cursor.execute("""
            CREATE TABLE [dbo].[tweet_sentiment] (
                tweet_id BIGINT,
                conversation_id BIGINT,
                sentiment_label NVARCHAR(510),                
                sentiment_score FLOAT,
                confidence FLOAT,
                is_airline_tweet BIT,
                tweet_position INT,
                created_at DATETIME2,
                language NVARCHAR(10),
                CONSTRAINT PK_tweet_sentiment PRIMARY KEY (tweet_id, conversation_id),
                CONSTRAINT FK_tweet_sentiment_tweet FOREIGN KEY (tweet_id) REFERENCES tweet(id) ON DELETE CASCADE,
                CONSTRAINT FK_tweet_sentiment_conversation FOREIGN KEY (conversation_id) REFERENCES conversation(id) ON DELETE CASCADE
            )
        """)
        print("Created tweet_sentiment table")
        
        # Create sentiment log table for tracking progression
        cursor.execute("""
            CREATE TABLE [dbo].[sentiment_log] (
                conversation_id BIGINT,
                position INT,
                sentiment_score FLOAT,
                tweet_time DATETIME2,
                is_airline_tweet BIT,
                CONSTRAINT PK_sentiment_log PRIMARY KEY (conversation_id, position),
                CONSTRAINT FK_sentiment_log_conversation FOREIGN KEY (conversation_id) REFERENCES conversation(id) ON DELETE CASCADE
            )
        """)
        print("Created sentiment_log table")

        # Create conversation sentiment table for overall metrics
        cursor.execute("""
            CREATE TABLE [dbo].[conversation_sentiment] (
                conversation_id BIGINT PRIMARY KEY,
                initial_sentiment FLOAT,
                final_sentiment FLOAT,
                sentiment_change NVARCHAR(50),
                first_response_time_sec BIGINT,
                avg_response_time_sec FLOAT,
                user_tweets_count INT,
                airline_tweets_count INT,
                resolved_to_dm BIT,
                CONSTRAINT FK_conversation_sentiment_conversation FOREIGN KEY (conversation_id) REFERENCES conversation(id) ON DELETE CASCADE
            )
        """)
        print("Created conversation_sentiment table")

        # Create detected issues table
        cursor.execute("""
            CREATE TABLE [dbo].[detected_issues] (
                conversation_id BIGINT,
                issue_type NVARCHAR(510),
                severity_score FLOAT,
                first_mention_position INT,
                resolved_in_conversation BIT,
                CONSTRAINT PK_detected_issues PRIMARY KEY (conversation_id, issue_type),
                CONSTRAINT FK_detected_issues_conversation FOREIGN KEY (conversation_id) REFERENCES conversation(id) ON DELETE CASCADE
            )
        """)
        print("Created detected_issues table")        # Create indexes for better performance
        cursor.execute("CREATE INDEX idx_tweet_sentiment_conv ON tweet_sentiment(conversation_id)")
        cursor.execute("CREATE INDEX idx_tweet_sentiment_tweet ON tweet_sentiment(tweet_id)")
        cursor.execute("CREATE INDEX idx_sentiment_log_conv ON sentiment_log(conversation_id)")
        cursor.execute("CREATE INDEX idx_sentiment_log_pos ON sentiment_log(position)")
        cursor.execute("CREATE INDEX idx_detected_issues_conv ON detected_issues(conversation_id)")
        cursor.execute("CREATE INDEX idx_detected_issues_type ON detected_issues(issue_type)")
        
        conn.commit()
        print("Analysis tables created successfully")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating tables: {str(e)}")
        raise

def analyze_sentiment(tweets):
    """Analyze sentiment for a batch of tweets with their languages using batch processing"""
    if not tweets:
        logger.warning("No tweets to analyze")
        return []
    
    results = []
    
    # Separate tweets by language
    english_texts = []
    multilingual_texts = []
    tweet_mapping = []  # Keep track of original tweet order
    
    for i, tweet in enumerate(tweets):
        if not tweet or len(tweet) < 6:  # Check if tweet has all required fields
            logger.warning(f"Invalid tweet structure at index {i}")
            results.append({
                'label': 'NEU',
                'confidence': 0.0,
                'score': 0.0,
                'language': 'en'
            })
            continue
            
        try:
            text = tweet[2]  # text field
            if not text or not isinstance(text, str):
                logger.warning(f"Invalid tweet text for tweet ID {tweet[1]}")
                results.append({
                    'label': 'NEU',
                    'confidence': 0.0,
                    'score': 0.0,
                    'language': tweet[5] if len(tweet) > 5 else 'en'
                })
                continue
                
            lang = tweet[5] if tweet[5] else 'en'  # language field with fallback
            
            # Preprocess text
            text = emoji.demojize(text.strip()) if text else ""
            
            if not text:  # Skip empty texts
                logger.warning(f"Empty text for tweet ID {tweet[1]}")
                results.append({
                    'label': 'NEU',
                    'confidence': 0.0,
                    'score': 0.0,
                    'language': lang
                })
                continue
            
            if lang == 'en':
                english_texts.append(text)
                tweet_mapping.append(('en', i))
            else:
                multilingual_texts.append(text)
                tweet_mapping.append(('multi', i))
                
        except Exception as e:
            logger.error(f"Error preprocessing tweet {tweet[1] if len(tweet) > 1 else 'unknown'}: {str(e)}")
            results.append({
                'label': 'NEU',
                'confidence': 0.0,
                'score': 0.0,
                'language': tweet[5] if len(tweet) > 5 else 'en'
            })
    
    # Initialize results list with neutral sentiments
    results = [None] * len(tweets)
    for i in range(len(tweets)):
        results[i] = {
            'label': 'NEU',
            'confidence': 0.0,
            'score': 0.0,
            'language': tweets[i][5] if tweets[i] and len(tweets[i]) > 5 else 'en'
        }
    
    try:        # Process English tweets in batches
        if english_texts:
            batch_size = 512  # Increased batch size for faster processing
            for i in range(0, len(english_texts), batch_size):
                batch = english_texts[i:i + batch_size]
                try:
                    inputs = english_tokenizer(batch, return_tensors="pt", truncation=True, 
                                            max_length=128, padding=True)
                    inputs = {k: v.to(device) for k, v in inputs.items()}
                    
                    with torch.no_grad():
                        outputs = english_model(**inputs)
                        probs = torch.nn.functional.softmax(outputs.logits, dim=1)
                        scores = probs.cpu().numpy()
                    
                    for j, score in enumerate(scores):
                        idx = i + j
                        if idx >= len(tweet_mapping):
                            break
                            
                        lang, orig_idx = tweet_mapping[idx]
                        if lang != 'en':
                            continue
                        
                        label_idx = score.argmax()
                        label = ['NEG', 'NEU', 'POS'][label_idx]
                        confidence = float(score[label_idx])
                        sentiment_score = label_mapping[label] * confidence
                        
                        results[orig_idx] = {
                            'label': label,
                            'confidence': confidence,
                            'score': sentiment_score,
                            'language': 'en'
                        }
                except Exception as e:
                    logger.error(f"Error processing English batch {i}: {str(e)}")
                    continue
          # Process multilingual tweets in batches
        if multilingual_texts:
            batch_size = 256  # Increased batch size to match English processing
            for i in range(0, len(multilingual_texts), batch_size):
                batch = multilingual_texts[i:i + batch_size]
                try:
                    inputs = multilingual_tokenizer(batch, return_tensors="pt", truncation=True, max_length=128, padding=True)
                    inputs = {k: v.to(device) for k, v in inputs.items()}
                    
                    with torch.no_grad(), torch.amp.autocast('cuda'):  # Use automatic mixed precision
                        outputs = multilingual_model(**inputs)
                        probs = torch.nn.functional.softmax(outputs.logits, dim=1)
                        scores = probs.cpu().numpy()
                    
                    for j, score in enumerate(scores):
                        idx = i + j
                        if idx >= len(tweet_mapping):
                            break
                        
                        lang, orig_idx = tweet_mapping[idx]
                        if lang != 'multi':
                            continue
                        
                        label_idx = score.argmax()
                        confidence = float(score[label_idx])
                        
                        if label_idx == 0:  # negative
                            label = 'NEG'
                            sentiment_score = -confidence
                        elif label_idx == 2:  # positive
                            label = 'POS'
                            sentiment_score = confidence
                        else:  # neutral
                            label = 'NEU'
                            sentiment_score = 0
                        
                        if orig_idx < len(results):  # Safety check
                            results[orig_idx] = {
                                'label': label,
                                'confidence': confidence,
                                'score': sentiment_score,
                                'language': tweets[orig_idx][5] if tweets[orig_idx] and len(tweets[orig_idx]) > 5 else 'en'
                            }
                except Exception as e:
                    logger.error(f"Error processing multilingual batch {i}: {str(e)}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
    
    return results

def classify_issues(text):
    """Classify issues in a text using keyword matching"""
    text = text.lower()
    detected = set()
    for issue, keywords in ISSUE_KEYWORDS.items():
        if any(k in text for k in keywords):
            detected.add(issue)
    return list(detected)

def process_conversations(conn):
    """Process conversations and populate tables"""
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # Get conversations with unprocessed tweets using a CTE for better organization
        query = """
            WITH UnprocessedTweets AS (
                SELECT DISTINCT 
                    c.id as conversation_id,
                    t.id as tweet_id,
                    t.text,
                    t.created_at,
                    CAST(CASE WHEN t.user_id = c.airline_id THEN 1 ELSE 0 END as BIT) as is_airline,
                    COALESCE(t.language, 'en') as language,
                    ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY t.created_at) as position
                FROM conversation c
                INNER JOIN conversation_tweet ct ON c.id = ct.conversation_id
                INNER JOIN tweet t ON ct.tweet_id = t.id
                LEFT JOIN tweet_sentiment ts ON t.id = ts.tweet_id AND c.id = ts.conversation_id
                WHERE ts.tweet_id IS NULL
            )
            SELECT *
            FROM UnprocessedTweets
            ORDER BY conversation_id, created_at;
        """
        cursor.execute(query)
        
        rows = cursor.fetchall()
        logger.info(f"Found {len(rows)} unprocessed tweets")
        
        if not rows:
            logger.info("No new tweets to process")
            return

        # Group by conversation
        conversations = {}
        for row in rows:
            conv_id = row[0]
            if conv_id not in conversations:
                conversations[conv_id] = []
            conversations[conv_id].append(row)
        
        # Process each conversation
        for conv_id, tweets in tqdm(conversations.items(), desc="Processing conversations"):            
            try:
                sentiments = analyze_sentiment(tweets)
                
                # Store tweet sentiments and build sentiment log
                for tweet, sentiment, pos in zip(tweets, sentiments, range(1, len(tweets) + 1)):
                    try:
                        # Insert tweet sentiment
                        cursor.execute("""
                            INSERT INTO tweet_sentiment (
                                tweet_id, conversation_id, sentiment_label, 
                                sentiment_score, confidence, is_airline_tweet,
                                tweet_position, created_at, language
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            tweet[1],  # tweet_id
                            conv_id,
                            sentiment['label'],
                            sentiment['score'],
                            sentiment['confidence'],
                            tweet[4],  # is_airline
                            pos,
                            tweet[3],  # created_at
                            sentiment['language']
                        ))
                        
                        # Insert sentiment log entry
                        cursor.execute("""
                            INSERT INTO sentiment_log (
                                conversation_id, position, sentiment_score,
                                tweet_time, is_airline_tweet
                            ) VALUES (?, ?, ?, ?, ?)
                        """, (
                            conv_id,
                            pos,
                            sentiment['score'],
                            tweet[3],  # created_at
                            tweet[4]   # is_airline
                        ))
                    except Exception as e:
                        logger.error(f"Error processing tweet {tweet[1]} in conversation {conv_id}: {str(e)}")
                        raise
                  # Calculate conversation metrics - only using user tweets
                user_sentiments = [(s['score'], pos) for pos, (t, s) in enumerate(zip(tweets, sentiments)) if not t[4]]
                if len(user_sentiments) >= 2:
                    initial_sentiment = user_sentiments[0][0]  # First user tweet sentiment
                    final_sentiment = user_sentiments[-1][0]   # Last user tweet sentiment
                    
                    initial_category = get_sentiment_category(initial_sentiment)
                    final_category = get_sentiment_category(final_sentiment)
                    
                    if initial_category != final_category:
                        # Only count as improved if moving to a better category
                        sentiment_change = 'improved' if (
                            (initial_category == 'negative' and final_category in ['neutral', 'positive']) or 
                            (initial_category == 'neutral' and final_category == 'positive')
                        ) else 'worsened'
                    else:
                        sentiment_change = 'unchanged'
                else:
                    # If there aren't at least 2 user tweets, can't determine change
                    initial_sentiment = user_sentiments[0][0] if user_sentiments else 0.0
                    final_sentiment = initial_sentiment
                    sentiment_change = 'unchanged'
                
                # Calculate response times
                response_times = []
                for i in range(len(tweets)-1):
                    if not tweets[i][4] and tweets[i+1][4]:  # user tweet followed by airline tweet
                        response_time = (tweets[i+1][3] - tweets[i][3]).total_seconds()
                        response_times.append(response_time)
                
                first_response = response_times[0] if response_times else None
                avg_response = sum(response_times) / len(response_times) if response_times else None
                
                # Count tweets by type
                airline_tweets = sum(1 for t in tweets if t[4])
                user_tweets = len(tweets) - airline_tweets
                  # Check for DM resolution and get evidence
                has_dm, dm_evidence = detect_dm_resolution(tweets)
                
                # Store conversation metrics
                cursor.execute("""
                    INSERT INTO conversation_sentiment (
                        conversation_id, initial_sentiment, final_sentiment,
                        sentiment_change, first_response_time_sec,
                        avg_response_time_sec, user_tweets_count,
                        airline_tweets_count, resolved_to_dm
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    conv_id,
                    initial_sentiment,
                    final_sentiment,
                    sentiment_change,
                    first_response,
                    avg_response,
                    user_tweets,
                    airline_tweets,
                    has_dm
                ))
                
                # Process and store issues
                detected_issues = {}  # Track issues and their first occurrence
                for pos, (tweet, sentiment) in enumerate(zip(tweets, sentiments), 1):
                    issues = classify_issues(tweet[2])
                    for issue_type in issues:
                        # Only store first occurrence of each issue type
                        if issue_type not in detected_issues:
                            detected_issues[issue_type] = {
                                'position': pos,
                                'severity': abs(sentiment['score'])
                            }
                
                # Insert unique issues
                for issue_type, issue_data in detected_issues.items():
                    cursor.execute("""
                        INSERT INTO detected_issues (
                            conversation_id, issue_type, severity_score,
                            first_mention_position, resolved_in_conversation
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        conv_id,
                        issue_type,
                        issue_data['severity'],
                        issue_data['position'],
                        has_dm  # Consider DM resolution as issue resolution
                    ))
                
                conn.commit()
            except Exception as e:
                logger.error(f"Error processing conversation {conv_id}: {str(e)}")
                conn.rollback()
                raise
        
        logger.info("All conversations processed successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing conversations: {str(e)}")
        raise

def get_sentiment_category(score):
    if score > 0.2:
        return 'positive'
    elif score < -0.2:
        return 'negative'
    else:
        return 'neutral'

# Dictionary of known airlines with their IDs
KNOWN_AIRLINES = {
    'KLM': 106062176,
    'AirFrance': 106062176,
    'British_Airways': 18332190,
    'AmericanAir': 22536055,
    'Lufthansa': 124476322,
    'easyJet': 38676903,
    'RyanAir': 1542862735,
    'SingaporeAir': 253340062,
    'Qantas': 218730857,
    'VirginAtlantic': 20626359
}

# Remove old phrase lists since we now use regex pattern matching

def detect_dm_resolution(tweets):
    """
    Detect if a conversation was resolved by moving to DMs.
    
    Args:
        tweets: List of tweet tuples (from the conversation)
        
    Returns:
        tuple: (bool, str) - (was resolved in DMs, evidence text)
    """
    for tweet in tweets:
        if not tweet[4]:  # Skip non-airline tweets
            continue
            
        text = tweet[2].lower()  # Get tweet text and convert to lowercase
        text = re.sub(r'http\S+', '', text)  # Remove URLs to prevent false positives
        
        # Simple pattern matching for dm/dms with word boundaries
        basic_dm = re.search(r'\b(dm|dms|direct message[s]?)\b', text)
        if basic_dm:
            # Get some context around the match
            start = max(0, basic_dm.start() - 30)
            end = min(len(text), basic_dm.end() + 30)
            return True, text[start:end].strip()
                    
    return False, ""

def main():
    """Main function to create tables and process conversations"""
    conn = get_connection()
    try:
        print("Creating analysis tables...")
        create_analysis_tables(conn)
        
        print("Processing conversations...")
        process_conversations(conn)
        
        print("Analysis completed successfully!")
        
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
