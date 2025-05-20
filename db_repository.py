import pyodbc
import os

def get_connection():
    server = 'S20203142'
    database = 'airline_tweets'
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    return pyodbc.connect(connection_string)

def fetch_tweets_by_airline_ids(airline_ids):
    """
    Fetch all tweets where user_id is in airline_ids.
    """
    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ','.join(['?'] * len(airline_ids))
    query = f"SELECT * FROM dbo.tweet WHERE user_id IN ({placeholders})"
    cursor.execute(query, tuple(airline_ids))
    columns = [column[0] for column in cursor.description]
    tweets = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return tweets

def fetch_all_tweets():
    """
    Fetch all tweets from the database.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.tweet")
    columns = [column[0] for column in cursor.description]
    tweets = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return tweets

def fetch_tweet_by_id(tweet_id):
    """
    Fetch a single tweet by its id.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.tweet WHERE id = ?", tweet_id)
    row = cursor.fetchone()
    columns = [column[0] for column in cursor.description]
    tweet = dict(zip(columns, row)) if row else None
    conn.close()
    return tweet

def fetch_replies_to_tweet(tweet_id):
    """
    Fetch all tweets that are replies to a given tweet_id.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.tweet WHERE in_reply_to_status_id = ?", tweet_id)
    columns = [column[0] for column in cursor.description]
    replies = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return replies

def fetch_user_by_id(user_id):
    """
    Fetch a user by user_id.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.[user] WHERE id = ?", user_id)
    row = cursor.fetchone()
    columns = [column[0] for column in cursor.description]
    user = dict(zip(columns, row)) if row else None
    conn.close()
    return user

def fetch_airline_reply_tweets(airline_ids):
    """
    Fetch tweets where the user is an airline and it's a reply to someone.
    """
    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ','.join(['?'] * len(airline_ids))
    query = f"""
        SELECT * FROM dbo.tweet
        WHERE user_id IN ({placeholders}) AND in_reply_to_status_id IS NOT NULL
    """
    cursor.execute(query, tuple(airline_ids))
    columns = [column[0] for column in cursor.description]
    tweets = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return tweets

def fetch_tweet_by_id(tweet_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.tweet WHERE id = ?", tweet_id)
    row = cursor.fetchone()
    columns = [column[0] for column in cursor.description]
    tweet = dict(zip(columns, row)) if row else None
    conn.close()
    return tweet

def fetch_replies_to_tweet(tweet_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.tweet WHERE in_reply_to_status_id = ?", tweet_id)
    columns = [column[0] for column in cursor.description]
    replies = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return replies

fetch_airline_reply_tweets([22536055])