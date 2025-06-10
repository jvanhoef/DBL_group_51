import pyodbc
import pandas as pd

def get_connection():
    server = 'S20203142'
    database = 'airline_tweets'
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    return pyodbc.connect(connection_string)

#Function to truncate(empty tables
def delete_tables(tables):
    conn = get_connection()
    cursor = conn.cursor()
    """
    Delete all rows from the specified tables while respecting foreign key constraints.
    """
    for table in tables:
        try:
            cursor.execute(f"DELETE FROM dbo.[{table}]")
            print(f"Table '{table}' cleared successfully.")
        except pyodbc.Error as e:
            print(f"Error clearing table '{table}': {e}")
            
            #Function to truncate(empty tables
def truncate_tables(tables):
    conn = get_connection()
    cursor = conn.cursor()
    """
    Delete all rows from the specified tables while respecting foreign key constraints.
    """
    for table in tables:
        try:
            cursor.execute(f"Truncate table dbo.[{table}]")
            print(f"Table '{table}' cleared successfully.")
        except pyodbc.Error as e:
            print(f"Error clearing table '{table}': {e}")

#getters
def get_airline_id(conn, airline_screen_name):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id
        FROM [user]
        WHERE screen_name = ?
    """, (airline_screen_name,))
    row = cursor.fetchone()
    return row[0] if row else None

def get_screen_name_by_id(conn, user_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT screen_name FROM [user] WHERE id = ?
    """, (user_id,))
    row = cursor.fetchone()
    return row[0] if row else None

def get_relevant_tweets(conn, airline_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM tweet
        WHERE user_id = ? OR in_reply_to_user = ?
        ORDER BY created_at
    """, (airline_id, airline_id))
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame.from_records(rows, columns=columns)

def get_conversation_text_by_id(conn, conversation_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            t.created_at,
            u.screen_name,
            t.text
        FROM conversation_tweet ct
        JOIN tweet t ON ct.tweet_id = t.id
        JOIN [user] u ON t.user_id = u.id
        WHERE ct.conversation_id = ?
        ORDER BY t.created_at
    """, (conversation_id,))
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame.from_records(rows, columns=columns)

# Add to db_repository.py

def create_indexes(conn):
    """Create indexes to speed up queries."""
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweet_user_id ON tweet(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweet_in_reply ON tweet(in_reply_to_status_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweet_created ON tweet(created_at)")
        conn.commit()
    except Exception as e:
        print(f"Error creating indexes: {e}")

#setters
def insert_conversation(conn, user_id, airline_id, root_tweet_id):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO conversation (user_id, airline_id, root_tweet_id)
        OUTPUT INSERTED.id
        VALUES (?, ?, ?)
    """, (user_id, airline_id, root_tweet_id))
    conv_id = cursor.fetchone()[0]
    conn.commit()
    return conv_id

def insert_conversation_tweets(conn, conversation_id, tweet_ids):
    cursor = conn.cursor()
    for tid in tweet_ids:
        cursor.execute("""
            INSERT INTO conversation_tweet (conversation_id, tweet_id)
            VALUES (?, ?)
        """, (conversation_id, tid))
    conn.commit()
    
def get_tweet_by_id(conn, tweet_id):
    """Get a single tweet by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tweet WHERE id = ?", (tweet_id,))
    columns = [column[0] for column in cursor.description]
    row = cursor.fetchone()
    return dict(zip(columns, row)) if row else None
    
#Debugging
def print_conversation_nicely(conversation_id, airline_id):
    conn = get_connection()
    df = get_conversation_text_by_id(conn, conversation_id)
    airline_screen_name = get_screen_name_by_id(conn, airline_id)

    if df.empty:
        print(f"No conversation found for ID {conversation_id}")
        return

    print(f"--- Conversation Start ({airline_screen_name}) ---")

    for _, row in df.iterrows():
        timestamp = int(pd.to_datetime(row["created_at"]).timestamp() * 1000)
        print(f"(Time: {timestamp}) @{row['screen_name']}: {row['text']}")

    print("--- Conversation End ---")
    
print_conversation_nicely(145414, 5920532)