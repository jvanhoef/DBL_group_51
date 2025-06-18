import pyodbc
import pandas as pd


start_date = ""
end_date = ""

# start_date = '2019-05-22 12:20:00.000'
# end_date = '2019-07-22 12:20:00.000'

def get_connection():
    server = 'S20203142'
    database = 'airline_tweets'
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    return pyodbc.connect(connection_string)


#getters
def get_issue_counts():
    """Get counts of issues by type, sorted by count in descending order"""
    conn = get_connection()
    query = """
    SELECT 
        [issue_type],
        COUNT(*) AS issue_count
    FROM 
        [airline_tweets].[dbo].[detected_issues]
    GROUP BY 
        [issue_type]
    ORDER BY 
        issue_count DESC;
    """
    return pd.read_sql(query, conn)

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

#Milestone 1
def get_tweet_count(conn):
    cursor = conn.cursor()
    query = """
        SELECT COUNT(DISTINCT id)
        FROM tweet
    """
    params = []
    if start_date and end_date:
        query += " WHERE created_at BETWEEN ? AND ?"
        params = [start_date, end_date]
        cursor.execute(query, params)
        return cursor.fetchone()[0] or 0
    
    cursor.execute(query)

    return cursor.fetchone()[0] or 0

def get_tweet_size(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SUM(size) * 8.0 / 1024 / 1024 
        FROM sys.master_files 
        WHERE database_id = DB_ID('airline_tweets')
    """)
    return cursor.fetchone()[0] or 0

def get_airline_mentions(conn, airline_id):
    cursor = conn.cursor()
    query = """
        SELECT COUNT(*)
        FROM mention m
        join tweet t on m.tweet_id = t.id
        WHERE name = 'AmericanAir'
    """
    
    params = []
    if start_date and end_date:
        query += " AND created_at BETWEEN ? AND ?"
        params = [start_date, end_date]
        cursor.execute(query, params)
        return cursor.fetchone()[0] or 0
    
    cursor.execute(query)  
    return cursor.fetchone()[0] or 0

def get_conversation_count_by_airline(conn, airline_id):
    cursor = conn.cursor()
    query = """
        SELECT COUNT(*)
        FROM conversation c
        join tweet t on c.root_tweet_id = t.id
        WHERE airline_id = ?
    """
    params = []
    if start_date and end_date:
        query += " AND created_at BETWEEN ? AND ?"
        params = [airline_id, start_date, end_date]
        cursor.execute(query, params)
        return cursor.fetchone()[0] or 0
    
    cursor.execute(query, (airline_id,))
    return cursor.fetchone()[0] or 0

def get_tweet_volume_over_time(conn):
    cursor = conn.cursor()
    query = """
        SELECT 
            CAST(created_at AS DATE) AS date,
            COUNT(*) AS tweet_count
        FROM tweet
    """
    params = []
    if start_date and end_date:
        query += " WHERE created_at BETWEEN ? AND ?"
        params = [start_date, end_date]
    query += " GROUP BY CAST(created_at AS DATE) ORDER BY date"
    cursor.execute(query, params)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame.from_records(rows, columns=columns)

def get_language_counts(conn):
    cursor = conn.cursor()
    query = """
        SELECT TOP 10 language, COUNT(*) as count
        FROM tweet
    """
    
    params = []
    if start_date and end_date:
        query += " WHERE created_at BETWEEN ? AND ?"
        params = [start_date, end_date]
        query += " GROUP BY language ORDER BY count DESC"
        cursor.execute(query, params)
        return cursor.fetchall()
    
    query += " GROUP BY language ORDER BY count DESC"
    cursor.execute(query)
    return cursor.fetchall()


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

# Milestone 2
def get_conversation_improvement_counts(conn, start_date=None, end_date=None):
    cursor = conn.cursor()

    query = """
        SELECT cs.sentiment_change, COUNT(*) as count
        FROM conversation_sentiment cs
        JOIN conversation c ON cs.conversation_id = c.id
        JOIN tweet t ON c.root_tweet_id = t.id
    """
    params = []
    if start_date and end_date:
        query += " WHERE created_at BETWEEN ? AND ?"
        query += " GROUP BY cs.sentiment_change"
        params = [start_date, end_date]
        cursor.execute(query, params)
        return cursor.fetchall()
    
    query += " GROUP BY cs.sentiment_change"
    cursor.execute(query, params)
    return cursor.fetchall()

def get_last_user_sentiment_counts(conn):
    cursor = conn.cursor()
    query = """
        SELECT 
            CASE 
                WHEN cs.final_sentiment < 0 THEN 'negative'
                WHEN cs.final_sentiment = 0 THEN 'neutral'
                WHEN cs.final_sentiment > 0 THEN 'positive'
                ELSE 'unknown'
            END as sentiment_group,
            COUNT(*) as count
        FROM conversation_sentiment cs
        JOIN conversation c ON cs.conversation_id = c.id
        JOIN tweet t ON c.root_tweet_id = t.id
    """
    params = []
    if start_date and end_date:
        query += " WHERE t.created_at BETWEEN ? AND ?"
        query += " GROUP BY CASE WHEN cs.final_sentiment < 0 THEN 'negative' WHEN cs.final_sentiment = 0 THEN 'neutral' WHEN cs.final_sentiment > 0 THEN 'positive' ELSE 'unknown' END"
        params = [start_date, end_date]
        cursor.execute(query, params)
        return cursor.fetchall()
    
    query += " GROUP BY CASE WHEN cs.final_sentiment < 0 THEN 'negative' WHEN cs.final_sentiment = 0 THEN 'neutral' WHEN cs.final_sentiment > 0 THEN 'positive' ELSE 'unknown' END"
    cursor.execute(query, params)
    return cursor.fetchall()

def get_response_time_buckets(conn):
    cursor = conn.cursor()
    query = """
        SELECT
            CASE
                WHEN avg_response_time_sec < 1800 THEN 'Within 30 min'
                WHEN avg_response_time_sec >= 1800 AND avg_response_time_sec < 3600 THEN '30-60 min'
                WHEN avg_response_time_sec >= 3600 AND avg_response_time_sec < 7200 THEN '60-120 min'
                WHEN avg_response_time_sec >= 7200 THEN 'Above 120 min'
            END as response_time_bucket,
            COUNT(*) as count
        FROM conversation_sentiment cs
        JOIN conversation c ON cs.conversation_id = c.id
        JOIN tweet t ON c.root_tweet_id = t.id
    """
    params = []
    if start_date and end_date:
        query += " WHERE t.created_at BETWEEN ? AND ?"
        params = [start_date, end_date]
    query += " GROUP BY CASE WHEN avg_response_time_sec < 1800 THEN 'Within 30 min' WHEN avg_response_time_sec >= 1800 AND avg_response_time_sec < 3600 THEN '30-60 min' WHEN avg_response_time_sec >= 3600 AND avg_response_time_sec < 7200 THEN '60-120 min' WHEN avg_response_time_sec >= 7200 THEN 'Above 120 min' END"
    cursor.execute(query, params)
    return cursor.fetchall()

def get_conversations_with_tweets_and_sentiment():
    conn = get_connection()
    """
    Returns a DataFrame with: conversation_id, airline, tweet_id, created_at, sentiment, text, resolution_status
    """
    query = """
   SELECT
        c.id AS conversation_id,
        u.screen_name AS airline,
        t.id AS tweet_id,
        t.created_at,
        t.text,
        t.sentiment,
        di.resolved_in_conversation,
        di.issue_type
    FROM conversation c
    JOIN conversation_tweet ct ON c.id = ct.conversation_id
    JOIN tweet t ON ct.tweet_id = t.id
    JOIN [user] u ON c.airline_id = u.id
    JOIN detected_issues di ON di.conversation_id = c.id
    ORDER BY c.id, t.created_at
    """
    df = pd.read_sql(query, conn)
    return df

def get_available_categories():
    """Get list of all issue categories from database"""
    conn = get_connection()
    query = """
    SELECT DISTINCT issue_type
    FROM detected_issues
    ORDER BY issue_type;
    """
    return pd.read_sql(query, conn)['issue_type'].tolist()

def get_available_airlines(issue_type=None):
    """Get list of airlines with issues of the specified type"""
    conn = get_connection()
    query = """
    SELECT DISTINCT u.screen_name as airline
    FROM detected_issues di
    JOIN conversation c ON di.conversation_id = c.id
    JOIN [user] u ON c.airline_id = u.id
    WHERE 1=1
    """ + (f"AND di.issue_type = '{issue_type}'" if issue_type else "") + """
    ORDER BY u.screen_name;
    """
    return pd.read_sql(query, conn)['airline'].tolist()

def get_sentiment_data(issue_type, selected_airlines=None):
    """Get sentiment data for specified issue type and airlines"""
    conn = get_connection()
    
    airline_filter = ""
    if selected_airlines:
        airlines_str = "','".join(selected_airlines)
        airline_filter = f"AND u.screen_name IN ('{airlines_str}')"
    
    query = f"""
    SELECT 
        u.screen_name as airline,
        cs.sentiment_change
    FROM detected_issues di
    JOIN conversation c ON di.conversation_id = c.id
    JOIN [user] u ON c.airline_id = u.id
    JOIN conversation_sentiment cs ON di.conversation_id = cs.conversation_id
    WHERE di.issue_type = '{issue_type}'
        AND cs.sentiment_change IS NOT NULL
        {airline_filter}
    """
    
    # Get raw data
    df = pd.read_sql(query, conn)
    
    # Process data for each airline
    results = []
    for airline in df['airline'].unique():
        airline_data = df[df['airline'] == airline]['sentiment_change']        # Count occurrences of each sentiment change category
        improved = len(airline_data[airline_data == 'improved'])
        worsened = len(airline_data[airline_data == 'worsened'])  # Using 'declined' from DB
        unchanged = len(airline_data[airline_data == 'unchanged'])
        total = improved + unchanged + worsened
        # Calculate percentages
        if total > 0:
            results.append({
                'airline': airline,
                'total_issues': total,
                'improved_count': improved,
                'worsened_count': worsened,
                'unchanged_count': unchanged,
                'improved_pct': (improved/total*100),
                'worsened_pct': (worsened/total*100),
                'unchanged_pct': (unchanged/total*100)
            })
    
    return pd.DataFrame(results)

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
    