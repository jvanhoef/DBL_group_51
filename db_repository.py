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

def get_response_time_buckets(conn, airline_id):
    cursor = conn.cursor()
    query = """
        SELECT
            CASE
                WHEN first_response_time_sec < 1800 THEN 'Within 30 min'
                WHEN first_response_time_sec >= 1800 AND first_response_time_sec < 3600 THEN '30-60 min'
                WHEN first_response_time_sec >= 3600 AND first_response_time_sec < 7200 THEN '60-120 min'
                WHEN first_response_time_sec >= 7200 THEN 'Above 120 min'
            END as response_time_bucket,
            COUNT(*) as count
        FROM conversation_sentiment cs
        JOIN conversation c ON cs.conversation_id = c.id
        JOIN tweet t ON c.root_tweet_id = t.id
        WHERE c.airline_id = ?
    """
    params = [airline_id]
    if start_date and end_date:
        query += " AND t.created_at BETWEEN ? AND ?"
        params = [airline_id, start_date, end_date]
    query += " GROUP BY CASE WHEN first_response_time_sec < 1800 THEN 'Within 30 min' WHEN first_response_time_sec >= 1800 AND first_response_time_sec < 3600 THEN '30-60 min' WHEN first_response_time_sec >= 3600 AND first_response_time_sec < 7200 THEN '60-120 min' WHEN first_response_time_sec >= 7200 THEN 'Above 120 min' END"
    cursor.execute(query, params)
    return cursor.fetchall()

def get_issue_type_count(conn, airline_id):
    cursor = conn.cursor()
    query = """
        SELECT issue_type, COUNT(*) as issue_count
        FROM detected_issues d
        JOIN conversation c ON d.conversation_id = c.id
        JOIN tweet t ON c.root_tweet_id = t.id
        WHERE c.airline_id = ?
    """
    params = []
    if start_date and end_date:
        query += " AND created_at BETWEEN ? AND ?"
        params = [airline_id, start_date, end_date]
    query += " GROUP BY issue_type ORDER BY issue_count DESC"
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query, (airline_id))
    return cursor.fetchall()
    
def get_activity_correlation(conn, start_date=None, end_date=None):
    """
    Returns a DataFrame with: hour_of_day, user_tweets, airline_tweets
    Optionally filters by tweet.created_at between start_date and end_date.
    """
    params = []
    where_clause = ""
    if start_date and end_date:
        where_clause = "WHERE t.created_at BETWEEN ? AND ?"
        params = [start_date, end_date]
    elif start_date:
        where_clause = "WHERE t.created_at >= ?"
        params = [start_date]
    elif end_date:
        where_clause = "WHERE t.created_at <= ?"
        params = [end_date]

    query = f"""
        WITH HourlyActivity AS (
            SELECT 
                DATEPART(HOUR, t.created_at) as hour_of_day,
                ts.is_airline_tweet,
                COUNT(*) as tweet_count
            FROM tweet t
            JOIN tweet_sentiment ts ON t.id = ts.tweet_id
            {where_clause}
            GROUP BY DATEPART(HOUR, t.created_at), ts.is_airline_tweet
        )
        SELECT 
            COALESCE(h1.hour_of_day, h2.hour_of_day) as hour_of_day,
            COALESCE(h1.tweet_count, 0) as user_tweets,
            COALESCE(h2.tweet_count, 0) as airline_tweets
        FROM 
            (SELECT hour_of_day, tweet_count FROM HourlyActivity WHERE is_airline_tweet = 0) h1
            FULL OUTER JOIN 
            (SELECT hour_of_day, tweet_count FROM HourlyActivity WHERE is_airline_tweet = 1) h2
            ON h1.hour_of_day = h2.hour_of_day
        ORDER BY hour_of_day
    """

    df = pd.read_sql(query, conn, params=params)
    return df

def get_hourly_user_airline_activity(conn, airline_id, start_date=None, end_date=None):
    """
    Returns two arrays: user_tweet_counts, airline_tweet_counts for each hour (0-23).
    Optionally filters by tweet.created_at between start_date and end_date.
    """
    import pandas as pd
    query = """
        SELECT 
            DATEPART(HOUR, t.created_at) as hour,
            CASE WHEN t.user_id = ? THEN 1 ELSE 0 END as is_airline
        FROM tweet t
        JOIN conversation_tweet ct ON t.id = ct.tweet_id
        JOIN conversation c ON ct.conversation_id = c.id
        WHERE c.airline_id = ?
    """
    params = [airline_id, airline_id]
    if start_date and end_date:
        query += " AND t.created_at BETWEEN ? AND ?"
        params.extend([start_date, end_date])
    df = pd.read_sql(query, conn, params=params)
    hourly = df.groupby(['hour', 'is_airline']).size().unstack(fill_value=0)
    for col in [0, 1]:
        if col not in hourly.columns:
            hourly[col] = 0
    hourly = hourly.reindex(range(24), fill_value=0)
    return hourly[0].values, hourly[1].values  # user, airline

#poster getters
def get_american_air_sentiment_flow(conn, start_date=None, end_date=None):
    """
    Returns a DataFrame with initial and final sentiment for American Airlines conversations,
    optionally filtered by tweet.created_at between start_date and end_date.
    """
    import pandas as pd
    query = """
    SELECT 
        cs.initial_sentiment,
        cs.final_sentiment,
        u.screen_name as airline_name
    FROM conversation_sentiment cs
    JOIN conversation c ON cs.conversation_id = c.id
    JOIN [user] u ON c.airline_id = u.id
    JOIN tweet t ON c.root_tweet_id = t.id
    WHERE u.screen_name = 'AmericanAir'
      AND cs.initial_sentiment IS NOT NULL 
      AND cs.final_sentiment IS NOT NULL
    """
    params = []
    if start_date and end_date:
        query += " AND t.created_at BETWEEN ? AND ?"
        params.extend([start_date, end_date])
    df = pd.read_sql(query, conn, params=params)
    return df

def get_airline_sentiment_data(conn, airline_name, start_date=None, end_date=None):
    """
    Returns a DataFrame with initial and final sentiment for the given airline,
    optionally filtered by tweet.created_at between start_date and end_date.
    """
    import pandas as pd
    query = """
    SELECT 
        cs.initial_sentiment,
        cs.final_sentiment,
        u.screen_name as airline_name
    FROM conversation_sentiment cs
    JOIN conversation c ON cs.conversation_id = c.id
    JOIN [user] u ON c.airline_id = u.id
    JOIN tweet t ON c.root_tweet_id = t.id
    WHERE u.screen_name = ?
      AND cs.initial_sentiment IS NOT NULL 
      AND cs.final_sentiment IS NOT NULL
    """
    params = [airline_name]
    if start_date and end_date:
        query += " AND t.created_at BETWEEN ? AND ?"
        params.extend([start_date, end_date])
    df = pd.read_sql(query, conn, params=params)
    return df

def fetch_sentiment_by_category_airline(start_date=None, end_date=None):
    conn = get_connection()
    query = """
        SELECT 
            di.issue_type,
            cs.sentiment_change,
            u.screen_name as airline_name,
            t.created_at
        FROM detected_issues di
        JOIN conversation_sentiment cs ON di.conversation_id = cs.conversation_id
        JOIN conversation c ON di.conversation_id = c.id
        JOIN [user] u ON c.airline_id = u.id
        JOIN tweet t ON c.root_tweet_id = t.id
        WHERE cs.sentiment_change IS NOT NULL
    """
    params = []
    if start_date and end_date:
        query += " AND t.created_at BETWEEN ? AND ?"
        params = [start_date, end_date]
    df = pd.read_sql(query, conn, params=params)
    conn.close()
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
