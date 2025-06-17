import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from db_repository import get_connection

# Connect to database
conn = get_connection()

# Query to get hourly activity for both users and airlines
query = """
WITH HourlyActivity AS (
    SELECT 
        DATEPART(HOUR, t.created_at) as hour_of_day,
        ts.is_airline_tweet,
        COUNT(*) as tweet_count
    FROM tweet t
    JOIN tweet_sentiment ts ON t.id = ts.tweet_id
    GROUP BY DATEPART(HOUR, t.created_at), ts.is_airline_tweet
)
SELECT 
    h1.hour_of_day,
    COALESCE(h1.tweet_count, 0) as user_tweets,
    COALESCE(h2.tweet_count, 0) as airline_tweets
FROM 
    (SELECT hour_of_day, tweet_count FROM HourlyActivity WHERE is_airline_tweet = 0) h1
    FULL OUTER JOIN 
    (SELECT hour_of_day, tweet_count FROM HourlyActivity WHERE is_airline_tweet = 1) h2
    ON h1.hour_of_day = h2.hour_of_day
ORDER BY hour_of_day
"""

# Get the data
df = pd.read_sql(query, conn)

# Calculate correlation
correlation = df['user_tweets'].corr(df['airline_tweets'])

# Create figure with two subplots
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

# Plot 1: Line plot of activity over hours
df_melted = df.melt(id_vars=['hour_of_day'], 
                    value_vars=['user_tweets', 'airline_tweets'],
                    var_name='Tweet Type', 
                    value_name='Count')

sns.lineplot(data=df_melted, x='hour_of_day', y='Count', 
            hue='Tweet Type', marker='o', ax=ax1)
ax1.set_title('Tweet Activity by Hour of Day')
ax1.set_xlabel('Hour of Day (24-hour format)')
ax1.set_ylabel('Number of Tweets')
ax1.set_xticks(range(0, 24))
ax1.grid(True, alpha=0.3)

# Plot 2: Scatter plot with regression line
sns.regplot(data=df, x='user_tweets', y='airline_tweets', ax=ax2)
ax2.set_title(f'Correlation between User and Airline Activity\nPearson Correlation: {correlation:.3f}')
ax2.set_xlabel('Number of User Tweets')
ax2.set_ylabel('Number of Airline Tweets')
ax2.grid(True, alpha=0.3)

# Adjust layout
plt.tight_layout()

# Print summary statistics
print("\nHourly Activity Summary:")
print(df.describe())
print(f"\nCorrelation coefficient between user and airline activity: {correlation:.3f}")

# Find peak hours
df['total_tweets'] = df['user_tweets'] + df['airline_tweets']
peak_hours = df.nlargest(3, 'total_tweets')
print("\nPeak Activity Hours:")
for _, row in peak_hours.iterrows():
    print(f"Hour {row['hour_of_day']:02d}:00 - Total tweets: {row['total_tweets']} "
          f"(Users: {row['user_tweets']}, Airlines: {row['airline_tweets']})")

plt.show()
