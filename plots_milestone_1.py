import matplotlib.pyplot as plt
from db_repository import (
    get_connection,get_airline_id,
    get_tweet_count, get_tweet_size,
    get_airline_mentions,
    get_conversation_count_by_airline, 
    get_language_counts,
    get_tweet_volume_over_time
)
from demo_util import save_plot

conn = get_connection()

def plot_effect_on_data():
    # Your provided values
    json_data = [6094135, 36.048, 849013, 94750]
    labels = [
        "Unique tweets",
        "Data Size (GB)",
        "Mentions of Airline",
        "Conversations"
    ]
    
    # Get American Airlines airline_id
    aa_id = get_airline_id(conn, 'AmericanAir')

    # Unique tweets
    unique_tweets = get_tweet_count(conn)

    # Size of database in GB
    db_size_gb = get_tweet_size(conn)

    # Mentions of airlines (for American Airlines only)
    mentions = get_airline_mentions(conn, aa_id)

    # Conversations (for American Airlines only)
    conversations = get_conversation_count_by_airline(conn, aa_id)

    db_data = [unique_tweets, db_size_gb, mentions, conversations]

    # Plotting
    fig, axes = plt.subplots(1, 4, figsize=(18, 5))
    bar_colors = ['#3498db', '#e67e22']

    for i, ax in enumerate(axes):
        ax.bar(['JSON', 'Database'], [json_data[i], db_data[i]], color=bar_colors)
        ax.set_title(labels[i])
        # Add value labels
        for j, val in enumerate([json_data[i], db_data[i]]):
            ax.text(j, val, f'{val:,.0f}' if val > 100 else f'{val:.2f}',
                    ha='center', va='bottom', fontsize=10, fontweight='bold')
        ax.set_ylabel('Count' if i != 1 else 'GB')
        ax.set_ylim(0, max(json_data[i], db_data[i]) * 1.15)

    plt.suptitle('Data Overview: JSON vs Database', fontsize=16)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    save_plot(fig, "data_overview_json_vs_db")

def plot_top_10_languages():
    rows = get_language_counts(conn)
    
    languages = [row[0] for row in rows]
    counts = [row[1] for row in rows]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(languages, counts, color='#3498db', edgecolor='#2980b9')
    ax.set_xlabel('Language')
    ax.set_ylabel('Number of Tweets')
    ax.set_title('Top 10 Languages used in Tweets')
    ax.bar_label(bars, padding=3)
    plt.tight_layout()
    save_plot(fig, "top_10_languages")

    
def plot_conversation_count_per_airline():
    airlines = ['easyJet', 'British_Airways', 'AmericanAir', 'Qantas', 'RyanAir', 'VirginAtlantic', 'KLM', 'SingaporeAir', 'Lufthansa', 'EtihadAirways', 'AirFrance']
    conversation_counts = [get_conversation_count_by_airline(conn, get_airline_id(conn, airline)) for airline in airlines]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(airlines, conversation_counts, color='#3498db', edgecolor='#2980b9')
    ax.set_xlabel('Airline')
    ax.set_ylabel('Number of Conversations')
    ax.set_title('Conversation Counts per Airline')
    ax.bar_label(bars, padding=3)
    plt.tight_layout()
    save_plot(fig, "conversation_count_per_airline")
    
def plot_tweet_volume_over_time():
    df = get_tweet_volume_over_time(conn)
    plt.figure(figsize=(14, 6))
    plt.plot(df['date'], df['tweet_count'], color='#2980b9')
    plt.xlabel('Date')
    plt.ylabel('Number of Tweets')
    plt.title('Tweet Volume Over Time')
    plt.tight_layout()
    save_plot(plt.gcf(), "tweet_volume_over_time")
