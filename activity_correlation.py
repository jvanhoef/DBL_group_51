import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from db_repository import get_connection, get_activity_correlation
from demo_util import save_plot

def plot_activity_correlation():
    conn = get_connection()
    df = get_activity_correlation(conn)
    if df.empty:
        return

    # Calculate correlation
    correlation = df['user_tweets'].corr(df['airline_tweets'])

    # Set style
    plt.style.use('seaborn-v0_8-whitegrid')
    sns.set_context("paper", font_scale=1.2)

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), gridspec_kw={'height_ratios': [2, 1]})

    # Plot 1: Line plot of activity over hours
    df_melted = df.melt(id_vars=['hour_of_day'], 
                        value_vars=['user_tweets', 'airline_tweets'],
                        var_name='Tweet Type', 
                        value_name='Count')

    palette = {'user_tweets': '#3498db', 'airline_tweets': '#e67e22'}
    sns.lineplot(
        data=df_melted, x='hour_of_day', y='Count', 
        hue='Tweet Type', marker='o', ax=ax1, palette=palette
    )
    ax1.set_title('Tweet Activity by Hour of Day', fontsize=15, fontweight='bold')
    ax1.set_xlabel('Hour of Day (24-hour format)', fontsize=12)
    ax1.set_ylabel('Number of Tweets', fontsize=12)
    ax1.set_xticks(range(0, 24))
    ax1.legend(title='', fontsize=11)
    ax1.grid(True, alpha=0.3)

    # Plot 2: Scatter plot with regression line
    sns.regplot(
        data=df, x='user_tweets', y='airline_tweets', 
        ax=ax2, scatter_kws={'color': '#3498db', 's': 60, 'alpha': 0.7}, line_kws={'color': '#e67e22'}
    )
    ax2.set_title(f'Correlation between User and Airline Activity\nPearson Correlation: {correlation:.3f}', fontsize=14)
    ax2.set_xlabel('Number of User Tweets', fontsize=12)
    ax2.set_ylabel('Number of Airline Tweets', fontsize=12)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.98])
    save_plot(fig, "activity_correlation")