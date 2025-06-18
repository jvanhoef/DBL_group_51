import matplotlib.pyplot as plt
import numpy as np
from db_repository import (
    get_connection,
    get_airline_id,
    get_conversation_improvement_counts,
    get_last_user_sentiment_counts,
    get_response_time_buckets,
    get_hourly_user_airline_activity,
    get_issue_type_count
)
from demo_util import save_plot

conn = get_connection()


def plot_conversation_donuts():

    # Get data from the repository functions, passing dates if provided
    improvement_counts = get_conversation_improvement_counts(conn)
    sentiment_counts = get_last_user_sentiment_counts(conn)

    # --- Improvement Donut ---
    # Ensure all expected categories are present and in order
    improvement_order = ['improved', 'unchanged', 'worsened']
    improvement_labels = {'improved': 'Improved', 'unchanged': 'Unchanged', 'worsened': 'Worsened'}
    improvement_colors = ['#2ecc71', '#f1c40f', '#e74c3c']

    improvement_dict = {str(row[0]).lower(): row[1] for row in improvement_counts if row[0] is not None}
    sizes1 = [improvement_dict.get(cat, 0) for cat in improvement_order]
    labels1 = [improvement_labels[cat] for cat in improvement_order]

    # --- Final Sentiment Donut ---
    # You may want to define the expected sentiment values, e.g. -1, 0, 1 or similar
    sentiment_dict = {}
    for row in sentiment_counts:
        key = str(row[0]) if row[0] is not None else "Unknown"
        sentiment_dict[key] = row[1]
    # Sort by key for consistent order
    sentiment_keys = sorted(sentiment_dict.keys(), key=lambda x: (x == "Unknown", x))
    sizes2 = [sentiment_dict[k] for k in sentiment_keys]
    labels2 = [k.capitalize() for k in sentiment_keys]

    # --- Plotting ---
    fig, axs = plt.subplots(1, 2, figsize=(14, 7))

    # Donut 1: Improvement
    wedges1, texts1, autotexts1 = axs[0].pie(
        sizes1, labels=labels1, autopct='%1.1f%%', startangle=90, pctdistance=0.85, colors=improvement_colors
    )
    centre_circle1 = plt.Circle((0, 0), 0.70, fc='white')
    axs[0].add_artist(centre_circle1)
    axs[0].set_title("Conversation Improvement")

    # Donut 2: Final User Sentiment
    wedges2, texts2, autotexts2 = axs[1].pie(
        sizes2, labels=labels2, autopct='%1.1f%%', startangle=90, pctdistance=0.85
    )
    centre_circle2 = plt.Circle((0, 0), 0.70, fc='white')
    axs[1].add_artist(centre_circle2)
    axs[1].set_title("Final User Sentiment")

    plt.suptitle("Conversation Outcomes", fontsize=16)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    save_plot(fig, "conversation_outcomes")

def plot_response_time_donut():
    airline_id = get_airline_id(conn, 'AmericanAir')
    response_time_counts = get_response_time_buckets(conn, airline_id)

    # Ensure all categories are present and in order
    bucket_order = ['Within 30 min', '30-60 min', '60-120 min', 'Above 120 min', 'Unknown']
    bucket_colors = ['#27ae60', '#f1c40f', '#e67e22', '#e74c3c', '#95a5a6']

    counts_dict = {row[0]: row[1] for row in response_time_counts if row[0] is not None}
    sizes = [counts_dict.get(cat, 0) for cat in bucket_order]
    labels = [cat for cat in bucket_order if counts_dict.get(cat, 0) > 0]
    sizes = [counts_dict.get(cat, 0) for cat in bucket_order if counts_dict.get(cat, 0) > 0]
    colors = [bucket_colors[i] for i, cat in enumerate(bucket_order) if counts_dict.get(cat, 0) > 0]

    fig, ax = plt.subplots(figsize=(7, 7))
    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels, autopct='%1.1f%%', startangle=90, pctdistance=0.85, colors=colors
    )
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    ax.add_artist(centre_circle)
    ax.set_title("First Response Time Distribution")
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    save_plot(fig, "response_time_donut")

def plot_issue_type_counts():
    airline_id = get_airline_id(conn, 'AmericanAir')

    rows = get_issue_type_count(conn, airline_id)
    
    issue_types = [row[0] for row in rows]
    counts = [row[1] for row in rows]

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(issue_types, counts, color='#3498db', edgecolor='#2980b9')
    ax.set_xlabel('Issue Type')
    ax.set_ylabel('Number of Issues')
    ax.set_title('Detected Issues by Type')
    ax.bar_label(bars, padding=3)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    from demo_util import save_plot
    save_plot(fig, "issue_type_counts")
    
def plot_hourly_activity_american_air():
    conn = get_connection()
    airline_name = "AmericanAir"
    airline_id = get_airline_id(conn, airline_name)
    if not airline_id:
        print(f"Airline '{airline_name}' not found in the database.")
        conn.close()
        return

    user_activity, airline_activity = get_hourly_user_airline_activity(conn, airline_id)
    conn.close()

    hours = np.arange(24)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(hours, user_activity, s=60, color='royalblue', label='User Activity')
    ax.scatter(hours, airline_activity, s=60, color='tomato', label='Airline Activity')
    ax.set_title(f'Hourly Tweet Activity for {airline_name}', fontsize=14)
    ax.legend()
    ax.set_xlabel('Hour of the day')
    ax.set_ylabel('Tweets per hour')
    ax.set_xticks(hours)
    ax.set_xlim(-0.5, 23.5)
    ax.set_ylim(0, max(user_activity.max(), airline_activity.max()) * 1.1 if (user_activity.max() or airline_activity.max()) else 1)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    save_plot(fig, f"hourly_activity_{airline_name}")
    plt.close(fig)
    print(f"Hourly activity plot saved as 'plots/hourly_activity_{airline_name}.png'.")
    

# Call the function to generate and save the plot
plot_hourly_activity_american_air()