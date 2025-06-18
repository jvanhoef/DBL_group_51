import plotly.graph_objects as go
import os
import matplotlib.pyplot as plt
import pandas as pd
from demo_util import save_plot
from db_repository import get_connection, get_american_air_sentiment_flow, fetch_sentiment_by_category_airline

def categorize_sentiment(score):
    if score < 0:
        return 'Negative'
    elif score == 0:
        return 'Neutral'
    else:
        return 'Positive'

def plot_american_airlines_sentiment_sankey():
    conn = get_connection()
    df = get_american_air_sentiment_flow(conn)
    conn.close()

    if df.empty:
        print("No data available for American Airlines sentiment flow.")
        return

    df['initial_category'] = df['initial_sentiment'].apply(categorize_sentiment)
    df['final_category'] = df['final_sentiment'].apply(categorize_sentiment)
    total_conversations = len(df)
    categories = ['Negative', 'Neutral', 'Positive']

    initial_counts = df['initial_category'].value_counts()
    initial_percentages = (initial_counts / total_conversations * 100).round(1)
    final_counts = df['final_category'].value_counts()
    final_percentages = (final_counts / total_conversations * 100).round(1)

    initial_labels = [f"{cat} ({initial_percentages.get(cat, 0)}%)" for cat in categories]
    final_labels = [f"{cat} ({final_percentages.get(cat, 0)}%)" for cat in categories]
    nodes = initial_labels + final_labels

    links = []
    for initial in categories:
        initial_total = len(df[df['initial_category'] == initial])
        for final in categories:
            count = len(df[(df['initial_category'] == initial) & (df['final_category'] == final)])
            if count > 0:
                percentage = round((count / initial_total * 100), 1) if initial_total else 0
                links.append({
                    'source': categories.index(initial),
                    'target': categories.index(final) + 3,
                    'value': count,
                    'percentage': percentage
                })

    def sentiment_order(cat):
        return {'Negative': -1, 'Neutral': 0, 'Positive': 1}[cat]

    df['change_type'] = df.apply(
        lambda row: 'improved' if sentiment_order(row['final_category']) > sentiment_order(row['initial_category'])
        else ('worsened' if sentiment_order(row['final_category']) < sentiment_order(row['initial_category']) else 'unchanged'),
        axis=1
    )

    improved_count = (df['change_type'] == 'improved').sum()
    worsened_count = (df['change_type'] == 'worsened').sum()
    unchanged_count = (df['change_type'] == 'unchanged').sum()

    improved_pct = round(improved_count / total_conversations * 100, 1)
    worsened_pct = round(worsened_count / total_conversations * 100, 1)
    unchanged_pct = round(unchanged_count / total_conversations * 100, 1)

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=nodes,
            color=['#e74c3c', '#95a5a6', '#2ecc71'] * 2
        ),
        link=dict(
            source=[link['source'] for link in links],
            target=[link['target'] for link in links],
            value=[link['value'] for link in links],
            color=['rgba(231, 76, 60, 0.4)' if categories[link['source']] == 'Negative' else
                   'rgba(149, 165, 166, 0.4)' if categories[link['source']] == 'Neutral' else
                   'rgba(46, 204, 113, 0.4)' for link in links],
            customdata=[f"{link['percentage']}%" for link in links],
            hovertemplate='%{customdata} of %{source.label} conversations<br>' +
                         'ended as %{target.label}<extra></extra>'
        )
    )])

    subtitle = (f"Improved: {improved_pct}% ({improved_count}) | "
                f"Unchanged: {unchanged_pct}% ({unchanged_count}) | "
                f"Worsened: {worsened_pct}% ({worsened_count})")

    fig.update_layout(
        title=dict(
            text="Sentiment Flow in American Airlines Conversations<br>"
                 f"Total Conversations: {total_conversations}<br>"
                 f"<span style='font-size:16px'>{subtitle}</span>",
            x=0.5,
            xanchor='center'
        ),
        font_size=12,
        height=650,
        width=1000
    )

    output_path = os.path.join("plots", "american_airlines_sentiment_flow.html")
    fig.write_html(output_path)    
    print("Sankey diagram has been saved as 'american_airlines_sentiment_flow.html'")

def plot_sentiment_stacked_bars_by_category_airline(start_date=None, end_date=None):
    df = fetch_sentiment_by_category_airline(start_date, end_date)
    airlines = ['AmericanAir', 'lufthansa', 'KLM', 'British_Airways']
    categories = ['customer_service', 'luggage', 'delay']
    sentiment_types = ['improved', 'unchanged', 'worsened']
    colors = ['#2ecc71', '#95a5a6', '#e74c3c']

    os.makedirs("plots", exist_ok=True)

    for category in categories:
        plt.figure(figsize=(12, 6))
        cat_df = df[(df['issue_type'] == category) & (df['airline_name'].isin(airlines))]
        data = []
        totals = []
        for airline in airlines:
            airline_df = cat_df[cat_df['airline_name'] == airline]
            total = len(airline_df)
            totals.append(total)
            if total == 0:
                data.append([0, 0, 0])
            else:
                improved = (airline_df['sentiment_change'] == 'improved').sum() / total * 100
                unchanged = (airline_df['sentiment_change'] == 'unchanged').sum() / total * 100
                worsened = (airline_df['sentiment_change'] == 'worsened').sum() / total * 100
                data.append([improved, unchanged, worsened])
        data = pd.DataFrame(data, columns=sentiment_types, index=airlines)

        bottom = None
        for i, sentiment in enumerate(sentiment_types):
            bars = plt.bar(airlines, data[sentiment], bottom=bottom, color=colors[i], label=sentiment.capitalize())
            if bottom is None:
                bottom = data[sentiment]
            else:
                bottom += data[sentiment]
            for j, bar in enumerate(bars):
                height = bar.get_height()
                if height > 0:
                    y = bar.get_y() + height / 2
                    color = 'white' if sentiment != 'unchanged' else 'black'
                    plt.text(bar.get_x() + bar.get_width()/2, y, f'{height:.1f}%', ha='center', va='center', color=color, fontsize=9, fontweight='bold')
        for i, airline in enumerate(airlines):
            plt.text(i, 102, f'Total: {totals[i]}', ha='center', va='bottom', fontsize=9, fontweight='bold', color='black')
        plt.ylabel('Percentage of Conversations')
        plt.title(f'Sentiment Change Distribution by Airline\nCategory: {category}')
        plt.xticks(rotation=45, ha='right')
        plt.ylim(0, 110)
        plt.legend()
        plt.tight_layout()
        save_plot(plt, f"sentiment_stacked_bars_{category}")
        plt.close()
