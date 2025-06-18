import pandas as pd
import matplotlib.pyplot as plt
from db_repository import get_connection

def fetch_sentiment_by_category_airline():
    conn = get_connection()
    query = """
        SELECT 
            di.issue_type,
            cs.sentiment_change,
            u.screen_name as airline_name
        FROM detected_issues di
        JOIN conversation_sentiment cs ON di.conversation_id = cs.conversation_id
        JOIN conversation c ON di.conversation_id = c.id
        JOIN [user] u ON c.airline_id = u.id
        WHERE cs.sentiment_change IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def plot_stacked_bars(df):
    # Restrict to these airlines and categories
    airlines = ['AmericanAir', 'lufthansa', 'KLM', 'British_Airways']
    categories = ['customer_service', 'luggage', 'delay']
    sentiment_types = ['improved', 'unchanged', 'worsened']
    colors = ['#2ecc71', '#95a5a6', '#e74c3c']

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

        # Plot stacked bar
        bottom = None
        for i, sentiment in enumerate(sentiment_types):
            bars = plt.bar(airlines, data[sentiment], bottom=bottom, color=colors[i], label=sentiment.capitalize())
            if bottom is None:
                bottom = data[sentiment]
            else:
                bottom += data[sentiment]
            # Add percentage labels inside each bar segment
            for j, bar in enumerate(bars):
                height = bar.get_height()
                if height > 0:
                    y = bar.get_y() + height / 2
                    color = 'white' if sentiment != 'unchanged' else 'black'
                    plt.text(bar.get_x() + bar.get_width()/2, y, f'{height:.1f}%', ha='center', va='center', color=color, fontsize=9, fontweight='bold')
        # Add total above each bar
        for i, airline in enumerate(airlines):
            plt.text(i, 102, f'Total: {totals[i]}', ha='center', va='bottom', fontsize=9, fontweight='bold', color='black')
        plt.ylabel('Percentage of Conversations')
        plt.title(f'Sentiment Change Distribution by Airline\nCategory: {category}')
        plt.xticks(rotation=45, ha='right')
        plt.ylim(0, 110)
        plt.legend()
        plt.tight_layout()
        plt.savefig(f'sentiment_stackedbar_{category.replace(" ", "_")}.png', dpi=200)
        plt.show()

if __name__ == "__main__":
    df = fetch_sentiment_by_category_airline()
    plot_stacked_bars(df) 
