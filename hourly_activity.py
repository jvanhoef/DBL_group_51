import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from db_repository import get_connection
from conversations_db import KNOWN_AIRLINES
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def select_airline():
    print("\nAvailable airlines:")
    airlines = sorted(KNOWN_AIRLINES.keys())
    for i, airline in enumerate(airlines, 1):
        print(f"{i}. {airline}")
    print("0. All Airlines (Combined Analysis)")
    while True:
        try:
            choice = int(input("\nSelect airline number (0-{}): ".format(len(airlines))))
            if 0 <= choice <= len(airlines):
                if choice == 0:
                    return None, "All Airlines"
                return KNOWN_AIRLINES[airlines[choice-1]], airlines[choice-1]
            print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def fetch_hourly_user_airline_activity(conn, airline_id):
    query = """
        SELECT 
            DATEPART(HOUR, t.created_at) as hour,
            CASE WHEN t.user_id = ? THEN 1 ELSE 0 END as is_airline
        FROM tweet t
        JOIN conversation_tweet ct ON t.id = ct.tweet_id
        JOIN conversation c ON ct.conversation_id = c.id
        WHERE c.airline_id = ?
    """
    df = pd.read_sql(query, conn, params=(airline_id, airline_id))
    hourly = df.groupby(['hour', 'is_airline']).size().unstack(fill_value=0)
    for col in [0, 1]:
        if col not in hourly.columns:
            hourly[col] = 0
    hourly = hourly.reindex(range(24), fill_value=0)
    return hourly[0].values, hourly[1].values  # user, airline

def main():
    try:
        conn = get_connection()
        airline_id, airline_name = select_airline()
        if not airline_id:
            print("Please select a specific airline (not 'All Airlines') for this plot.")
            return
        logger.info(f"Fetching hourly user vs. airline activity for {airline_name}...")
        user_activity, airline_activity = fetch_hourly_user_airline_activity(conn, airline_id)
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
        ax.set_ylim(0, max(user_activity.max(), airline_activity.max())*1.1)
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('hourly_activity.png', dpi=150)
        plt.show()
        logger.info("Hourly activity plot saved as 'hourly_activity.png'.")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main() 
