import pandas as pd
import matplotlib.pyplot as plt
from db_repository import get_connection

def extract_conversations(filename):
    """Extract conversations from text file"""
    conversations = []
    current_conversation = []
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            if "--- Conversation" in line:
                if current_conversation:
                    conversations.append(current_conversation)
                current_conversation = []
            elif line.strip() and "filepowath:" not in line:
                current_conversation.append(line.strip())
                
    if current_conversation:
        conversations.append(current_conversation)
        
    return conversations

def get_available_airlines():
    """Get list of unique airlines from database"""
    conn = get_connection()
    query = """
    SELECT DISTINCT u.screen_name as airline
    FROM conversation c
    JOIN conversation_tweet ct ON c.id = ct.conversation_id 
    JOIN [user] u ON c.airline_id = u.id
    WHERE ct.language IS NOT NULL
    ORDER BY u.screen_name
    """
    return pd.read_sql(query, conn)['airline'].tolist()

def categorize_language(lang):
    """Categorize language code into English/Other/Unknown"""
    try:
        return 'English' if lang.lower() == 'en' else 'Other'
    except:
        return 'Unknown'

def get_conversation_languages(selected_airlines=None):
    """Get conversation language data from database"""
    try:
        print("Connecting to database...")
        conn = get_connection()
        print("Connected successfully")
        
        print("Fetching conversations from database...")
        
        # Build query with airline filter if provided
        airline_filter = ""
        if selected_airlines:
            airlines_str = "','".join(selected_airlines)
            airline_filter = f"AND u.screen_name IN ('{airlines_str}')"
        
        # Updated query to use table names and include language
        query = f"""
        SELECT 
            u.screen_name as airline,
            t.language
        FROM dbo.conversation c
        JOIN dbo.conversation_tweet ct ON c.id = ct.conversation_id 
        JOIN dbo.tweet t ON ct.tweet_id = t.id
        JOIN dbo.[user] u ON c.airline_id = u.id
        WHERE t.language IS NOT NULL {airline_filter}
        """
        
        print("Executing query...")
        # Get raw data
        df = pd.read_sql(query, conn)
        print(f"Retrieved {len(df)} rows from database")
        
        # Categorize languages
        print("Categorizing languages...")
        df['language_category'] = df['language'].apply(categorize_language)
        
        # Process data for each airline
        results = []
        for airline in df['airline'].unique():
            airline_data = df[df['airline'] == airline]
            total = len(airline_data)
            english = len(airline_data[airline_data['language_category'] == 'English'])
            other = len(airline_data[airline_data['language_category'] == 'Other'])
            unknown = len(airline_data[airline_data['language_category'] == 'Unknown'])
            
            results.append({
                'airline': airline,
                'total_conversations': total,
                'english_count': english,
                'other_count': other,
                'unknown_count': unknown,
                'english_pct': (english/total*100) if total > 0 else 0,
                'other_pct': (other/total*100) if total > 0 else 0,
                'unknown_pct': (unknown/total*100) if total > 0 else 0
            })
        
        return pd.DataFrame(results)
        
    except Exception as e:
        print(f"Error in get_conversation_languages: {str(e)}")
        raise
          print("Executing query...")
        # Get raw data
        df = pd.read_sql(query, conn)
        print(f"Retrieved {len(df)} rows from database")
        
        # Categorize languages
        print("Categorizing languages...")
        df['language_category'] = df['language'].apply(categorize_language)
        
        # Process data for each airline
        results = []
        for airline in df['airline'].unique():
            airline_data = df[df['airline'] == airline]
            total = len(airline_data)
            english = len(airline_data[airline_data['language_category'] == 'English'])
            other = len(airline_data[airline_data['language_category'] == 'Other'])
            unknown = len(airline_data[airline_data['language_category'] == 'Unknown'])
        
        results.append({
            'airline': airline,
            'total_conversations': total,
            'english_count': english,
            'other_count': other,
            'unknown_count': unknown,
            'english_pct': (english/total*100) if total > 0 else 0,
            'other_pct': (other/total*100) if total > 0 else 0,
            'unknown_pct': (unknown/total*100) if total > 0 else 0
        })
    
    return pd.DataFrame(results)

def plot_language_distribution(df):
    """Create and save the visualization"""
    plt.figure(figsize=(14, 6))
    bar_width = 0.75
    index = range(len(df))

    # Create stacked percentage bars
    english_bars = plt.bar(index, df['english_pct'], bar_width, 
                          label='English', color='#3498db')  # Blue
    other_bars = plt.bar(index, df['other_pct'], bar_width,
                        bottom=df['english_pct'], label='Other Languages', color='#e67e22')  # Orange
    unknown_bars = plt.bar(index, df['unknown_pct'], bar_width,
                          bottom=df['english_pct'] + df['other_pct'], 
                          label='Unknown', color='#95a5a6')  # Gray

    plt.xlabel('Airline')
    plt.ylabel('Percentage of Conversations')
    plt.title('Language Distribution in Customer Service Conversations by Airline')
    plt.xticks(index, df['airline'], rotation=45, ha='right')

    # Add percentage labels inside the bars
    for i in index:
        english_pct = df.iloc[i]['english_pct']
        other_pct = df.iloc[i]['other_pct']
        unknown_pct = df.iloc[i]['unknown_pct']
        total = df.iloc[i]['total_conversations']
        
        # Show percentages if > 3%
        if english_pct > 3:
            plt.text(i, english_pct/2, f'{english_pct:.1f}%', 
                    ha='center', va='center', color='white')
        
        if other_pct > 3:
            plt.text(i, english_pct + other_pct/2, f'{other_pct:.1f}%', 
                    ha='center', va='center', color='white')
        
        if unknown_pct > 3:
            plt.text(i, english_pct + other_pct + unknown_pct/2, f'{unknown_pct:.1f}%', 
                    ha='center', va='center', color='white')
        
        # Add total number of conversations above the bars
        plt.text(i, 115, f'Total: {total:,}', 
                 ha='center', va='bottom', fontsize=9)

    plt.ylim(0, 125)  # Make room for totals
    plt.grid(True, alpha=0.3, color='#666666')
    plt.gca().set_facecolor('#f0f0f0')
    plt.gca().set_axisbelow(True)
    
    # Move legend outside of the plot
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout(rect=[0, 0, 0.9, 1])
    
    plt.savefig('language_distribution.png', bbox_inches='tight', dpi=300, facecolor='white')
    plt.show()

def print_statistics(df):
    """Print summary statistics"""
    print("\nLanguage Distribution Analysis by Airline")
    print("=" * 80)
    print("\nTotal conversations and language breakdown by airline:")
    for _, row in df.iterrows():
        print(f"\n{row['airline']}:")
        print(f"Total conversations: {row['total_conversations']:,}")
        print(f"English: {row['english_pct']:.1f}%")
        print(f"Other languages: {row['other_pct']:.1f}%")
        print(f"Unknown: {row['unknown_pct']:.1f}%")

def main():
    try:
        print("Starting language distribution analysis...")
        print("Getting list of airlines...")
        airlines = get_available_airlines()
        
        if not airlines:
            raise ValueError("No airlines found in the database")
            
        print(f"\nFound airlines: {', '.join(airlines)}")
        print("\nAnalyzing all airlines...")
        
        # Get and process data
        df = get_conversation_languages()
        
        if len(df) == 0:
            print("No data found")
            return
        
        # Create visualization and print statistics
        plot_language_distribution(df)
        print_statistics(df)
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
