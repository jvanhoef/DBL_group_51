import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from db_repository import get_connection

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

def plot_sentiment_data(df, issue_type):
    """Create and save the visualization"""
    plt.figure(figsize=(14, 6))  # Made wider to accommodate legend
    bar_width = 0.75
    index = range(len(df))

    # Create stacked percentage bars with same colors as sentiment_improvement_by_category
    improved_bars = plt.bar(index, df['improved_pct'], bar_width, 
                           label='Improved', color='#2ecc71')  # Green
    unchanged_bars = plt.bar(index, df['unchanged_pct'], bar_width,
                            bottom=df['improved_pct'], label='Unchanged', color='#95a5a6')  # Gray
    worsened_bars = plt.bar(index, df['worsened_pct'], bar_width,
                           bottom=df['improved_pct'] + df['unchanged_pct'], 
                           label='Worsened', color='#e74c3c')  # Red

    plt.xlabel('Airline')
    plt.ylabel('Percentage of Issues')
    plt.title(f'Sentiment Changes in {issue_type.capitalize()} Issues by Airline')
    plt.xticks(index, df['airline'], rotation=45, ha='right')
    plt.legend()    # Add percentage labels inside the bars
    for i in index:
        improved_pct = df.iloc[i]['improved_pct']
        unchanged_pct = df.iloc[i]['unchanged_pct']
        worsened_pct = df.iloc[i]['worsened_pct']
        total_issues = df.iloc[i]['total_issues']
        
        # Show all percentages
        plt.text(i, improved_pct/2, f'{improved_pct:.1f}%', 
                ha='center', va='center', color='white')
        
        plt.text(i, improved_pct + unchanged_pct/2, f'{unchanged_pct:.1f}%', 
                ha='center', va='center', color='black')
        
        plt.text(i, improved_pct + unchanged_pct + worsened_pct/2, f'{worsened_pct:.1f}%', 
                ha='center', va='center', color='white')
        
        # Add total number of issues above the bars
        plt.text(i, 115, f'Total: {total_issues:,}', 
                 ha='center', va='bottom', fontsize=9)

    plt.ylim(0, 125)  # Increased to make room for totals
    plt.grid(True, alpha=0.3, color='#666666')
    plt.gca().set_facecolor('#f0f0f0')
    plt.gca().set_axisbelow(True)
    
    # Move legend outside of the plot
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout(rect=[0, 0, 0.9, 1])  # Make room for legend
    
    # Save with category name in filename
    filename = f'{issue_type.lower()}_sentiment_analysis.png'
    plt.savefig(filename, bbox_inches='tight', dpi=300, facecolor='white')
    plt.show()

def print_statistics(df, issue_type):
    """Print summary statistics"""
    print(f"\n{issue_type.capitalize()} Issues Resolution Analysis by Airline")
    print("=" * 80)
    print(f"\nTotal {issue_type.lower()} issues by airline:")
    for _, row in df.iterrows():
        print(f"\n{row['airline']}:")
        print(f"Total issues: {row['total_issues']:,}")
        print(f"Improved: {row['improved_pct']:.1f}%")
        print(f"Unchanged: {row['unchanged_pct']:.1f}%")
        print(f"Worsened: {row['worsened_pct']:.1f}%")

def main():
    try:
        # Get available categories
        print("Getting list of issue categories...")
        categories = get_available_categories()
        
        if not categories:
            raise ValueError("No issue categories found in the database")
        
        # Show available categories
        print("\nAvailable issue categories:")
        for i, category in enumerate(categories, 1):
            print(f"{i}. {category}")
        
        # Get category choice
        while True:
            try:
                category_choice = input("\nSelect an issue category (enter the number): ")
                category_idx = int(category_choice) - 1
                if 0 <= category_idx < len(categories):
                    selected_category = categories[category_idx]
                    break
                else:
                    print("Invalid choice. Please enter a valid number.")
            except ValueError:
                print("Please enter a valid number.")
        
        # Get airlines for selected category
        print(f"\nGetting airlines with {selected_category} issues...")
        airlines = get_available_airlines(selected_category)
        
        if not airlines:
            raise ValueError(f"No airlines found with {selected_category} issues")
        
        # Show available airlines
        print("\nAvailable airlines:")
        for i, airline in enumerate(airlines, 1):
            print(f"{i}. {airline}")
        
        # Get airline choice
        print("\nEnter the numbers of the airlines you want to analyze (comma-separated)")
        print("Press Enter for all airlines, or type 'q' to quit")
        choice = input("Your choice: ")
        
        if choice.lower() == 'q':
            return
        
        selected_airlines = None
        if choice.strip():
            try:
                indices = [int(x.strip()) - 1 for x in choice.split(',')]
                selected_airlines = [airlines[i] for i in indices]
                print(f"\nAnalyzing airlines: {', '.join(selected_airlines)}")
            except (ValueError, IndexError):
                print("Invalid input. Please enter valid numbers separated by commas.")
                return
        
        # Get and process data
        df = get_sentiment_data(selected_category, selected_airlines)
        
        if len(df) == 0:
            print("No data found for the selected airlines")
            return
        
        # Create visualization and print statistics
        plot_sentiment_data(df, selected_category)
        print_statistics(df, selected_category)
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
