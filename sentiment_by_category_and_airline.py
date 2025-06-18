import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
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

def get_sentiment_data(issue_types, selected_airlines=None):
    """Get sentiment data for specified issue types and airlines"""
    conn = get_connection()
    
    airline_filter = ""
    if selected_airlines:
        airlines_str = "','".join(selected_airlines)
        airline_filter = f"AND u.screen_name IN ('{airlines_str}')"
    
    # Convert single category to list for consistency
    if isinstance(issue_types, str):
        issue_types = [issue_types]
        
    issue_types_str = "','".join(issue_types)
    query = f"""
    SELECT 
        u.screen_name as airline,
        di.issue_type,
        cs.sentiment_change
    FROM detected_issues di
    JOIN conversation c ON di.conversation_id = c.id
    JOIN [user] u ON c.airline_id = u.id
    JOIN conversation_sentiment cs ON di.conversation_id = cs.conversation_id
    WHERE di.issue_type IN ('{issue_types_str}')
        AND cs.sentiment_change IS NOT NULL
        {airline_filter}
    """
    
    # Get raw data
    df = pd.read_sql(query, conn)
      # Process data for each airline and category
    results = []
    for airline in df['airline'].unique():
        for issue_type in df['issue_type'].unique():
            airline_category_data = df[(df['airline'] == airline) & 
                                    (df['issue_type'] == issue_type)]['sentiment_change']
            
            improved = len(airline_category_data[airline_category_data == 'improved'])
            worsened = len(airline_category_data[airline_category_data == 'worsened'])
            unchanged = len(airline_category_data[airline_category_data == 'unchanged'])
            total = improved + unchanged + worsened
            
            # Calculate percentages
            if total > 0:
                results.append({
                    'airline': airline,
                    'issue_type': issue_type,
                    'total_issues': total,
                    'improved_count': improved,                    'worsened_count': worsened,
                    'unchanged_count': unchanged,
                    'improved_pct': (improved/total*100),
                    'worsened_pct': (worsened/total*100),
                    'unchanged_pct': (unchanged/total*100)
                })
    
    return pd.DataFrame(results)

def plot_sentiment_data(df, selected_categories):
    """Create and save the visualization with multiple categories"""
    plt.figure(figsize=(20, 12))  # Made even wider and taller for better readability
      # Calculate bar positions
    n_categories = len(selected_categories)
    n_airlines = len(df['airline'].unique())
    bar_width = 0.6  # Make bars wider
    category_spacing = 3  # Increase space between category groups
    
    # Create array for category positions (these will be the centers of each group)
    category_positions = np.arange(n_categories) * (n_airlines + category_spacing)
    
    # Get unique airlines and sort them
    unique_airlines = sorted(df['airline'].unique())
    
    # Store bar objects for legend
    improved_bars, unchanged_bars, worsened_bars = None, None, None
    
    # Create bars for each category
    for i, category in enumerate(selected_categories):
        category_data = df[df['issue_type'] == category].copy()
        category_data = category_data.sort_values('airline')  # Sort airlines alphabetically
        
        # Calculate x positions for this category's bars
        x_positions = np.arange(len(unique_airlines)) + category_positions[i]
        
        # Create stacked bars for this category
        improved = plt.bar(x_positions, category_data['improved_pct'], 
                         bar_width, label='Improved' if i == 0 else "", 
                         color='#2ecc71', alpha=0.9)  # Green
        
        unchanged = plt.bar(x_positions, category_data['unchanged_pct'],
                          bar_width, bottom=category_data['improved_pct'], 
                          label='Unchanged' if i == 0 else "",
                          color='#95a5a6', alpha=0.9)  # Gray
        
        worsened = plt.bar(x_positions, category_data['worsened_pct'],
                          bar_width, bottom=category_data['improved_pct'] + category_data['unchanged_pct'],
                          label='Worsened' if i == 0 else "",
                          color='#e74c3c', alpha=0.9)  # Red
        
        # Add black edges to bars
        for bar in improved + unchanged + worsened:
            bar.set_edgecolor('black')
            bar.set_linewidth(1)
            
        # Save first set of bars for legend
        if i == 0:
            improved_bars = improved
            unchanged_bars = unchanged
            worsened_bars = worsened
        
        # Add percentage labels and airline names
        for j, row in category_data.iterrows():
            x_pos = x_positions[category_data.index.get_loc(j)]
            
            # Show percentages if > 3%
            if row['improved_pct'] > 3:
                plt.text(x_pos, row['improved_pct']/2, f'{row["improved_pct"]:.0f}%', 
                        ha='center', va='center', color='white', fontsize=8, fontweight='bold')
            
            if row['unchanged_pct'] > 3:
                plt.text(x_pos, row['improved_pct'] + row['unchanged_pct']/2, 
                        f'{row["unchanged_pct"]:.0f}%', 
                        ha='center', va='center', color='black', fontsize=8, fontweight='bold')
            
            if row['worsened_pct'] > 3:
                plt.text(x_pos, row['improved_pct'] + row['unchanged_pct'] + row['worsened_pct']/2, 
                        f'{row["worsened_pct"]:.0f}%', 
                        ha='center', va='center', color='white', fontsize=8, fontweight='bold')
            
            # Add airline name below the bar
            plt.text(x_pos, -5, row['airline'], 
                    ha='right', va='top', fontsize=8, rotation=45)
            
            # Add total number of issues above the bars
            plt.text(x_pos, 105, f'Total:\n{row["total_issues"]:,}', 
                     ha='center', va='bottom', fontsize=8)
        
        # Add category label centered under each group
        group_center = np.mean(x_positions)
        plt.text(group_center, -15, category, 
                ha='center', va='top', fontsize=10, fontweight='bold')
        
        # Add vertical separator line after each category except the last
        if i < len(selected_categories) - 1:
            sep_x = (category_positions[i] + category_positions[i+1]) / 2
            plt.axvline(x=sep_x, color='black', linestyle='--', alpha=0.3)
    
    # Customize plot
    plt.xlabel('')  # Remove x-label since we have category labels
    plt.ylabel('Percentage of Issues')
    plt.title('Sentiment Changes by Category and Airline', pad=15)
    
    # Remove x-ticks since we have custom labels
    plt.xticks([])
    
    # Adjust y-limit to make room for labels
    plt.ylim(-20, 125)
    
    # Add grid and styling
    plt.grid(True, axis='y', alpha=0.3, color='#666666')
    plt.gca().set_facecolor('#f0f0f0')
    plt.gca().set_axisbelow(True)
    
    # Create a custom legend with sentiment categories only
    plt.legend([improved_bars, unchanged_bars, worsened_bars],
              ['Improved', 'Unchanged', 'Worsened'],
              bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout(rect=[0, 0, 0.85, 1])
    
    # Save with category names in filename
    filename = f'{"_".join(cat.lower() for cat in selected_categories)}_sentiment_analysis.png'
    plt.savefig(filename, bbox_inches='tight', dpi=300, facecolor='white')
    plt.show()

def print_statistics(df, categories_str):
    """Print summary statistics for multiple categories"""
    print(f"\nSentiment Analysis by Airline and Category")
    print("=" * 80)
    
    for airline in df['airline'].unique():
        print(f"\n{airline}:")
        airline_data = df[df['airline'] == airline]
        for _, row in airline_data.iterrows():
            print(f"\n  {row['issue_type']}:")
            print(f"  Total issues: {row['total_issues']:,}")
            print(f"  Improved: {row['improved_pct']:.1f}%")
            print(f"  Unchanged: {row['unchanged_pct']:.1f}%")
            print(f"  Worsened: {row['worsened_pct']:.1f}%")

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
        
        # Get multiple category choices
        print("\nEnter the numbers of the categories you want to analyze (comma-separated)")
        print("Example: 1,2,3 for first three categories")
        while True:
            try:
                category_choice = input("Your choices: ")
                category_indices = [int(x.strip()) - 1 for x in category_choice.split(',')]
                
                # Validate all indices
                if all(0 <= idx < len(categories) for idx in category_indices):
                    selected_categories = [categories[idx] for idx in category_indices]
                    break
                else:
                    print("Invalid choice(s). Please enter valid numbers.")
            except ValueError:
                print("Please enter valid numbers separated by commas.")
        
        # Get airlines for all selected categories
        print(f"\nGetting airlines with issues in selected categories...")
        all_airlines = set()
        for category in selected_categories:
            category_airlines = get_available_airlines(category)
            all_airlines.update(category_airlines)
        
        all_airlines = sorted(list(all_airlines))
        if not all_airlines:
            raise ValueError(f"No airlines found with issues in the selected categories")
        
        # Show available airlines
        print("\nAvailable airlines:")
        for i, airline in enumerate(all_airlines, 1):
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
                selected_airlines = [all_airlines[i] for i in indices]
                print(f"\nAnalyzing airlines: {', '.join(selected_airlines)}")
            except (ValueError, IndexError):
                print("Invalid input. Please enter valid numbers separated by commas.")
                return
        
        # Get and process data
        print("\nRetrieving and analyzing data...")
        df = get_sentiment_data(selected_categories, selected_airlines)
        
        if len(df) == 0:
            print("No data found for the selected airlines and categories")
            return
        
        # Create visualization and print statistics
        categories_str = ' & '.join(selected_categories)
        plot_sentiment_data(df, selected_categories)
        print_statistics(df, categories_str)
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
