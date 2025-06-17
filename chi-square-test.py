import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency
from db_repository import get_connection
import matplotlib.pyplot as plt
import seaborn as sns
import traceback

def get_sentiment_data():
    """Get sentiment data from database"""
    try:
        print("Attempting to connect to database...")
        conn = get_connection()
        
        query = """
        SELECT 
            di.issue_type,
            cs.sentiment_change,
            c.airline_id,
            u.screen_name as airline_name
        FROM detected_issues di
        JOIN conversation_sentiment cs ON di.conversation_id = cs.conversation_id
        JOIN conversation c ON di.conversation_id = c.id
        JOIN [user] u ON c.airline_id = u.id
        WHERE cs.sentiment_change IS NOT NULL
        """
        
        print("Executing query...")
        df = pd.read_sql(query, conn)
        print(f"Query successful. Retrieved {len(df)} rows.")
        print(f"Unique airlines found: {df['airline_name'].unique()}")
        print(f"Unique categories found: {df['issue_type'].unique()}")
        
        conn.close()
        return df
    except Exception as e:
        print("Error occurred while fetching data:")
        print(traceback.format_exc())
        raise

def compare_airlines(df, airline1, airline2, category):
    """Compare sentiment changes between two airlines for a specific category"""
    try:
        # Filter data for the two airlines and category
        mask = (df['airline_name'].isin([airline1, airline2])) & (df['issue_type'] == category)
        comparison_data = df[mask]
        
        if len(comparison_data) == 0:
            print(f"Warning: No data found for {airline1} vs {airline2} in category {category}")
            return None
        
        # Create contingency table
        contingency = pd.crosstab(
            comparison_data['airline_name'],
            comparison_data['sentiment_change']
        )
        
        # Perform chi-square test
        chi2, p_value, dof, expected = chi2_contingency(contingency)
        
        # Calculate percentages
        results = []
        for airline in [airline1, airline2]:
            airline_data = comparison_data[comparison_data['airline_name'] == airline]
            total = len(airline_data)
            
            improved = len(airline_data[airline_data['sentiment_change'] == 'improved'])
            unchanged = len(airline_data[airline_data['sentiment_change'] == 'unchanged'])
            worsened = len(airline_data[airline_data['sentiment_change'] == 'worsened'])
            
            results.append({
                'airline': airline,
                'total': total,
                'improved_pct': (improved/total)*100 if total > 0 else 0,
                'unchanged_pct': (unchanged/total)*100 if total > 0 else 0,
                'worsened_pct': (worsened/total)*100 if total > 0 else 0
            })
        
        return {
            'contingency': contingency,
            'p_value': p_value,
            'chi2': chi2,
            'dof': dof,
            'results': pd.DataFrame(results)
        }
    except Exception as e:
        print(f"Error in compare_airlines for {airline1} vs {airline2}, category {category}:")
        print(traceback.format_exc())
        return None

def plot_airline_comparison(df, airline1, airline2, category, comparison_results):
    """Create visualization for airline comparison"""
    try:
        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Plot 1: Stacked bar chart of sentiment changes
        comparison_data = df[
            (df['airline_name'].isin([airline1, airline2])) & 
            (df['issue_type'] == category)
        ]
        
        pivot_data = pd.crosstab(
            comparison_data['airline_name'],
            comparison_data['sentiment_change'],
            normalize='index'
        ) * 100
        
        pivot_data.plot(kind='bar', stacked=True, ax=ax1)
        ax1.set_title(f'Sentiment Changes: {airline1} vs {airline2}\n{category}')
        ax1.set_ylabel('Percentage')
        ax1.set_xlabel('Airline')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # Plot 2: Heatmap of contingency table
        sns.heatmap(comparison_results['contingency'], annot=True, fmt='d', cmap='YlOrRd', ax=ax2)
        ax2.set_title('Contingency Table')
        
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"Error in plot_airline_comparison for {airline1} vs {airline2}, category {category}:")
        print(traceback.format_exc())

def main():
    try:
        # Get data
        print("Fetching data from database...")
        df = get_sentiment_data()
        
        if df is None or len(df) == 0:
            print("No data retrieved from database. Exiting.")
            return
        
        # Get unique categories and airlines
        categories = df['issue_type'].unique()
        airlines = df['airline_name'].unique()
        
        print(f"\nFound {len(categories)} categories and {len(airlines)} airlines")
        
        # Focus on American Airlines
        american_airlines = 'AmericanAir'
        
        if american_airlines not in airlines:
            print(f"Error: {american_airlines} not found in the data")
            return
        
        # List available airlines to compare
        print("\nAvailable airlines to compare with AmericanAir:")
        available_airlines = [a for a in airlines if a != american_airlines]
        for idx, airline in enumerate(available_airlines):
            print(f"{idx+1}. {airline}")
        
        # Prompt user to select an airline
        while True:
            try:
                selection = int(input("\nEnter the number of the airline you want to compare with AmericanAir: "))
                if 1 <= selection <= len(available_airlines):
                    selected_airline = available_airlines[selection-1]
                    break
                else:
                    print("Invalid selection. Please enter a valid number.")
            except ValueError:
                print("Invalid input. Please enter a number.")
        
        print(f"\nYou selected: {selected_airline}")
        
        # Compare American Airlines with the selected airline for each category
        significant_categories = []
        non_significant_categories = []
        
        for category in categories:
            print(f"\n{'='*80}")
            print(f"Analyzing category: {category}")
            print(f"{'='*80}")
            
            print(f"\nComparing {american_airlines} with {selected_airline}:")
            
            # Perform comparison
            comparison = compare_airlines(df, american_airlines, selected_airline, category)
            
            if comparison is None:
                print(f"Skipping category {category} due to insufficient data")
                continue
            
            # Print statistical results
            print(f"\nChi-square test results:")
            print(f"Chi-square value: {comparison['chi2']:.2f}")
            print(f"p-value: {comparison['p_value']:.4f}")
            print(f"Degrees of freedom: {comparison['dof']}")
            
            if comparison['p_value'] < 0.05:
                print("\nThe difference in sentiment changes is statistically significant!")
                significant_categories.append(category)
            else:
                print("\nThe difference in sentiment changes is not statistically significant.")
                non_significant_categories.append(category)
            
            # Print detailed analysis
            print("\nDetailed comparison:")
            print(comparison['results'].to_string(index=False))
            
            # Create visualization
            plot_airline_comparison(df, american_airlines, selected_airline, category, comparison)
        
        # Print summary
        print("\n" + "="*80)
        print(f"Summary: AmericanAir vs {selected_airline}")
        print("="*80)
        print("Statistically significant categories:")
        for cat in significant_categories:
            print(f"- {cat}")
        print("\nNot statistically significant categories:")
        for cat in non_significant_categories:
            print(f"- {cat}")
            
    except Exception as e:
        print("An error occurred in the main function:")
        print(traceback.format_exc())

if __name__ == "__main__":
    main() 
