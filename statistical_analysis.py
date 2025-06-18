from scipy import stats
def run_consolidated_sentiment_analysis():
    """
    Run t-tests for all categories and display results in a single consolidated view
    """
    from db_repository import get_available_categories, get_connection
    
    # Get all available categories
    categories = get_available_categories()
    print(f"Running t-tests for {len(categories)} categories")
    
    # Create results container
    results = []
    
    # Process each category
    for category in categories:
        print(f"Analyzing category: {category}")
        
        # Get data from database
        conn = get_connection()
        query = f"""
        SELECT 
            u.screen_name AS airline,
            cs.final_sentiment
        FROM detected_issues di
        JOIN conversation c ON di.conversation_id = c.id
        JOIN [user] u ON c.airline_id = u.id
        JOIN conversation_sentiment cs ON c.id = cs.conversation_id
        WHERE di.issue_type = '{category}'
        AND u.screen_name IN ('AmericanAir', 'British_Airways')
        AND cs.final_sentiment IS NOT NULL
        """
        
        df = pd.read_sql(query, conn)
        
        if len(df) == 0:
            print(f"No data found for category: {category}")
            continue
            
        # Separate the data for each airline
        ba_data = df[df['airline'] == 'British_Airways']['final_sentiment']
        aa_data = df[df['airline'] == 'AmericanAir']['final_sentiment']
        
        # Skip if either airline doesn't have enough data
        if len(ba_data) < 2 or len(aa_data) < 2:
            print(f"Not enough samples for category: {category}")
            continue
        
        # Perform t-test
        t_stat, p_value = stats.ttest_ind(ba_data, aa_data, equal_var=False)
        
        # Calculate means and confidence intervals
        ba_mean = ba_data.mean()
        aa_mean = aa_data.mean()
        difference = ba_mean - aa_mean
        
        # Calculate 95% confidence interval for the difference
        # Using pooled standard error formula for Welch's t-test
        ba_var = ba_data.var()
        aa_var = aa_data.var()
        se = np.sqrt(ba_var/len(ba_data) + aa_var/len(aa_data))
        ci_95 = 1.96 * se  # Approximate 95% CI
        
        results.append({
            'category': category,
            'ba_count': len(ba_data),
            'aa_count': len(aa_data),
            'ba_mean': ba_mean,
            'aa_mean': aa_mean,
            'difference': difference,
            'ci_95': ci_95,
            'p_value': p_value,
            'significant': p_value < 0.05
        })
    
    # Create DataFrame with results
    results_df = pd.DataFrame(results)
    
    if len(results_df) == 0:
        print("No valid results found!")
        return
    
    # Sort by absolute difference (to highlight largest effects)
    results_df = results_df.sort_values('p_value')
    
    # Save results to CSV
    os.makedirs("plots", exist_ok=True)
    results_df.to_csv("plots/all_sentiment_ttest_results.csv", index=False)
    
    # Create consolidated visualization
    plt.figure(figsize=(14, max(8, len(results_df)*0.5)))
    
    # Create a forest plot showing differences with confidence intervals
    y_pos = np.arange(len(results_df))
    
    # Plot the difference line at zero (no difference)
    plt.axvline(x=0, color='gray', linestyle='--', alpha=0.7)
    
    # Plot differences as points with confidence intervals
    for i, (_, row) in enumerate(results_df.iterrows()):
        color = 'red' if row['significant'] else 'blue'
        marker = 'o' if row['significant'] else 'o'
        
        plt.plot([row['difference']], [i], marker=marker, markersize=10, 
                 color=color, alpha=0.8)
        plt.plot([row['difference'] - row['ci_95'], 
                  row['difference'] + row['ci_95']], [i, i], 
                 color=color, linewidth=2, alpha=0.8)
    
    # Add category names and p-values
    categories = []
    for i, (_, row) in enumerate(results_df.iterrows()):
        sig_symbol = "**" if row['significant'] else ""
        p_format = f"{row['p_value']:.4f}" if row['p_value'] >= 0.0001 else "<0.0001"
        sample_info = f"BA: {row['ba_count']}, AA: {row['aa_count']}"
        categories.append(f"{row['category']} {sig_symbol}\np={p_format}, {sample_info}")
    
    plt.yticks(y_pos, categories)
    
    # Add means to the right of the plot
    for i, (_, row) in enumerate(results_df.iterrows()):
        plt.text(plt.xlim()[1]*0.85, i, f"BA: {row['ba_mean']:.3f}\nAA: {row['aa_mean']:.3f}", 
                 va='center', ha='left', fontsize=9,
                 bbox=dict(facecolor='white', alpha=0.7, boxstyle='round,pad=0.2'))
    
    # Style the plot
    plt.title('Sentiment Differences Between British Airways and American Airlines\nBy Issue Category', fontsize=14)
    plt.xlabel('Difference in Final Sentiment (BA - AA)', fontsize=12)
    plt.grid(axis='x', linestyle='--', alpha=0.3)
    
    # Add legend
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker='o', color='red', label='Significant (p<0.05)', 
               markersize=10, linestyle=''),
        Line2D([0], [0], marker='o', color='blue', label='Not Significant', 
               markersize=10, linestyle='')
    ]
    plt.legend(handles=legend_elements, loc='lower right')
    
    # Create a heatmap showing sentiment by airline and category
    plt.figure(figsize=(16, 10))
    
    # Reshape data for heatmap
    heatmap_data = []
    for _, row in results_df.iterrows():
        heatmap_data.append({
            'category': row['category'],
            'airline': 'British Airways',
            'mean_sentiment': row['ba_mean']
        })
        heatmap_data.append({
            'category': row['category'],
            'airline': 'American Air',
            'mean_sentiment': row['aa_mean']
        })
    
    heatmap_df = pd.DataFrame(heatmap_data)
    heatmap_pivot = heatmap_df.pivot(index='category', columns='airline', values='mean_sentiment')
    
    # Create heatmap
    ax = sns.heatmap(heatmap_pivot, annot=True, cmap='RdBu_r', center=0, 
                     fmt='.3f', linewidths=.5, cbar_kws={'label': 'Mean Sentiment'})
    
    # Mark significant differences with asterisks
    for i, category in enumerate(heatmap_pivot.index):
        row = results_df[results_df['category'] == category]
        if len(row) > 0 and row.iloc[0]['significant']:
            plt.text(0.5, i+0.5, '*', fontsize=20, ha='center', va='center', color='black')
    
    plt.title('Mean Final Sentiment by Airline and Issue Category', fontsize=14)
    plt.tight_layout()
    
    # Save plots
    plt.savefig("plots/consolidated_sentiment_analysis.png", dpi=300, bbox_inches='tight')
    
    plt.show()
    
    # Print summary of findings
    sig_count = results_df['significant'].sum()
    print(f"\nAnalysis complete. Found {sig_count} categories with significant differences.")
    if sig_count > 0:
        print("\nCategories with significant differences:")
        sig_df = results_df[results_df['significant']]
        for _, row in sig_df.iterrows():
            better = "British Airways" if row['difference'] > 0 else "American Airlines"
            print(f"- {row['category']}: {better} has significantly better sentiment (p={row['p_value']:.4f})")

    return results_df

# Use this as the main function
if __name__ == "__main__":
    import os
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    # Configure plot style
    plt.style.use('seaborn-v0_8-whitegrid')
    sns.set_context("paper", font_scale=1.2)
    
    # Create output directory if it doesn't exist
    os.makedirs("plots", exist_ok=True)
    
    print("Running consolidated sentiment analysis for all categories...")
    
    # Run consolidated analysis
    run_consolidated_sentiment_analysis()
    
    print("Analysis complete. Results saved to 'plots' directory.")