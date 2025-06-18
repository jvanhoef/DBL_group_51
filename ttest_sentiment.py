import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import ttest_ind, mannwhitneyu, shapiro
from scipy import stats
from statsmodels.stats.power import ttest_power
from db_repository import get_connection
import warnings
import os
warnings.filterwarnings('ignore')

# Set up plotting style
plt.style.use('default')

def get_available_airlines():
    """Get list of airlines available in the database"""
    conn = get_connection()
    
    query = """
    SELECT DISTINCT u.screen_name as airline_name
    FROM conversation c
    JOIN [user] u ON c.airline_id = u.id
    JOIN conversation_sentiment cs ON c.id = cs.conversation_id
    JOIN detected_issues di ON c.id = di.conversation_id
    WHERE cs.initial_sentiment IS NOT NULL 
      AND cs.final_sentiment IS NOT NULL
    ORDER BY u.screen_name
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df['airline_name'].tolist()

def select_comparison_airline(available_airlines):
    """Let user select which airline to compare with AmericanAir"""
    american_air = 'AmericanAir'
    
    if american_air not in available_airlines:
        print("Error: AmericanAir not found in database")
        return None
    
    # Remove AmericanAir from selection list
    comparison_airlines = [airline for airline in available_airlines if airline != american_air]
    
    print(f"\n{'='*60}")
    print("AIRLINE SENTIMENT T-TEST ANALYSIS")
    print(f"{'='*60}")
    print(f"Base airline: {american_air}")
    print(f"\nAvailable airlines for comparison:")
    
    for i, airline in enumerate(comparison_airlines, 1):
        print(f"{i:2d}. {airline}")
    
    while True:
        try:
            choice = int(input(f"\nSelect airline to compare with {american_air} (1-{len(comparison_airlines)}): "))
            if 1 <= choice <= len(comparison_airlines):
                selected_airline = comparison_airlines[choice - 1]
                print(f"\nSelected: {selected_airline}")
                return selected_airline
            else:
                print(f"Please enter a number between 1 and {len(comparison_airlines)}")
        except ValueError:
            print("Please enter a valid number")

def fetch_sentiment_data(airline1, airline2):
    """Fetch sentiment data for two airlines"""
    conn = get_connection()
    
    query = """
    SELECT 
        u.screen_name as airline_name,
        di.issue_type,
        cs.final_sentiment - cs.initial_sentiment as sentiment_difference,
        cs.initial_sentiment,
        cs.final_sentiment,
        cs.first_response_time_sec,
        cs.avg_response_time_sec,
        cs.resolved_to_dm,
        cs.user_tweets_count,
        cs.airline_tweets_count,
        COUNT(*) OVER (PARTITION BY u.screen_name, di.issue_type) as sample_size
    FROM conversation_sentiment cs
    JOIN conversation c ON cs.conversation_id = c.id
    JOIN [user] u ON c.airline_id = u.id
    JOIN detected_issues di ON cs.conversation_id = di.conversation_id
    WHERE cs.initial_sentiment IS NOT NULL 
      AND cs.final_sentiment IS NOT NULL
      AND u.screen_name IN (?, ?)
    ORDER BY u.screen_name, di.issue_type
    """
    
    df = pd.read_sql(query, conn, params=(airline1, airline2))
    conn.close()
    
    return df

def analyze_sample_sizes(df):
    """Analyze and display sample sizes for each airline-issue combination"""
    print(f"\n{'='*60}")
    print("SAMPLE SIZE ANALYSIS")
    print(f"{'='*60}")
    
    sample_sizes = df.groupby(['airline_name', 'issue_type']).size().unstack(fill_value=0)
    
    print("\nSample sizes per airline-issue combination:")
    print("-" * 80)
    
    for airline in df['airline_name'].unique():
        print(f"\n{airline}:")
        for issue_type in df['issue_type'].unique():
            size = len(df[(df['airline_name'] == airline) & (df['issue_type'] == issue_type)])
            if size > 0:
                status = "✓ Sufficient" if size >= 30 else "⚠ Small sample" if size >= 10 else "✗ Too small"
                print(f"  {issue_type:20s}: {size:3d} samples {status}")
    
    return sample_sizes

def check_normality(data, airline_name, issue_type):
    """Check if data is normally distributed using Shapiro-Wilk test"""
    if len(data) < 3:
        return False, 0
    
    statistic, p_value = shapiro(data)
    is_normal = p_value > 0.05
    
    return is_normal, p_value

def plot_distributions(data1, data2, airline1, airline2, issue_type, test_name, p_value):
    """Plot clean, readable histograms showing overlap and p-value interpretation"""
    plt.figure(figsize=(14, 10))
    
    # Create clean histogram
    plt.hist(data1, bins=30, alpha=0.7, label=airline1, color='blue', density=True, edgecolor='black', linewidth=0.5)
    plt.hist(data2, bins=30, alpha=0.7, label=airline2, color='red', density=True, edgecolor='black', linewidth=0.5)
    
    # Add normal distribution curves
    x_range = np.linspace(min(min(data1), min(data2)), max(max(data1), max(data2)), 100)
    
    # Normal curve for data1
    mean1, std1 = data1.mean(), data1.std()
    normal1 = stats.norm.pdf(x_range, mean1, std1)
    plt.plot(x_range, normal1, color='blue', linestyle='-', linewidth=2, alpha=0.8, 
             label=f'{airline1} Normal Curve')
    
    # Normal curve for data2
    mean2, std2 = data2.mean(), data2.std()
    normal2 = stats.norm.pdf(x_range, mean2, std2)
    plt.plot(x_range, normal2, color='red', linestyle='-', linewidth=2, alpha=0.8, 
             label=f'{airline2} Normal Curve')
    
    # Add vertical lines for means
    plt.axvline(mean1, color='blue', linestyle='--', linewidth=3, label=f'{airline1} Average: {mean1:.3f}')
    plt.axvline(mean2, color='red', linestyle='--', linewidth=3, label=f'{airline2} Average: {mean2:.3f}')
    
    # Calculate overlap area (simplified)
    hist1, _ = np.histogram(data1, bins=30, density=True)
    hist2, _ = np.histogram(data2, bins=30, density=True)
    overlap = np.minimum(hist1, hist2).sum() / hist1.sum() if hist1.sum() > 0 else 0
    
    # Determine significance interpretation
    if p_value < 0.001:
        significance = "*** HIGHLY SIGNIFICANT ***"
        significance_color = "darkgreen"
    elif p_value < 0.01:
        significance = "** VERY SIGNIFICANT **"
        significance_color = "green"
    elif p_value < 0.05:
        significance = "* SIGNIFICANT *"
        significance_color = "orange"
    else:
        significance = "NOT SIGNIFICANT"
        significance_color = "red"
    
    # Set up the plot
    plt.title(f'Comparing {airline1} vs {airline2} for {issue_type}\n'
              f'Are they really different? p-value: {p_value:.4f}', 
              fontsize=16, fontweight='bold')
    
    plt.xlabel('Sentiment Change (Final - Initial)', fontsize=14)
    plt.ylabel('How Common Each Value Is', fontsize=14)
    plt.legend(fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # Add simple stats box
    stats_text = f"Sample Sizes:\n{airline1}: {len(data1):,} conversations\n{airline2}: {len(data2):,} conversations\n\n"
    stats_text += f"Average Sentiment Change:\n{airline1}: {mean1:.3f}\n{airline2}: {mean2:.3f}\n\n"
    stats_text += f"Difference: {abs(mean1 - mean2):.3f}"
    
    props2 = dict(boxstyle='round', facecolor='lightblue', alpha=0.8)
    plt.text(0.02, 0.95, stats_text, transform=plt.gca().transAxes, fontsize=10,
             verticalalignment='top', bbox=props2)
    
    # Add p-value explanation
    p_explanation = f"p-value = {p_value:.4f}\n"
    if p_value < 0.05:
        p_explanation += "p < 0.05 means the difference\nis statistically significant"
    else:
        p_explanation += "p ≥ 0.05 means the difference\nis NOT statistically significant"
    
    props3 = dict(boxstyle='round', facecolor='yellow', alpha=0.8)
    plt.text(0.75, 0.95, p_explanation, transform=plt.gca().transAxes, fontsize=10,
             verticalalignment='top', bbox=props3, fontweight='bold')
    
    plt.tight_layout()
    
    # Create folder for saving plots
    folder_name = f"ttest_AmericanAir_vs_{airline2}"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    
    # Save the plot
    filename = f"{folder_name}/histogram_{airline1}_vs_{airline2}_{issue_type.replace(' ', '_')}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Educational histogram saved as: {filename}")
    
    plt.show()

def choose_appropriate_test(n1, n2, data1, data2, airline1, airline2, issue_type):
    """Choose the most appropriate test based on sample sizes and normality"""
    
    # Check normality for both groups
    normal1, p1 = check_normality(data1, airline1, issue_type)
    normal2, p2 = check_normality(data2, airline2, issue_type)
    
    # Choose test based on sample size and normality
    if n1 >= 30 and n2 >= 30:
        # Large samples - use Welch's t-test regardless of normality
        test_name = "Welch's T-Test"
        t_stat, p_value = ttest_ind(data1, data2, equal_var=False)
        test_statistic = t_stat
        test_explanation = "Large samples (n≥30) - Central Limit Theorem applies"
        
    elif n1 >= 10 and n2 >= 10:
        if normal1 and normal2:
            # Normal data, sufficient samples - use Welch's t-test
            test_name = "Welch's T-Test"
            t_stat, p_value = ttest_ind(data1, data2, equal_var=False)
            test_statistic = t_stat
            test_explanation = "Sufficient samples with normal distributions"
        else:
            # Non-normal data - use Mann-Whitney U test
            test_name = "Mann-Whitney U Test"
            statistic, p_value = mannwhitneyu(data1, data2, alternative='two-sided')
            test_statistic = statistic
            test_explanation = "Non-normal data - using non-parametric test"
            
    else:
        # Small samples - use Mann-Whitney U test
        test_name = "Mann-Whitney U Test"
        statistic, p_value = mannwhitneyu(data1, data2, alternative='two-sided')
        test_statistic = statistic
        test_explanation = "Small samples - using non-parametric test"
    
    return test_name, p_value, test_statistic, normal1, normal2, test_explanation

def calculate_effect_size(data1, data2):
    """Calculate Cohen's d effect size"""
    n1, n2 = len(data1), len(data2)
    
    if n1 == 0 or n2 == 0:
        return 0, "N/A"
    
    # Pooled standard deviation
    pooled_std = np.sqrt(((n1-1)*np.var(data1, ddof=1) + (n2-1)*np.var(data2, ddof=1)) / (n1+n2-2))
    
    if pooled_std == 0:
        return 0, "N/A"
    
    cohens_d = (np.mean(data1) - np.mean(data2)) / pooled_std
    
    # Interpret effect size
    if abs(cohens_d) < 0.2:
        effect_size = "Small"
    elif abs(cohens_d) < 0.5:
        effect_size = "Medium"
    else:
        effect_size = "Large"
    
    return cohens_d, effect_size

def perform_ttest_analysis(df, airline1, airline2):
    """Perform t-test analysis for only the specified issue categories"""
    print(f"\n{'='*60}")
    print(f"T-TEST ANALYSIS: {airline1} vs {airline2}")
    print(f"{'='*60}")
    
    results = []
    allowed_categories = ['customer_service', 'luggage', 'delay']
    for issue_type in allowed_categories:
        # Get data for both airlines
        data1 = df[(df['airline_name'] == airline1) & (df['issue_type'] == issue_type)]['sentiment_difference']
        data2 = df[(df['airline_name'] == airline2) & (df['issue_type'] == issue_type)]['sentiment_difference']
        n1, n2 = len(data1), len(data2)
        # Skip if insufficient data
        if n1 < 5 or n2 < 5:
            print(f"\n{issue_type}: Insufficient data (n1={n1}, n2={n2})")
            continue
        # Choose and perform appropriate test
        test_name, p_value, test_statistic, normal1, normal2, test_explanation = choose_appropriate_test(
            n1, n2, data1, data2, airline1, airline2, issue_type
        )
        # Calculate effect size
        cohens_d, effect_size = calculate_effect_size(data1, data2)
        # Determine significance
        significant = p_value < 0.05
        significance_level = "p < 0.001" if p_value < 0.001 else f"p = {p_value:.3f}"
        # Store results
        result = {
            'issue_type': issue_type,
            'test_used': test_name,
            'test_explanation': test_explanation,
            'p_value': p_value,
            'test_statistic': test_statistic,
            'significant': significant,
            'significance_level': significance_level,
            'effect_size': cohens_d,
            'effect_magnitude': effect_size,
            'airline1_sample_size': n1,
            'airline2_sample_size': n2,
            'airline1_mean': data1.mean(),
            'airline2_mean': data2.mean(),
            'airline1_std': data1.std(),
            'airline2_std': data2.std(),
            'normal1': normal1,
            'normal2': normal2
        }
        results.append(result)
        # Print results
        print(f"\n{issue_type}:")
        print(f"  Test: {test_name}")
        print(f"  Explanation: {test_explanation}")
        print(f"  Sample sizes: {airline1} (n={n1}), {airline2} (n={n2})")
        print(f"  Means: {airline1} ({data1.mean():.3f}), {airline2} ({data2.mean():.3f})")
        print(f"  {significance_level} | Effect size: {cohens_d:.3f} ({effect_size})")
        print(f"  Significant difference: {'✓ YES' if significant else '✗ NO'}")
        print(f"  Normality: {airline1} ({'✓' if normal1 else '✗'}), {airline2} ({'✓' if normal2 else '✗'})")
        # Plot distributions
        plot_distributions(data1, data2, airline1, airline2, issue_type, test_name, p_value)
    return pd.DataFrame(results)

def print_summary_report(results_df, airline1, airline2):
    """Print a summary report of the analysis"""
    print(f"\n{'='*60}")
    print(f"SUMMARY REPORT: {airline1} vs {airline2}")
    print(f"{'='*60}")
    
    if results_df.empty:
        print("No valid comparisons could be made due to insufficient data.")
        return
    
    # Overall statistics
    total_comparisons = len(results_df)
    significant_comparisons = len(results_df[results_df['significant']])
    significant_percentage = (significant_comparisons / total_comparisons) * 100
    
    print(f"\nOverall Results:")
    print(f"  Total issue categories compared: {total_comparisons}")
    print(f"  Significant differences found: {significant_comparisons} ({significant_percentage:.1f}%)")
    
    # Significant differences
    if significant_comparisons > 0:
        print(f"\nSignificant Differences (p < 0.05):")
        significant_results = results_df[results_df['significant']].sort_values('p_value')
        for _, row in significant_results.iterrows():
            direction = "better" if row['airline1_mean'] > row['airline2_mean'] else "worse"
            print(f"  {row['issue_type']}: {airline1} performs {direction} "
                  f"(d={row['effect_size']:.3f}, {row['significance_level']})")
    
    # Effect size summary
    print(f"\nEffect Size Summary:")
    effect_sizes = results_df['effect_magnitude'].value_counts()
    for size, count in effect_sizes.items():
        print(f"  {size} effect: {count} categories")
    
    # Tests used
    print(f"\nStatistical Tests Used:")
    tests_used = results_df['test_used'].value_counts()
    for test, count in tests_used.items():
        print(f"  {test}: {count} comparisons")
    
    # Test explanations
    print(f"\nTest Selection Logic:")
    for _, row in results_df.iterrows():
        print(f"  {row['issue_type']}: {row['test_explanation']}")

def print_detailed_statistics(df, airline1, airline2):
    """Print detailed descriptive statistics for only the specified categories"""
    print(f"\n{'='*60}")
    print("DETAILED DESCRIPTIVE STATISTICS")
    print(f"{'='*60}")
    allowed_categories = ['customer_service', 'luggage', 'delay']
    for issue_type in allowed_categories:
        print(f"\n{issue_type}:")
        print("-" * 40)
        for airline in [airline1, airline2]:
            data = df[(df['airline_name'] == airline) & (df['issue_type'] == issue_type)]['sentiment_difference']
            if len(data) > 0:
                print(f"\n{airline}:")
                print(f"  Count: {len(data)}")
                print(f"  Mean: {data.mean():.3f}")
                print(f"  Std: {data.std():.3f}")
                print(f"  Min: {data.min():.3f}")
                print(f"  Max: {data.max():.3f}")
                print(f"  Median: {data.median():.3f}")
                # Count positive/negative changes
                positive = (data > 0).sum()
                negative = (data < 0).sum()
                neutral = (data == 0).sum()
                total = len(data)
                print(f"  Positive changes: {positive} ({positive/total*100:.1f}%)")
                print(f"  Negative changes: {negative} ({negative/total*100:.1f}%)")
                print(f"  No change: {neutral} ({neutral/total*100:.1f}%)")

def main():
    """Main function to run the analysis for AmericanAir vs each specified airline and categories only"""
    try:
        # Define the airlines and categories to compare
        base_airline = 'AmericanAir'
        comparison_airlines = ['lufthansa', 'KLM', 'British_Airways']
        allowed_categories = ['customer_service', 'luggage', 'delay']

        for comparison_airline in comparison_airlines:
            airline1 = base_airline
            airline2 = comparison_airline

            print(f"\n{'='*60}")
            print(f"Comparing {airline1} vs {airline2} for categories: {', '.join(allowed_categories)}")
            print(f"{'='*60}")

            # Fetch data (already restricted in fetch_sentiment_data)
            print(f"\nFetching data for {airline1} and {airline2}...")
            df = fetch_sentiment_data(airline1, airline2)

            if df.empty:
                print(f"No data found for {airline1} and {airline2}.")
                continue

            print(f"Retrieved {len(df)} conversation records")

            # Analyze sample sizes
            sample_sizes = analyze_sample_sizes(df)

            # Print detailed statistics
            print_detailed_statistics(df, airline1, airline2)

            # Perform t-test analysis (already restricted in perform_ttest_analysis)
            results_df = perform_ttest_analysis(df, airline1, airline2)

            # Print summary report
            print_summary_report(results_df, airline1, airline2)

            # Save results to CSV
            if not results_df.empty:
                filename = f"ttest_results_{airline1}_vs_{airline2}.csv"
                results_df.to_csv(filename, index=False)
                print(f"\nDetailed results saved to: {filename}")

        print(f"\nAll comparisons complete!")

    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 
