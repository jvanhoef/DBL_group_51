import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
from scipy.stats import chi2_contingency, kruskal, mannwhitneyu
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from itertools import combinations
from db_repository import get_conversations_with_tweets_and_sentiment

tqdm.pandas()

def load_data():
    """Load data from database and perform initial processing"""
    df = get_conversations_with_tweets_and_sentiment()
    df = df.rename(columns={"issue_type": "topic"})  # for clarity
    
    # Calculate sentiment change per conversation (assuming sentiment values are available)
    # Modify this according to your actual sentiment data structure
    if 'sentiment' in df.columns:
        # Group by conversation and calculate initial and final sentiment
        sentiment_change = df.groupby('conversation_id').agg({
            'sentiment': ['first', 'last']
        })
        sentiment_change.columns = ['initial_sentiment', 'final_sentiment']
        sentiment_change['sentiment_change'] = sentiment_change['final_sentiment'] - sentiment_change['initial_sentiment']
        
        # Merge back to conversation-level dataframe
        df_conv = (
            df.groupby("conversation_id")
            .agg({
                "airline": "first",
                "topic": "first",
                "resolved_in_conversation": "first"
            })
        )
        df_conv = df_conv.join(sentiment_change['sentiment_change'])
    else:
        # If sentiment isn't available, create conversation-level dataframe without sentiment change
        df_conv = (
            df.groupby("conversation_id")
            .agg({
                "airline": "first",
                "topic": "first",
                "resolved_in_conversation": "first"
            })
        )
    
    return df, df_conv

def basic_statistical_analysis(df_conv):
    """Perform the original basic statistical analysis"""
    # Prepare data for plots
    topic_counts = df_conv['topic'].value_counts().sort_values(ascending=False)
    resolution_rate = df_conv.groupby('topic')['resolved_in_conversation'].mean().sort_values(ascending=False)
    airline_resolution = df_conv.groupby('airline')['resolved_in_conversation'].mean().sort_values(ascending=False)
    contingency = pd.crosstab(df_conv['topic'], df_conv['resolved_in_conversation'])
    contingency.columns = ['Not Resolved', 'Resolved']

    # Chi-Square Test for Independence
    chi2, p, dof, expected = chi2_contingency(contingency)
    print(f"Chi-square test: chi2={chi2:.4f}, p-value={p:.4f}, dof={dof}")

    # Combined plot
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))

    # Top left: Number of conversations per topic
    sns.barplot(x=topic_counts.index, y=topic_counts.values, palette='viridis', ax=axes[0,0])
    axes[0,0].set_title('Number of Conversations per Topic')
    axes[0,0].set_ylabel('Number of Conversations')
    axes[0,0].set_xlabel('Topic')
    axes[0,0].tick_params(axis='x', rotation=45)

    # Top right: Resolution rate per topic
    sns.barplot(x=resolution_rate.index, y=resolution_rate.values, palette='mako', ax=axes[0,1])
    axes[0,1].set_title('Resolution Rate per Topic')
    axes[0,1].set_ylabel('Resolution Rate')
    axes[0,1].set_xlabel('Topic')
    axes[0,1].set_ylim(0,1)
    axes[0,1].tick_params(axis='x', rotation=45)

    # Bottom left: Overall resolution rate per airline
    sns.barplot(x=airline_resolution.index, y=airline_resolution.values, palette='crest', ax=axes[1,0])
    axes[1,0].set_title('Overall Resolution Rate per Airline')
    axes[1,0].set_ylabel('Resolution Rate')
    axes[1,0].set_xlabel('Airline')
    axes[1,0].set_ylim(0,1)
    axes[1,0].tick_params(axis='x', rotation=45)

    # Bottom right: Heatmap of topic vs. resolution status
    sns.heatmap(contingency, annot=True, fmt='d', cmap='Blues', ax=axes[1,1])
    axes[1,1].set_title("Topic vs. Resolution Status")
    axes[1,1].set_ylabel("Topic")
    axes[1,1].set_xlabel("Resolution Status")

    plt.tight_layout()
    plt.savefig("combined_statistical_overview.png")
    plt.show()

def within_airline_topic_comparison(df_conv, airline_name):
    """
    Analyze sentiment change differences between topics for a specific airline
    
    Parameters:
    df_conv (DataFrame): Conversation-level dataframe
    airline_name (str): Name of the airline to analyze
    """
    if 'sentiment_change' not in df_conv.columns:
        print("Error: Sentiment change data not available")
        return
        
    # Filter for the specific airline
    df_airline = df_conv[df_conv['airline'] == airline_name].copy()
    
    if len(df_airline) == 0:
        print(f"No data found for airline: {airline_name}")
        return
        
    print(f"\n--- Topic Analysis for {airline_name} ---")
    
    # ANOVA analysis
    try:
        model = ols('sentiment_change ~ C(topic)', data=df_airline).fit()
        anova_table = sm.stats.anova_lm(model, typ=2)
        print("\nANOVA Results for Topic Comparison:")
        print(anova_table)
        
        # If ANOVA is significant, run post-hoc tests
        if anova_table['PR(>F)'][0] < 0.05:
            tukey = pairwise_tukeyhsd(df_airline['sentiment_change'], 
                                    df_airline['topic'], 
                                    alpha=0.05)
            print("\nTukey HSD Post-hoc Test:")
            print(tukey)
    except Exception as e:
        print(f"Error running ANOVA: {str(e)}")
        
    # Non-parametric alternative: Kruskal-Wallis Test
    topics = df_airline['topic'].unique()
    if len(topics) < 2:
        print("Not enough topics for comparison")
        return
        
    print("\nKruskal-Wallis Test (Non-parametric alternative to ANOVA):")
    try:
        samples = [df_airline[df_airline['topic']==topic]['sentiment_change'] for topic in topics]
        # Filter out empty samples
        samples = [s for s in samples if len(s) > 0]
        
        if len(samples) < 2:
            print("Not enough topics with data for Kruskal-Wallis test")
        else:
            kruskal_stat, kruskal_p = kruskal(*samples)
            print(f"Statistic={kruskal_stat:.4f}, p-value={kruskal_p:.4f}")
            
            # If significant, run Mann-Whitney U tests with Bonferroni correction
            if kruskal_p < 0.05:
                print("\nRunning pairwise Mann-Whitney U tests:")
                topic_pairs = list(combinations(topics, 2))
                alpha = 0.05 / len(topic_pairs)  # Bonferroni correction
                
                for t1, t2 in topic_pairs:
                    sample1 = df_airline[df_airline['topic']==t1]['sentiment_change']
                    sample2 = df_airline[df_airline['topic']==t2]['sentiment_change']
                    if len(sample1) > 0 and len(sample2) > 0:
                        u_stat, p_val = mannwhitneyu(sample1, sample2, alternative='two-sided')
                        print(f"{t1} vs {t2}: U={u_stat:.4f}, p={p_val:.4f}, significant: {p_val < alpha}")
    except Exception as e:
        print(f"Error running Kruskal-Wallis test: {str(e)}")
    
    # Visualize with boxplots
    plt.figure(figsize=(14, 7))
    sns.boxplot(x='topic', y='sentiment_change', hue='resolved_in_conversation', data=df_airline)
    plt.title(f'Sentiment Change by Topic for {airline_name}')
    plt.xlabel('Conversation Topic')
    plt.ylabel('Sentiment Change')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{airline_name}_sentiment_by_topic.png")
    plt.show()

def cross_airline_topic_comparison(df_conv):
    """
    Compare how topics affect sentiment change across different airlines
    
    Parameters:
    df_conv (DataFrame): Conversation-level dataframe
    """
    if 'sentiment_change' not in df_conv.columns:
        print("Error: Sentiment change data not available")
        return
        
    print("\n--- Cross-Airline Topic Comparison ---")
    
    # Two-way ANOVA: Airline x Topic
    try:
        model = ols('sentiment_change ~ C(airline) + C(topic) + C(airline):C(topic)', data=df_conv).fit()
        anova_table = sm.stats.anova_lm(model, typ=2)
        print("\nTwo-way ANOVA Results:")
        print(anova_table)
    except Exception as e:
        print(f"Error running two-way ANOVA: {str(e)}")
        
    # Visualize with interaction plot
    plt.figure(figsize=(14, 8))
    for airline in df_conv['airline'].unique():
        airline_data = df_conv[df_conv['airline'] == airline]
        if len(airline_data) > 0:
            means = airline_data.groupby('topic')['sentiment_change'].mean()
            plt.plot(means.index, means.values, marker='o', label=airline)

    plt.title('Average Sentiment Change by Topic Across Airlines')
    plt.xlabel('Conversation Topic')
    plt.ylabel('Mean Sentiment Change')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("cross_airline_sentiment_comparison.png")
    plt.show()
    
    # Topic-Specific Airline Comparisons
    print("\n--- Topic-Specific Airline Comparisons ---")
    for topic in df_conv['topic'].unique():
        topic_data = df_conv[df_conv['topic'] == topic]
        
        if len(topic_data) < 10:  # Skip topics with very little data
            continue
            
        try:
            # ANOVA for this specific topic across airlines
            model = ols('sentiment_change ~ C(airline)', data=topic_data).fit()
            anova_result = sm.stats.anova_lm(model, typ=2)
            
            print(f"\nTopic: {topic}")
            print(f"ANOVA result: F={anova_result['F'][0]:.4f}, p-value={anova_result['PR(>F)'][0]:.4f}")
            
            # If significant, run post-hoc tests
            if anova_result['PR(>F)'][0] < 0.05:
                tukey = pairwise_tukeyhsd(topic_data['sentiment_change'], 
                                         topic_data['airline'], 
                                         alpha=0.05)
                print(tukey)
        except Exception as e:
            print(f"Error analyzing topic {topic}: {str(e)}")

def resolution_status_analysis(df_conv):
    """
    Analyze how topics affect resolution status
    
    Parameters:
    df_conv (DataFrame): Conversation-level dataframe
    """
    print("\n--- Resolution Status Analysis by Topic ---")
    
    # Create contingency table
    contingency = pd.crosstab(df_conv['topic'], df_conv['resolved_in_conversation'])
    print("\nContingency table:")
    print(contingency)
    
    # Chi-square test
    chi2, p, dof, expected = chi2_contingency(contingency)
    print(f"\nChi-square test for association between topic and resolution status:")
    print(f"Chi2={chi2:.4f}, p-value={p:.4f}, dof={dof}")
    
    # Visualize with stacked bar chart
    plt.figure(figsize=(14, 7))
    contingency_pct = contingency.div(contingency.sum(axis=1), axis=0) * 100
    contingency_pct.plot(kind='bar', stacked=True)
    plt.title('Resolution Rate by Topic')
    plt.xlabel('Topic')
    plt.ylabel('Percentage')
    plt.legend(title='Resolved')
    plt.tight_layout()
    plt.savefig("topic_resolution_rate.png")
    plt.show()
    
    # Check if airline-specific analysis is possible
    if len(df_conv['airline'].unique()) > 1:
        print("\nComparing resolution rates by topic across airlines:")
        for airline in df_conv['airline'].unique():
            airline_data = df_conv[df_conv['airline'] == airline]
            print(f"\n{airline}:")
            resolution_by_topic = airline_data.groupby('topic')['resolved_in_conversation'].mean()
            print(resolution_by_topic)

def main():
    """Main function to run the analysis"""
    print("Loading data...")
    df, df_conv = load_data()
    
    print("\nChoose an analysis to run:")
    print("1. Basic Statistical Analysis")
    print("2. Within-Airline Topic Comparison")
    print("3. Cross-Airline Topic Comparison")
    print("4. Resolution Status Analysis")
    print("5. Run All Analyses")
    
    choice = input("Enter your choice (1-5): ")
    
    if choice == '1':
        basic_statistical_analysis(df_conv)
    elif choice == '2':
        airline = input("Enter airline name to analyze: ")
        within_airline_topic_comparison(df_conv, airline)
    elif choice == '3':
        cross_airline_topic_comparison(df_conv)
    elif choice == '4':
        resolution_status_analysis(df_conv)
    elif choice == '5':
        basic_statistical_analysis(df_conv)
        
        # Run within-airline analysis for each airline
        for airline in df_conv['airline'].unique():
            within_airline_topic_comparison(df_conv, airline)
            
        cross_airline_topic_comparison(df_conv)
        resolution_status_analysis(df_conv)
    else:
        print("Invalid choice. Please run again and select a valid option.")

if __name__ == "__main__":
    main()