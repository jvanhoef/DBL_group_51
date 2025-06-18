import inspect
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from demo_util import save_plot
from db_repository import get_issue_counts
from plots_milestone_1 import (
    plot_effect_on_data,
    plot_top_10_languages,
    plot_conversation_count_per_airline,
    plot_tweet_volume_over_time
)
from plots_milestone_2 import (
    plot_conversation_donuts,
    plot_response_time_donut,
    plot_issue_type_counts
)
from activity_correlation import plot_activity_correlation
from plot_poster import plot_american_airlines_sentiment_sankey

# Configure matplotlib and seaborn for better plots
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_context("paper", font_scale=1.2)

# ===== PLOTTING FUNCTIONS =====
#milestone 1 - Data Overview
def plot_milestone_1():
    print("Generating: Milestone 1: Data Overview")
    plot_effect_on_data()
    plot_top_10_languages()
    plot_conversation_count_per_airline()
    plot_tweet_volume_over_time()
    
def plot_milestone_2():
    print("Generating: Milestone 2: Conversation Outcomes")
    plot_conversation_donuts()
    plot_response_time_donut()
    plot_issue_type_counts()
    plot_activity_correlation()
    
def plot_poster():
    plot_american_airlines_sentiment_sankey()
    
def plot_issue_type_counts():
    """Bar chart showing counts of different issue types"""
    print("Generating: Issue Type Distribution")
    
    # Get data from database
    df = get_issue_counts()
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # Plot bars
    bars = ax.bar(
        df['issue_type'], 
        df['issue_count'],
        color='#3498db', 
        edgecolor='#2980b9',
        alpha=0.8
    )
    
    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width()/2.,
            height + 0.01*max(df['issue_count']),
            f'{int(height):,}',
            ha='center', 
            va='bottom',
            fontsize=9
        )
    
    # Add labels and title
    ax.set_xlabel('Issue Type', fontsize=12)
    ax.set_ylabel('Number of Issues', fontsize=12)
    ax.set_title('Distribution of Customer Issues by Type', fontsize=14)
    
    # Format x-axis labels
    plt.xticks(rotation=45, ha='right')
    
    # Add grid
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    ax.set_axisbelow(True)
    
    # Tight layout
    plt.tight_layout()
    
    # Save the plot
    save_plot(fig, "issue_type_distribution")
    
    # Close the plot to free memory
    plt.close(fig)

# Add more plotting functions here...
# def plot_another_visualization():
#     # Your code here...
#     save_plot(fig, "another_visualization")
#     plt.close(fig)

def get_all_plotting_functions():
    """Get all functions in this module that start with 'plot_'"""
    # Get all functions from this module
    functions = inspect.getmembers(inspect.getmodule(get_all_plotting_functions), inspect.isfunction)
    
    # Filter for plot_ functions (excluding this helper function)
    plot_functions = [func for name, func in functions 
                     if name.startswith('plot_') and name != 'plot_all']
    
    return plot_functions

def main():
    """Run all plotting functions and save the results"""
    print("Starting plot generation...")
    
    # Get all plotting functions
    plot_functions = get_all_plotting_functions()
    print(f"Found {len(plot_functions)} plotting functions")
    
    # Run each plotting function
    for func in plot_functions:
        try:
            func()
        except Exception as e:
            print(f"Error in {func.__name__}: {str(e)}")
    
    print("Plot generation complete")

if __name__ == "__main__":
    main()