import os
import inspect
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime
from db_repository import get_issue_counts

# Configure matplotlib and seaborn for better plots
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_context("paper", font_scale=1.2)

# Constants
OUTPUT_DIR = "plots"  # Output directory for all plots

def create_output_directory():
    """Create the output directory if it doesn't exist"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")

def save_plot(fig, filename):
    """Save the given figure to the output directory"""
    # Create output directory if it doesn't exist
    create_output_directory()
    
    # Ensure filename has the correct extension
    if not filename.endswith(".png"):
        filename = f"{filename}.png"
    
    # Create the full path
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    # Save the figure
    fig.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved: {filepath}")
    
    return filepath

# ===== PLOTTING FUNCTIONS =====

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
    create_output_directory()
    
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