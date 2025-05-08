import matplotlib.pyplot as plt
import numpy as np

# Data
categories = [
    "Unique Tweets",
    "Data Size (GB)",
    "Mentions of Airline",
    "Conversations"
]

before_cleaning = [6094135, 36.048, 812954, 94750]
after_cleaning = [5863237, 28.985, 812737, 94750]

# Create subplots
fig, axes = plt.subplots(nrows=1, ncols=len(categories), figsize=(20, 5))

for i, (ax, category) in enumerate(zip(axes, categories)):
    bars = ax.bar(["Before", "After"], 
                  [before_cleaning[i], after_cleaning[i]], 
                  color=['skyblue', 'orange'])

    # Add labels, title, and values on top of the bars
    ax.set_title(f'{category}')
    ax.set_ylabel('Value')
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:,.0f}' if isinstance(height, int) else f'{height:.3f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # Offset text by 3 points
                    textcoords="offset points",
                    ha='center', va='bottom')

# Adjust layout
plt.tight_layout()
plt.show()