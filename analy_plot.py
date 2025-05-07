import matplotlib.pyplot as plt
import numpy as np

# Data for raw and clean conversations
airlines = [
    "easyJet", "BritishAirways", "AmericanAir", "Qantas", "RyanAir",
    "VirginAtlantic", "KLM", "SingaporeAir", "Lufthansa", "EtihadAirways", "AirFrance"
]

raw_data = [34497, 57034, 94750, 8411, 12520, 14218, 20608, 7468, 9670, 958, 5236]
clean_data = [34495, 57034, 94750, 8411, 12520, 14216, 20607, 7468, 9670, 958, 5236]

# Bar chart settings
x = np.arange(len(airlines))  # X-axis positions
width = 0.3  # Reduce the width of the bars to create more space

# Create the bar chart
fig, ax = plt.subplots(figsize=(12, 6))
bars1 = ax.bar(x - width, raw_data, width, label='Raw Data', color='skyblue')  # Shift left
bars2 = ax.bar(x + width, clean_data, width, label='Clean Data', color='orange')  # Shift right

# Add labels, title, and legend
ax.set_xlabel('Airlines')
ax.set_ylabel('Number of Conversations')
ax.set_title('Conversations per Airline: Raw vs Clean Data')
ax.set_xticks(x)
ax.set_xticklabels(airlines, rotation=45, ha='right')
ax.legend()

# Add values on top of the bars
def add_values(bars):
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # Offset text by 3 points
                    textcoords="offset points",
                    ha='center', va='bottom')

add_values(bars1)
add_values(bars2)

# Show the plot
plt.tight_layout()
plt.show()