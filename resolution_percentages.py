import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from db_repository import get_connection

# Connect to database
conn = get_connection()

# Query to get problem categories and DM resolution counts
query = """
WITH ProblemStats AS (
    SELECT 
        di.issue_type,
        cs.resolved_to_dm,
        COUNT(*) as issue_count
    FROM detected_issues di
    JOIN conversation_sentiment cs ON di.conversation_id = cs.conversation_id
    GROUP BY di.issue_type, cs.resolved_to_dm
)
SELECT 
    issue_type,
    SUM(issue_count) as total_issues,
    SUM(CASE WHEN resolved_to_dm = 1 THEN issue_count ELSE 0 END) as resolved_in_dm,
    SUM(CASE WHEN resolved_to_dm = 0 THEN issue_count ELSE 0 END) as resolved_in_public,
    CAST(SUM(CASE WHEN resolved_to_dm = 1 THEN issue_count ELSE 0 END) AS FLOAT) / 
        SUM(issue_count) * 100 as dm_percentage
FROM ProblemStats
GROUP BY issue_type
ORDER BY total_issues DESC
"""

# Get the data
df = pd.read_sql(query, conn)

# Calculate percentages
df['public_percentage'] = (df['resolved_in_public'] / df['total_issues'] * 100).round(1)
df['dm_percentage'] = (df['resolved_in_dm'] / df['total_issues'] * 100).round(1)

# Create the figure
plt.figure(figsize=(12, 6))
bar_width = 0.75
index = range(len(df))

# Create stacked percentage bars
public_bars = plt.bar(index, df['public_percentage'], bar_width, 
        label='Resolved in Public', color='lightblue')
dm_bars = plt.bar(index, df['dm_percentage'], bar_width,
        bottom=df['public_percentage'], label='Resolved in DM', color='darkblue')

plt.xlabel('Issue Type')
plt.ylabel('Percentage of Issues')
plt.title('Resolution Methods by Category (in %)')
plt.xticks(index, df['issue_type'], rotation=45, ha='right')
plt.legend()

# Add percentage labels inside both parts of the bars
for i in index:
    public_pct = df.iloc[i]['public_percentage']
    dm_pct = df.iloc[i]['dm_percentage']
    
    # Public percentage (in the middle of public portion)
    plt.text(i, public_pct/2, f'{public_pct:.1f}%', 
             ha='center', va='center', color='black')
    
    # DM percentage (in the middle of DM portion)
    plt.text(i, public_pct + dm_pct/2, f'{dm_pct:.1f}%', 
             ha='center', va='center', color='white')

# Set y-axis to show percentages from 0 to 100
plt.ylim(0, 100)
plt.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()
