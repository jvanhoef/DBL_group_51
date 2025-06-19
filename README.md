# Airline twitter sentiment analysis

## Table of contents

- [Prerequisites](#Prerequisites)
- [Project files](#Project-files)
    - [db_repository](#db_repository)
    - [completeLoading](#completeLoading)
    - [creating_conversations](#creating_conversations)
    - [sentiment_and_issues](#sentiment_and_issues)
    - [demo_util](#demo_util)
    - [plots_milestone_1](#plots_milestone_1)
    - [plots_milestone_2](#plots_milestone_2)
    - [plots_poster](#plots_poster)
    - [activity_correlation](#activity_correlation)
    



## 📦 Prerequisites

- Python 3.7 or higher  
- Visual Studio Code (or any Python IDE)  
- SQL Server Management Studio (SSMS)  
- Microsoft ODBC Driver for SQL Server  
- Required Python libraries:

    Standard Python libraries (no installation needed): `datetime`, `re`, `json`, `os`, `sys`, `collections`, `logging`

    Third-party libraries (install with pip): `torch`, `transformers`, `emoji`, `pyodbc`, `pandas`, `tqdm`, `langdetect`, `numpy`, `plotly`


- user should create an empty database in ssms called airline_tweets, in db_repository the user should change the server name to their own
  
---


## 📁 Project files


# db_repository

This Python module provides a collection of utility functions to interact with the `airline_tweets` SQL Server database. It enables querying tweet and conversation data, issue counts, sentiment metrics, and inserting conversation records. It is designed for analysis and reporting on airline-related Twitter conversations.

## Features

- Connects to the `airline_tweets` database using ODBC.
- Fetches counts and details for:
  - Issue types and counts.
  - Tweets and conversations filtered by date.
  - Airline mentions, languages, and tweet volumes over time.
  - Tweets and conversations by airline or conversation ID.
- Retrieves sentiment and resolution data linked to conversations and detected issues.
- Inserts new conversations and associated tweets into the database.
- Provides helper functions to get airline IDs, screen names, and nicely print conversation threads.
- Supports creation of database indexes to speed up queries.

## Usage

1. Set the database connection parameters: 
   Update the `server` and `database` variables inside `get_connection()` if needed.

2. Call functions as needed:  
   Import this script or run interactively to use functions such as:
   - `get_issue_counts()`
   - `get_tweet_count(conn)`
   - `get_conversation_text_by_id(conn, conversation_id)`
   - `get_sentiment_data(issue_type, selected_airlines=None)`
   - `insert_conversation(conn, user_id, airline_id, root_tweet_id)`


# completeLoading

This script processes large collections of tweet JSON files and stores their content efficiently in a **SQL Server** database. The data is processed in three structured stages: users, tweets, and tweet entities (hashtags and mentions). It includes progress tracking, batch processing, and indexing for performance.

## Features

- Reads and cleans raw tweet JSON data
- Processes users, tweets, and entities in separate stages
- Efficient batch insert/update using SQL Server `MERGE`
- Progress tracking to resume interrupted jobs
- Automatic database table creation and indexing
- TQDM-based progress bars for line-level tracking
- Creates detailed logs for each processing stage using Python's `logging` module


## Usage

1. Configure the script:
Set your database connection string and data directory paths near the top of the script.

connection = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=TwitterDB;Trusted_Connection=yes;")
data_directory = "data"

2. Place your JSON tweet files in the specified data_directory.

3. Run the script:

```bash
python process_tweets.py
```


# creating_conversations


This script analyzes and extracts Twitter conversations involving specific airline accounts. It retrieves relevant tweets from a database, reconstructs conversations, stores them, and optionally prints or writes them to a file.

## Features

- Identifies conversations initiated or replied to by airline accounts.
- Supports multiple major airline Twitter accounts.
- Displays conversation formatting and storing progress with `tqdm`.
- Optionally saves output to a file.
- Includes existing conversation checking and optional database clearing.
- `plots_presentation_1.py` — Contains functions to generate visual plots  
- `db_repository.py` — Manages database connections and queries  
- `demo_util.py` — Utility helpers such as saving plots  
- `requirements.txt` — Lists all required Python packages  

## Usage

1. Run the script from the terminal:

```bash
python creating_conversations.py
```

2. You will be prompted to enter the airline screen name.

**Supported airlines include:**

KLM, AirFrance, British_Airways, AmericanAir, Lufthansa, AirBerlin,
AirBerlin_assist, easyJet, RyanAir, SingaporeAir, Qantas, EtihadAirways, VirginAtlantic


# sentiment_and_issues


Script to create and populate tables for sentiment analysis and issue detection.
- Creates required tables with proper relationships
- Analyzes tweet sentiment
- Detects issues in conversations
- Tracks sentiment changes and response patterns

## Usage

1. Run the script from the terminal:

```bash
python sentiment_and_issues.py
```


# demo_util


This file contains the `save_plot` utility function, which saves a Matplotlib figure as a high-resolution PNG file. It automatically creates the output directory if it doesn't exist and appends the `.png` extension to the filename if missing.

## Usage

Function: `save_plot`

```python
save_plot(fig, filename, output_dir="plots")
```


# plots_milestone_1


This script generates key visualizations from the `airline_tweets` database, including:

- Comparison of JSON summary stats vs database values  
- Top 10 languages in tweets  
- Conversation counts per airline  
- Tweet volume over time  

Plots are saved as PNG files in the `plots` directory using `demo_util.save_plot()`.

## Usage

Call the plotting functions:

```python
plot_effect_on_data()
plot_top_10_languages()
plot_conversation_count_per_airline()
plot_tweet_volume_over_time()
```


# plots_milestone_2


Generates donut charts for:
- Conversation Improvement
- Final User Sentiment
- Response Time Distribution

## Usage

Run the script to save charts as PNG files in the `plots` folder. Requires a working database connection via `db_repository.get_connection()`.

## Functions
- `plot_conversation_donuts()`
- `plot_response_time_donut()`


# plots_poster
Generates the following plots for the poster:
- Sankey diagram 
- T-test
- Stacked bar chart

## Features

- Generates a Sankey diagram to visualize flow relationships.
- Performs T-test analysis with visual representation.
- Creates a stacked bar chart for categorical comparison.
- Saves all plots as PNG or HTML files using `save_plot()` utility.


# activity_correlation


Analyzes hourly tweet counts for users and airlines from a database.

## Features

- Fetches and compares tweet counts by hour.
- Calculates Pearson correlation between user and airline tweets.
- Visualizes data with line and scatter plots.
- Prints summary stats and peak activity hours.

## Usage

Run the script (requires `get_connection()` from `db_repository`).


