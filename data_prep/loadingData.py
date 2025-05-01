import os
import json
import sys

# Ensure the terminal uses UTF-8 encoding for output
sys.stdout.reconfigure(encoding='utf-8')

# Define the directory containing the JSON files dynamically
script_directory = os.path.dirname(__file__)
data_directory = os.path.join(script_directory, '..', 'data')

# Get a list of all files in the directory
files = os.listdir(data_directory)

# Filter the list to include only JSON files
json_files = [file for file in files if file.endswith('.json')]

# Check if there are any JSON files
if json_files:
    # Get the first JSON file
    first_json_file = json_files[0]
    first_json_path = os.path.join(data_directory, first_json_file)
    
    # Load the JSON file into a Python object
    # Load the JSON file into Python objects
    with open(first_json_path, 'r', encoding='utf8') as file:
        data_objects = []
        for line in file:
            try:
                data_objects.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
    
    # Print the loaded object (optional)
    print(json.dumps(data_objects[2], ensure_ascii=False, indent=4))
else:
    print("No JSON files found in the directory.")