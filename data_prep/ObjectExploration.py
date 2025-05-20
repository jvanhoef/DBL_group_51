import os
import json

# Use the same file path definition as in loadingData.py
script_directory = os.path.dirname(__file__)
data_directory = os.path.join(script_directory, '..', 'data')

# Pick a file from the directory (for example, the first .json file)
files = [f for f in os.listdir(data_directory) if f.endswith('.json')]
if files:
    json_path = os.path.join(data_directory, files[0])
    with open(json_path, 'r', encoding='utf8') as file:
        for line in file:
            try:
                data = json.loads(line)
                # print(data)
                # break
                poll = data.get('entities', {}).get('polls', [])
                if poll:
                    print(f"Tweet ID {data.get('id')} has poll: {poll}")
                    break  # Remove this break if you want to check all tweets
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")