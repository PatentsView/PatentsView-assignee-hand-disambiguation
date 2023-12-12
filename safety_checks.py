import os
import pandas as pd
import pickle
from tqdm import tqdm

# Step 1: Get a list of CSV files in the given directory
directory_path = "/Users/sengineer/Library/CloudStorage/OneDrive-AIR/Week 2 Dropbox/"
csv_files = [file for file in os.listdir(directory_path) if file.endswith(".csv")]

# Step 2: Initialize the set variable all_patent_ids
all_patent_ids = set()
pickle_file_path = "data/all_patent_ids.pkl"
if os.path.exists(pickle_file_path):
    with open(pickle_file_path, 'rb') as pickle_file: # Load data from the pickle file
        all_patent_ids = pickle.load(pickle_file)

# Step 3: Loop through each CSV file, load it with pandas, and extract patent_id
for csv_file in tqdm(csv_files, desc="Looping through files"):
    file_path = os.path.join(directory_path, csv_file)
    df = pd.read_csv(file_path, dtype={'patent_id': str, 'assignee_sequence': str})
    patent_ids = set("US" + df['patent_id'] + "-" + df['assignee_sequence']) # Extract the patent_id column and add it to the all_patent_ids set
    all_patent_ids.update(patent_ids)

    # Output error message if CSV file doesn't contain seed patent
    seed_patent = csv_file[:-4]
    if seed_patent not in patent_ids:
        print("ERROR:", csv_file, "does not contain seed patent.")

    # TODO - Add other safety checks

# Step 4: Save the all_patent_ids set to the pickle file
with open(pickle_file_path, 'wb') as pickle_file:
    pickle.dump(all_patent_ids, pickle_file)