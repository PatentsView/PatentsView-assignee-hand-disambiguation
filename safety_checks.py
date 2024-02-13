import os
import pandas as pd
import pickle
from tqdm import tqdm
import numpy as np

LOG_FP = "data/error_log.txt"
def log(filename, new_line):
    try:
        # Open the file in append mode
        with open(filename, 'a') as file:
            file.write(new_line + '\n')
    except Exception as e:
        log(LOG_FP, f"ERROR WHILE LOGGING: {e}")

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
    try:
        file_path = os.path.join(directory_path, csv_file)
        df = pd.read_csv(file_path, dtype={'patent_id': str, 'assignee_sequence': str})
        patent_ids = set("US" + df['patent_id'] + "-" + df['assignee_sequence']) # Extract the patent_id column and add it to the all_patent_ids set
        all_patent_ids.update(patent_ids)

        # Get seed patent related info
        seed_patent = csv_file[:-4]
        patent_id = seed_patent[2:seed_patent.find('-')]
        assignee_sequence = seed_patent[seed_patent.find('-')+1:]
        seed_rows = df[np.logical_and(df['patent_id'] == patent_id, df['assignee_sequence'] == assignee_sequence)]
        seed_info = seed_rows.iloc[0].to_dict()

        # Ensure seed patent is contained in file
        if len(seed_rows) == 0:
            log(LOG_FP, f"CHECK: {csv_file} does not contain seed patent.")
            continue

        # Ensure organization/last_name first letter matches
        key = "assignee_organization" if not pd.isna(seed_info['assignee_organization']) else "assignee_individual_name_first"
        if not (df[key].str[0].str.lower() == seed_info['assignee_organization'][0].lower()).all():
            log(LOG_FP, f"CHECK: {csv_file} {key} do not all start with the same character.")
            continue

    except Exception as e:
        log(LOG_FP, f"ERROR WHILE CHECKING: {e}")

# Step 4: Save the all_patent_ids set to the pickle file
with open(pickle_file_path, 'wb') as pickle_file:
    pickle.dump(all_patent_ids, pickle_file)