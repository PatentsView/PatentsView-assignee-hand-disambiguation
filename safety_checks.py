import os
import pandas as pd
import pickle
from tqdm import tqdm
import numpy as np
<<<<<<< Updated upstream

LOG_FP = "data/error_log.txt"
def log(filename, new_line):
    try:
        # Open the file in append mode
        with open(filename, 'a') as file:
            file.write(new_line + '\n')
    except Exception as e:
        log(LOG_FP, f"ERROR WHILE LOGGING: {e}")
=======
import json

LOG_FP = "data/error_log.csv"
ERROR_LOG = []
def log(message, message_type, file, desc, data):
    try:
        ERROR_LOG.append({"message": message, "message_type": message_type, "file": file, "desc": desc, "data": data})
    except Exception as e:
        log("ERROR", "LOGGING", None, e, None)
>>>>>>> Stashed changes

# Step 1: Get a list of CSV files in the given directory
directory_path = "data/Week 2 Dropbox/"
csv_files = [file for file in os.listdir(directory_path) if file.endswith(".csv")]

# Step 2: Initialize the set variable all_patent_ids
all_patent_ids = set()
pickle_file_path = "data/all_patent_ids.pkl"

# Step 3: Loop through each CSV file, load it with pandas, and extract patent_id
for csv_file in tqdm(csv_files, desc="Looping through files"):
    try:
<<<<<<< Updated upstream
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
=======
        # Extract patent_ids from new file
        file_path = os.path.join(directory_path, csv_file)
        df = pd.read_csv(file_path, dtype={'patent_id': str, 'assignee_sequence': str})
        patent_ids = set("US" + df['patent_id'] + "-" + df['assignee_sequence']) # Extract the patent_id column and add it to the all_patent_ids set
        intersection = patent_ids.intersection(all_patent_ids)

        # Ensure patent_ids have not been seen before by another disambiguation file
        if len(patent_ids.intersection(all_patent_ids)) > 0: 
            log("CHECK", "DUPLICATE", csv_file, "Contains one or more patent_id's which belong to another file", intersection)
            continue
        all_patent_ids.update(patent_ids)

        # Get seed patent related info
        seed_patent = csv_file[:-4]
        patent_id = seed_patent[2:seed_patent.find('-')]
        assignee_sequence = seed_patent[seed_patent.find('-')+1:]
        seed_rows = df[np.logical_and(df['patent_id'] == patent_id, df['assignee_sequence'] == assignee_sequence)]
        seed_info = seed_rows.iloc[0].to_dict()

        # Ensure seed patent is contained in file
        if len(seed_rows) == 0:
            log("CHECK", "SEED", csv_file, "File does not contain seed patent", None)
            continue
        
        # Get info on the assignee type
        key = "assignee_organization" if not pd.isna(seed_info['assignee_organization']) else "assignee_individual_name_first"
        expected_first_letter = seed_info['assignee_organization'][0].lower()
        actual_first_letters = df[key].str[0].str.lower()

        # Ensure organization/last_name first letter matches
        if not (actual_first_letters == expected_first_letter).all():
            # Output message
            expected_key = seed_info['assignee_organization']
            wrong_keys = df[key][actual_first_letters != expected_first_letter].tolist()
            log("CHECK", "FIRST CHAR", csv_file, f"{expected_key} is the {key} which has mismatching first characters", json.dumps(wrong_keys))
            continue

    except Exception as e:
        log("ERROR", "CHECKING", csv_file, e, None)
>>>>>>> Stashed changes

# Step 4: Save the all_patent_ids set to the pickle file
with open(pickle_file_path, 'wb') as pickle_file:
    pickle.dump(all_patent_ids, pickle_file)

# Step 5: Save error log
pd.DataFrame(ERROR_LOG).to_csv(LOG_FP)