import pandas as pd
import sys
import os
import openpyxl

# Read user input
INPUT_DIR = "data/06 - compare/"
OUTPUT_DIR = "data/07 - evaluation/"
MENTION_ID = sys.argv[1]
labeler1 = sys.argv[2]
labeler2 = sys.argv[3]

# Load in user data
f1_path = os.path.join(INPUT_DIR, MENTION_ID + "-" + labeler1 + ".csv")
f2_path = os.path.join(INPUT_DIR, MENTION_ID + "-" + labeler2 + ".csv")
file1 = pd.read_csv(f1_path, index_col=0, dtype={'patent_id': str, 'assignee_state': object})
file2 = pd.read_csv(f2_path, index_col=0, dtype={'patent_id': str, 'assignee_state': object})

# Add a column to indicate the labeler
merge_cols = list(file1.columns)
file1[labeler1] = True
file2[labeler2] = True

# Merge the two DataFrames using an outer join on all columns
merged = pd.merge(file1, file2, on=merge_cols, how='outer')
merged[labeler1].fillna(False, inplace=True)
merged[labeler2].fillna(False, inplace=True)

# Filter rows where both 'File1' and 'File2' are False
# result = merged[(merged['File1'] == False) & (merged['File2'] == False)]
output_path = os.path.join(OUTPUT_DIR, MENTION_ID + "-difference.csv")
merged.to_csv(output_path, index=False)