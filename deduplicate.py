import pandas as pd
import sys
import os

# Read user input
INPUT_DIR = "/Users/sengineer/Library/CloudStorage/OneDrive-AIR/Week 2 Dropbox/"
OUTPUT_DIR = "data/08 - deduplication/"
MENTION_ID_1 = sys.argv[1]
MENTION_ID_2 = sys.argv[2]

# Load in user data
f1_path = os.path.join(INPUT_DIR, MENTION_ID_1 + ".csv")
f2_path = os.path.join(INPUT_DIR, MENTION_ID_2 + ".csv")
file1 = pd.read_csv(f1_path, index_col=0, dtype={'patent_id': str, 'assignee_state': object})
file2 = pd.read_csv(f2_path, index_col=0, dtype={'patent_id': str, 'assignee_state': object})
print("Done reading the files.")

"""
# Add a column to indicate the labeler
merge_cols = list(file1.columns)
file1[labeler1] = True
file2[labeler2] = True

# Merge the two DataFrames using an outer join on all columns
merged = pd.merge(file1, file2, on=merge_cols, how='outer')
merged[labeler1].fillna(False, inplace=True)
merged[labeler2].fillna(False, inplace=True)
"""

# Merge not working properly, using set operations instead
file1_patents = set(file1.patent_id) - set(file2.patent_id)
file2_patents = set(file2.patent_id) - set(file1.patent_id)
file1_diff = file1[file1.patent_id.isin(file1_patents)].copy()
file2_diff = file2[file2.patent_id.isin(file2_patents)].copy()

# Indicate origin file
file1_diff["Origin"] = "1"
file2_diff["Origin"] = "2"
file1_diff["Mention ID"] = MENTION_ID_1
file2_diff["Mention ID"] = MENTION_ID_2
merged = pd.concat([file1_diff, file2_diff])
print("Done merging the files.")

# Filter rows where both 'File1' and 'File2' are False
output_path = os.path.join(OUTPUT_DIR, MENTION_ID_1 + "_" + MENTION_ID_2 + ".csv")
merged.to_csv(output_path, index=False)
print("Successfully saved the output file.")

# Code used for when two mention IDs belong to different clusters
# intersection_patents = set(file1.patent_id).intersection(set(file2.patent_id))
# print(intersection_patents)
# intersection = file1[file1.patent_id.isin(intersection_patents)].copy()
# output_path = os.path.join(OUTPUT_DIR, MENTION_ID_1 + "_" + MENTION_ID_2 + ".csv")
# intersection.to_csv(output_path, index=False)