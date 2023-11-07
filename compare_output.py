import pandas as pd

DATA_DIR = "data/06 - review/"
MENTION_ID = "US5031150-0"
file1 = pd.read_csv(DATA_DIR + MENTION_ID + '.csv', index_col=0)
file2 = pd.read_csv(DATA_DIR + MENTION_ID + '-original.csv', index_col=0)

print("File1 length:", len(file1.index))
print("File2 length:", len(file2.index))

# Add a boolean column to indicate the source file
file1['File1'] = True
file2['File2'] = True

# Merge the two DataFrames using an outer join on all columns
merged = pd.merge(file1, file2, on=['patent_id', 'assignee_sequence'], how='outer')

# Fill NaN values in the 'File1' and 'File2' columns with False
merged['File1'].fillna(False, inplace=True)
merged['File2'].fillna(False, inplace=True)

# Filter rows where both 'File1' and 'File2' are False
result = merged[(merged['File1'] == False) & (merged['File2'] == False)]

# Save the result to a new CSV file
result.to_csv(DATA_DIR + MENTION_ID + '-difference.csv', index=False)
