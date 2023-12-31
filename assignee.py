from sqlalchemy import create_engine, text
from dotenv import dotenv_values
import pandas as pd
import numpy as np
import os


"""
Establish MySQL connection with environmental variables

Returns
-------
engine : a PyMySQL engine object
"""
def establish_connection():
    config = dotenv_values(".env")
    user = config['user']
    pswd = config['password']
    hostname = config['hostname']
    dbname = config['dbname']
    engine = create_engine(f"mysql+pymysql://{user}:{pswd}@{hostname}/{dbname}?charset=utf8mb4")
    return engine


"""
Parameters
----------
mention_id : string
    Mention ID of an assignee, of the form US<patent_id>-<sequence_number>, such as "US7315019-0" for the first 
    assignee (Pacific Biosciences) of patent US7315019.
patent_only : Boolean
    If true, only include patent specific information (returns one row). If false, include patent information, 
    CPC categories, and inventor information (returns multiple rows).
connection : sqlalchemy.engine.base.Connection
    Connection using sqlalchemy to the PV database.

Returns
-------
Pandas Dataframe, with columns for (the non-disambiguated version of):
    - URL to the PatentsView website page for the corresponding patent
    - Assignee name
    - Assignee location
    - Patent title
    - Patent classification codes
    - Patent date filed
    - Patent date granted
    - Patent type
    - Patent inventors
"""
def assignee_data(mention_id: str, patent_only: bool, connection):
    # Getting patent information
    split = mention_id.split("-")
    patent_id = split[0][2:]
    sequence = split[1]

    # Running query on algorithms_assignee_labeling view
    if patent_only:
        query = f"SELECT patent_id, assignee_sequence, organization, name_first, name_last, assignee_type, title, patent_date,\
            assignee_country, assignee_city, assignee_state FROM algorithms_assignee_labeling.assignee WHERE\
            patent_id='{patent_id}' and assignee_sequence='{sequence}'"
    else:
        query = (f"SELECT * FROM algorithms_assignee_labeling.assignee WHERE patent_id='{patent_id}' and "
                 f"assignee_sequence='{sequence}'")
    result = connection.execute(text(query)).fetchall()

    # Return pandas df
    df = pd.DataFrame(result).drop_duplicates()
    return df


"""
Parameters
----------
size : int
    Number of samples

Pulls a random sample of assignee mention_id's from AWS and saves it as an output CSV file
"""
def sample_mentions(size=800, output_dir="data/", seed=20231025):
    # Load in data
    disamb = pd.read_csv(
        "g_persistent_assignee.tsv.zip",
        dtype=str,
        sep="\t",
        compression="zip")

    # Clean data
    disamb["mention_id"] = "US" + disamb["patent_id"] + "-" + disamb["assignee_sequence"]
    disamb_20230629 = disamb[["mention_id", "disamb_assignee_id_20230629"]]
    disamb_20230629 = disamb_20230629.dropna()

    # Sample and extract cluster sizes
    sampled_df = disamb_20230629.sample(n=size, random_state=seed)
    cluster_size_lookup = disamb_20230629["disamb_assignee_id_20230629"].value_counts()
    sampled_df["cluster_size"] = [cluster_size_lookup[disamb_id] for disamb_id in sampled_df["disamb_assignee_id_20230629"]]

    # Save output
    np.savetxt(os.path.join(output_dir, "01 - sample.txt"), sampled_df["mention_id"].values, fmt="%s")
    sampled_df[["mention_id", "cluster_size"]].to_csv(os.path.join(output_dir, "01 - sample_with_cluster_size.csv"), index=False)


"""
Parameters
----------
connection : sqlalchemy.engine.base.Connection
    Connection using sqlalchemy to the PV database.
sample_path : str
    Path to CSV file containing a list of mention IDs (`mention_id` field values)
output_path : str
    Path to CSV file for saving populated data. This method both reads and writes for intermitent progress

Output
------
Saves data to `output_path` which is a dataframe with one row for every element in `sample`
Each row has all the attributes in assignee_data()
"""
def populate_sample(connection, sample_path="data/01 - sample.txt", output_path="data/02 - sample_with_data.csv"):
    # Load sample data and determine which are previously populated
    sample = np.loadtxt(sample_path, dtype=str)
    prev_df = pd.read_csv(output_path) if os.path.exists(output_path) else pd.DataFrame()
    df_list = [prev_df]
    populated = ["US" + str(row.patent_id) + "-" + str(row.assignee_sequence) for index, row in prev_df.iterrows()]

    for mention_id in sample:
        if mention_id in populated:
            continue

        # Populate mention_id and save data
        temp_df = assignee_data(mention_id, True, connection)
        df_list.append(temp_df)
        populated.append(mention_id)

        # Output messages
        percent = str(round(100 * len(populated) / len(sample), 1)) + "%"
        print(percent, "- created row for", mention_id)

        # Store dataframe intermitently
        if len(populated) % 20 == 0:
            pd.concat(df_list, axis=0, ignore_index=True).to_csv(output_path)

    # Save final dataframe
    pd.concat(df_list, axis=0, ignore_index=True).to_csv(output_path)


"""
Parameters
----------
n (int) : Number of hand labelers
sample_path (str) : path to CSV file containing samples with data
output_path (str) : folder to save n different dataframes

Output
------
Folder with n different files, each an equally sized partition of sample_path
"""
def segment_sample(n=3, sample_path="data/02 - sample_with_data.csv", output_folder="data/03 - segmented samples/"):
    # Load input data and create output folder
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    samples = pd.read_csv(sample_path, index_col=0)
    sample_count = len(samples.index)

    # Values to increment
    increment_size = sample_count // n
    start = 0
    end = increment_size

    # Loop through each hand labeler
    for i in range(n):
        end = end if i in range(sample_count % n) else end - 1 # Handle uneven distribution

        # Save each data partition
        output_path = os.path.join(output_folder, str(i) + " - hand_labeler_file.csv")
        samples.loc[start:end].to_csv(output_path)

        # Increment values for next iteration
        start = end + 1
        end = start + increment_size


def main():
    engine = establish_connection()
    with engine.connect() as connection:
        populate_sample(connection)
    # ids = ["a0ba1f5c-6e5f-4f62-b309-22bd81c8b043", "e1d5391e-94c9-4ced-843b-6992e29b6fee", "8f703249-da60-44ea-a257-fb6a07b08f50"]

if __name__ == "__main__":
    main()