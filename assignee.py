from sqlalchemy import *
from dotenv import dotenv_values
import pandas as pd
import numpy as np
import os

"""
Establish MySQL connection with environmental variables

Returns
-------
connection : a PyMySQL connection object
"""
def establish_connection():
    config = dotenv_values(".env")
    user = config['user']
    pswd = config['password']
    hostname = config['hostname']
    dbname = config['dbname']
    connection = create_engine(f"mysql+pymysql://{user}:{pswd}@{hostname}/{dbname}?charset=utf8mb4")
    return connection

"""
Parameters
----------
mention_id: string
    Mention ID of an assignee, of the form US<patent_id>-<sequence_number>, such as "US7315019-0" for the first assignee (Pacific Biosciences) of patent US7315019.
Connection: sqlalchemy.engine.base.Connection
    Connection using sqlalchemy to the PV database.

Returns
-------
Pandas Dataframe with a single row, with columns for the (non-disambiguated version of):
    - URL to the PatentsView website page for the corresponding patent
    - Assignee name
    - Assignee location
    - Co-assignees
    - Patent title
    - Patent classification codes
    - Patent date filed
    - Patent date granted
    - Patent type
    - Patent inventors
    - Inventor locations
    - Any other piece of relevant information for assignee disambiguation?
"""
def assignee_data(mention_id: str, connection):
    # Getting patent information
    split = mention_id.split("-")
    patent_id = split[0][2:]
    sequence = split[1]
    # Running query on algorithms_assignee_labeling view
    query = f"SELECT * FROM algorithms_assignee_labeling.assignee WHERE patent_id='{patent_id}' and assignee_sequence='{sequence}'"
    result = connection.execute(text(query)).fetchall()
    df = pd.DataFrame(result)
    return df
    # TODO - integrate expand_rows()

"""
Parameters
----------
row: pandas dataframe row

Returns
-------
Row with merged cells for CPC subgroup and inventors such that each mention_id
    has one embedded row for each CPC category and one embedded for for each inventor
"""
def expand_row(row):
    return None # TODO

"""
Parameters
----------

Pulls a random sample of assignee mention_id's from AWS and saves it as an output CSV file
"""
def sample(size=10000):
    np.random.seed(0)
    disamb = pd.read_csv(
        "https://s3.amazonaws.com/data.patentsview.org/download/g_persistent_assignee.tsv.zip",
        dtype=str,
        sep="\t",
        compression="zip")

    disamb["mention_id"] = "US" + disamb["patent_id"] + "-" + disamb["assignee_sequence"]
    disamb_20220929 = disamb.set_index("mention_id")["disamb_assignee_id_20220929"]
    disamb_20220929 = disamb_20220929.dropna()
    mention_ids = disamb_20220929.index

    sample = np.random.choice(mention_ids, size=10000, replace=False)
    pd.Series(sample).to_csv("data/sample.csv")

"""
Parameters
----------
Sample: list[str]
    List of mention IDs (`mention_id` field values)
Output Path: str
    Path to CSV file for saving populated data
Connection: sqlalchemy.engine.base.Connection
    Connection using sqlalchemy to the PV database.

Output
-------
Saves data to `output_path` which is a dataframe with one row for every element in `sample`
Each row has all the attributes in assignee_data()
"""
def populate_sample(sample_path, output_path, connection):
    # Load sample data and determine which are previously populated
    sample = pd.read_csv(sample_path)['0'].tolist()
    prev_df = pd.read_csv(output_path) if os.path.exists(output_path) else pd.DataFrame()
    df_list = [prev_df]
    temp_list = []
    populated = ["US" + str(row.patent_id) + "-" + str(row.assignee_sequence) for index, row in prev_df.iterrows()]

    for mention_id in sample:
        if mention_id in populated:
            continue

        # Populate mention_id and save data
        temp_df = assignee_data(mention_id, connection)
        df_list.append(temp_df)
        populated.append(mention_id)

        # Output messages
        percent = str(round(100 * len(populated) / len(sample), 1)) + "%"
        print(percent, "- created row for", mention_id)

        # Store dataframe intermitently
        if len(populated) % 20 == 0:
            temp_df = pd.concat(temp_list, axis=0, ignore_index=True)
            df_list.append(temp_df)
            pd.concat(df_list, axis=0, ignore_index=True).to_csv(output_path)
            temp_list = []
        
    # Save final dataframe
    df = pd.concat(df_list, axis=0, ignore_index=True).to_csv(output_path)

"""
Parameters
----------
assignee_disamiguation_IDs: list[str]
    List of disambiguated assignee IDs (`assignee_id` field values)
Connection: sqlalchemy.engine.base.Connection
    Connection using sqlalchemy to the PV database.

Returns
-------
Pandas Dataframe with rows for all assignee mentions that correspond to one disambiguated assignee ID in the provided list. 
The columns should be the same as in the `assignee_data` function.
Specifically, rows should be of the form `assignee_data(mention_id)` for each mention_id that corresponds to one of the disambiguated assignee ID in the provided list.
"""
def disambiguated_assignees_data(assignee_disambiguation_IDs: list[str], connection):
    id_list = '("' + '","'.join(assignee_disambiguation_IDs) + '")'
    query = f"SELECT * FROM algorithms_assignee_labeling.assignee a WHERE a.disambiguated_assignee_id IN {id_list}"
    result = connection.execute(text(query)).fetchall()
    df = pd.DataFrame(result)
    return df
    # TODO - move mention_id row to top
    # TODO - integrate expand_rows()

def main():
    engine = establish_connection()
    
    # Calling first method
    with engine.connect() as connection:
        # Generates data for sample
        
        populate_sample('data/sample.csv', 'data/samples_with_data.csv', connection)
        # ids = ['717e8394-c25d-4ea6-b444-09d6036a4cde']
        # df = disambiguated_assignees_data(ids, connection)
        # df.to_csv('output.csv')

if __name__ == "__main__":
    main()