from sqlalchemy import *
from dotenv import dotenv_values
import pandas as pd
import os
import pickle

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
def populate_sample(sample, output_path, connection):
    # Load sample data and previously populated
    total = len(sample)
    prev_df = pd.read_csv(output_path) if os.path.exists(output_path) else pd.DataFrame()
    df_list = [prev_df]
    populated = ["US" + str(row.patent_id) + "-" + str(row.assignee_sequence) for index, row in prev_df.iterrows()]

    for mention_id in sample:
        # Populate mention_id and save data
        temp_df = assignee_data(mention_id, connection)
        df_list.append(temp_df)
        populated.append(mention_id)

        # Output messages
        percent = str(round(100 * len(populated) / total, 1)) + "%"
        print(percent, "- created row for", mention_id)

        # Store dataframe intermitently
        if len(populated) % 20 == 0:
            df = pd.concat(df_list, axis=0, ignore_index=True)
            df.to_csv('output.csv')
        
    # Save final dataframe
    df = pd.concat(df_list, axis=0, ignore_index=True)
    df.to_csv('output.csv')

def main():
    # Establishing SQL connection
    config = dotenv_values(".env")
    engine = create_engine(f"mysql+pymysql://{config['user']}:{config['password']}@{config['hostname']}/{config['dbname']}?charset=utf8mb4")

    # Calling first method
    with engine.connect() as connection:
        sample = pd.read_csv('data/sample.csv')['0'].tolist()
        populate_sample(sample, 'data/output.csv', connection)
        # ids = ['8314bfc2-8005-4202-ae98-63c90a4e245e']
        # df = disambiguated_assignees_data(ids, connection)
        # df.to_csv('output.csv')

if __name__ == "__main__":
    main()