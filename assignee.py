from sqlalchemy import *
from dotenv import dotenv_values

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
    pass

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
def disambiguated_assignees_data(assignee_disamiguation_IDs: list[str], connection):
    pass

config = dotenv_values(".env")
engine = create_engine(f"mysql+pymysql://{config['user']}:{config['password']}@{config['hostname']}/{config['dbname']}?charset=utf8mb4")

with engine.connect() as connection:
    mention_id = "US7315019-0"
    row = assignee_data(mention_id, connection)
    print(row)