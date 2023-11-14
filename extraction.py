from sqlalchemy import create_engine, text
from dotenv import dotenv_values
import pandas as pd
import numpy as np

ASSIGNEE_TYPE_DICT = {
    1: "Unassigned",
    2: "United States company or corporation",
    3: "Foreign company or corporation",
    4: "United States individual",
    5: "Foreign individual",
    6: "U.S. Federal government",
    7: "Foreign government",
    8: "U.S. county government",
    9: "U.S. state government"
}

def get_engine():
    config = dotenv_values(".env")
    user = config['user']
    pswd = config['password']
    hostname = config['hostname']
    dbname = config['dbname']
    engine = create_engine(f"mysql+pymysql://{user}:{pswd}@{hostname}/{dbname}?charset=utf8mb4")
    return engine

def run_extraction(assignee_IDs=["160cad21-ac45-48a2-86db-3c935d5e53ce"], output_path=None, simplified=True):
    engine = get_engine()
    assignee_IDs_SQL = ', '.join("'" + item + "'" for item in assignee_IDs)
    with engine.connect() as connection:
        query = f"SELECT * FROM rawassignee_for_hand_labeling r WHERE r.assignee IN ({assignee_IDs_SQL})"
        result = connection.execute(text(query)).fetchall()

    df = pd.DataFrame(result)
    df['assignee_type'] = df['assignee_type'].apply(lambda x: ASSIGNEE_TYPE_DICT[x])
    df = df[['patent_id', 'assignee_sequence', 'patent_title', 'patent_abstract', 'patent_date', 'patent_type',\
        'assignee', 'assignee_type', 'assignee_individual_name_first', 'asassignee_individual_name_last',\
        'assignee_organization', 'assignee_city', 'assignee_state', 'assignee_country']]
    
    df.to_csv(output_path)

if __name__ == "__main__":
    # disamb_IDs = np.loadtxt('data/05 - extraction/US5031150-0.txt', dtype="str").tolist()
    disamb_IDs = ['9e98dbf5-7cc3-42fe-a6ad-8cd62eda7372']
    run_extraction(assignee_IDs=disamb_IDs, output_path="data/05 - extraction/test_output.csv", simplified=True)