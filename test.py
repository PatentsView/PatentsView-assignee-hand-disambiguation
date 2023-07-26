from sqlalchemy import *
from dotenv import dotenv_values
import pandas as pd

config = dotenv_values(".env")
engine = create_engine(f"mysql+pymysql://{config['user']}:{config['password']}@{config['hostname']}/{config['dbname']}?charset=utf8mb4")

with open('query2.sql', "r") as f:
    query = f.read()

with engine.connect() as connection:
    result = connection.execute(text(query))

    df = pd.DataFrame(result)
    df.to_csv('output.csv')