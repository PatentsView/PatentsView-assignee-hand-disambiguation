from sqlalchemy import *
from dotenv import dotenv_values

config = dotenv_values(".env")
engine = create_engine(f"mysql+pymysql://{config['user']}:{config['password']}@{config['hostname']}/{config['dbname']}?charset=utf8mb4")

with engine.connect() as connection:
    tables = connection.execute(text("show tables")).fetchall()
    for table in tables:
        print(table[0])