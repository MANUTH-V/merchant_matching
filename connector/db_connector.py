# from lib.dremioAPI import dreamioAPI
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

import os
from dotenv import load_dotenv
load_dotenv()

driver = '{ODBC Driver 17 for SQL Server}'
host = os.environ.get('HOST_SERVER')
db = os.environ.get('DATABASE')
user_name = os.environ.get('USER')
password = os.environ.get('PW')


connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=visa_vca;UID=visal;PWD=Visal@123"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

uat_connection_string = f"DRIVER={driver};SERVER={host}; DATABASE={db}; UID={user_name};PWD={password}"
uat_connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": uat_connection_string})

def getEngine():    
    return create_engine(connection_url)

def getFastEngine():    
    return create_engine(connection_url,fast_executemany=True, connect_args={'connect_timeout': 10}, echo=False)

def getFastEngineUAT():    
    return create_engine(uat_connection_url,fast_executemany=True, connect_args={'connect_timeout': 10}, echo=False)
