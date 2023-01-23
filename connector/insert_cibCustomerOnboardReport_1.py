import warnings
warnings.filterwarnings("ignore")
import sys
sys.path += ['./', '../']
import os
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from modules.telegram_chat_bot import send_msg
from datetime import datetime
from sqlalchemy.engine import URL
from sqlalchemy import create_engine


driver = '{ODBC Driver 17 for SQL Server}'
host = os.environ.get('HOST_SERVER')
db = os.environ.get('DATABASE')
user_name = os.environ.get('USER')
password = os.environ.get('PW')


uat_connection_string = f"DRIVER={driver};SERVER={host}; DATABASE={db}; UID={user_name};PWD={password}"
uat_connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": uat_connection_string})

def getFastEngineUAT():    
    return create_engine(uat_connection_url,fast_executemany=True, connect_args={'connect_timeout': 10}, echo=False)


# data source
src_db = os.environ.get('SOURCE_DATABASE')
src_schema = os.environ.get('SOURCE_SCHEMA')
src_table = os.environ.get('SOURCE_TABLE_1')
full_src = src_db + '.' + src_schema + '.' + src_table

# data destination
dst_db = os.environ.get('DESTINATION_DATABASE')
dst_schema = os.environ.get('DESTINATION_SCHEMA')
dst_table = os.environ.get('DESTINATION_TABLE_1')
full_dst = dst_db + '.' + dst_schema + '.' + dst_table

exc_dt = os.environ.get('EXECUTION_DATE_1')

host = os.environ.get('HOST_SERVER')

sql = text(f"""INSERT INTO
                {full_dst}
                SELECT
                    REFERENCE as REFERENCE_ID,
                    CIF,
                    COPORATE_CUSTOMER_NAME,
                    REQUEST_TYPE,
                    '' CHANNEL,
                    STATUS,
                    CAST(CAST(REQUEST_DATE as datetimeoffset) as DATETIME) AS REQUEST_DATE,
                    CAST(CAST(APPROVE_DATE as datetimeoffset) as DATETIME) AS APPROVE_DATE,
                    PHONE,MOBILE,EMAIL_ADDRESS,REQUESTER,CHECKER,CHECKER_COMMENT,
                    GETDATE() AS import_date
                FROM 
                {full_src}
                WHERE STATUS = 'Accepted' 
                    AND CAST(APPROVE_DATE as date) = {exc_dt} """)

def chat_bot_temp(num_record):
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

    msg = f"""<b>Schedule                          : everyday at  <code>5:35PM</code></b>
<b>Actual run@                   : <code>{date_time}</code></b>
<b>Apply to database      : <code>{dst_db}</code> (HOST: <code>{host}</code>)</b>
<b>Insert into table           : <code>{dst_table}</code> with <code>{num_record}</code> records</b>"""
    send_msg(msg)


from prefect import task, flow


@task()
def create_conn():
    return getFastEngineUAT()


@task
def exe_query(engine):
    engine.execute(sql)
    num_record_query = f"""SELECT COUNT(*) 
                            FROM {full_dst} 
                            WHERE STATUS = 'Accepted'
                                    AND CAST([APPROVE DATE] AS DATE) = {exc_dt}
                        """
    num_record = engine.execute(num_record_query).fetchall()[0][0]    

    return num_record

@task
def msg_bot(num_record):
    # send msg to telegram bot
    chat_bot_temp(num_record)           


@flow()
def NewCusDB_cibCustomerOnboardReport():

    engine = create_conn()

    num_record = exe_query(engine)

    msg_bot(num_record)

  
# NewCusDB_cibCustomerOnboardReport()


    