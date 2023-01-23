import os
from db import DB
import pandas as pd

PW_TABLE_NAME = "VW_PW_DWH_MERCHANTS"

class PW():
    merchant_name: str
    channel: str
    merchant_id: str

    def __init__(self, merchant_name: str, channel: str, merchant_id: str):
        self.merchant_name = merchant_name
        self.channel = channel
        self.merchant_id = merchant_id

    @staticmethod
    def create_table():
        DB.execute(
            f"CREATE TABLE IF NOT EXISTS {PW_TABLE_NAME} (merchant_name VARCHAR(255), channel VARCHAR(255), merchant_id VARCHAR(255))"
        )
    
    @staticmethod
    def find_all():
        DB.execute(
            f"SELECT merchant_name, channel, merchant_id FROM {PW_TABLE_NAME}"
        )
        records = DB.fetchall()

        data = []
        for record in records:
            pw = PW(
                merchant_name=record[0],
                channel=record[1],
                merchant_id=record[2],
            )
            data.append(pw)

        return data

    @staticmethod
    def save_csv(file_path: str = "pw.csv"):
        with open(file_path, "w") as f:
            f.write("merchant_name, channel, merchant_id\n")

        with open(file_path, "+a") as f:
            for pw in PW.find_all():
                f.write(f"{pw.merchant_name}, {pw.channel}, {pw.merchant_id}\n")

        # save count to file
        with open("/configs/pw_count.conf", "w") as f:
            f.write(str(len(PW.find_all())))

    @staticmethod
    def save_db(file_path: str):
        # check if table exists
        PW.create_table()

        # read file
        pw = pd.read_csv(file_path)

        # save to db with character encoding utf-8
        for index, row in pw.iterrows():
            merchant_name = row["merchant_name"]
            channel = row["channel"]
            merchant_id = row["merchant_id"]

            DB.execute(
                f"INSERT INTO {PW_TABLE_NAME} (merchant_name, channel, merchant_id) VALUES (%s, %s, %s)",
                (merchant_name, channel, merchant_id),
            )
            