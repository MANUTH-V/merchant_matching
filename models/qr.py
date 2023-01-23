import os
from db import DB
import pandas as pd

QR_TABLE_NAME = "qr_transaction"

class QR():
    merchant_name: str
    bank_name: str

    def __init__(self, merchant_name: str, bank_name: str):
        self.merchant_name = merchant_name
        self.bank_name = bank_name

    @staticmethod
    def create_table():
        DB.execute(
            f"CREATE TABLE IF NOT EXISTS {QR_TABLE_NAME} (merchant_name VARCHAR(255), bank_name VARCHAR(255))"
        )

    @staticmethod
    def new_data_exists():
        # check if cached_qr.csv exists
        if not os.path.exists("data/cached_qr.csv"):
            return True

        # check if new data exists
        DB.execute(f"SELECT COUNT(*) FROM {QR_TABLE_NAME}")
        count = DB.fetchone()[0]

        cached_data = pd.read_csv("data/cached_qr.csv")
        old_count = cached_data.shape[0]

        if count != old_count:
            return True
        else:
            return False

    @staticmethod
    def find_all():
        DB.execute(f"SELECT merchant_name, bank_name FROM {QR_TABLE_NAME}")
        records = DB.fetchall()

        data = []
        for record in records:
            qr = QR(
                merchant_name=record[0],
                bank_name=record[1],
            )
            data.append(qr)

        return data
    
    @staticmethod
    def save_csv(file_path: str = "qr.csv"):
        with open(file_path, "w") as f:
                f.write("merchant_name, bank_name\n")

        with open(file_path, "+a") as f:
            for qr in QR.find_all():
                f.write(f"{qr.merchant_name}, {qr.bank_name}\n")

        # check if cached_qr.csv not exists
        if not os.path.exists("data/cached_qr.csv"):
            # save cached file
            with open("data/cached_qr.csv", "w") as f:
                f.write("merchant_name, bank_name\n")

            with open("data/cached_qr.csv", "+a") as f:
                for qr in QR.find_all():
                    f.write(f"{qr.merchant_name}, {qr.bank_name}\n")
        else:
            # compare cached data
            cached_data = pd.read_csv("data/cached_qr.csv")
            new_data = pd.read_csv(file_path)

            # get the difference between cached and new data
            diff = new_data.merge(cached_data, indicator=True, how='outer').loc[lambda x : x['_merge']=='left_only']

            # remove _merge column
            diff = diff.drop(columns=["_merge"])

            # save the difference to qr.csv
            diff.to_csv(file_path, index=False)

            # append the difference to cached_qr.csv
            diff.to_csv("data/cached_qr.csv", mode="a", index=False, header=False)

    @staticmethod
    def save_db(file_path: str):
        # check if table exists
        QR.create_table()

        # read file
        qr = pd.read_csv(file_path)

        # save to db
        for index, row in qr.iterrows():
            merchant_name = row["merchant_name"]
            bank_name = row["bank_name"]
            DB.execute(
                f"INSERT INTO {QR_TABLE_NAME} (merchant_name, bank_name) VALUES (%s, %s)",
                (merchant_name, bank_name)
            )