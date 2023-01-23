from db import DB, DB_ENGINE
import pandas as pd

class Result():
    merchant_name: str
    aba_merchat_name_matched: str
    aba_merchat_score_matched: float
    aba_merchat_id_matched: int

    def __init__(self, merchant_name: str, aba_merchat_name_matched: str, aba_merchat_score_matched: float, aba_merchat_id_matched: int):
        self.merchant_name = merchant_name
        self.aba_merchat_name_matched = aba_merchat_name_matched
        self.aba_merchat_score_matched = aba_merchat_score_matched
        self.aba_merchat_id_matched = aba_merchat_id_matched

    @staticmethod
    def create_table():
        DB.execute(
            "CREATE TABLE IF NOT EXISTS matching_result (merchant_name VARCHAR(255), aba_merchat_name_matched VARCHAR(255), aba_merchat_score_matched FLOAT, aba_merchat_id_matched INT)"
        )

    @staticmethod
    def save_db(result_path="result.csv", score=0.97):
        # check if table exists
        Result.create_table()

        # read result
        result = pd.read_csv(result_path)

        # filter result by aba_merchat_score_matched <= 1 - score
        result = result[result['aba_merchat_score_matched'] <= 1 - score]

        # save to db
        for index, row in result.iterrows():
            DB.execute(
                "INSERT INTO matching_result (merchant_name, aba_merchat_name_matched, aba_merchat_score_matched, aba_merchat_id_matched) VALUES (%s, %s, %s, %s)",
                (row['merchant_name'], row['aba_merchat_name_matched'], row['aba_merchat_score_matched'], row['aba_merchat_id_matched'])
            )

        


