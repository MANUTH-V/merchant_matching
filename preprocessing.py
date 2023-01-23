from db import DB
from models.qr import QR
from models.pw import PW
import os

def start(qr_path: str, pw_path: str,result_path: str):

    # check if new data exists
    if not QR.new_data_exists():
        raise Exception("No new data found")

    if os.path.exists(qr_path):
        os.remove(qr_path)
    # if os.path.exists(pw_path):
    #     os.remove(pw_path)
    if os.path.exists(result_path):
        os.remove(result_path)

    QR.save_csv(file_path=qr_path)
    # PW.save_csv(file_path=pw_path)
