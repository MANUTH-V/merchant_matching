from models.result import Result


def start(result_path: str):
    Result.save_db(result_path=result_path,score=0.97)
