import datetime
import os
from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every
import preprocessing
import postprocessing
import algorithm
import logging.config

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)

logger = logging.getLogger(__name__) 

app = FastAPI(
    title="ABA MERCHANT MATCHIING",
    debug=True,
    version="0.0.1",
)

@app.on_event("startup")
@repeat_every(seconds=60 * int(os.getenv("RUN_EVERY_MINUTES")))
def index():
    try:
        logger.info("Start Cron Job ...")
        start_time = datetime.datetime.now()

        qr_path = "data/qr.csv"
        pw_path = "data/pw.csv"
        result_path = "data/result.csv"

        logger.info("Working on preprocessing ...")
        preprocessing.start(qr_path=qr_path, pw_path=pw_path, result_path=result_path)

        logger.info("Working on algorithm ...")
        algorithm.start(result_path=result_path, qr_path=qr_path, pw_path=pw_path)

        logger.info("Working on postprocessing ...")
        postprocessing.start(result_path=result_path)

        end_time = datetime.datetime.now()
        duration = end_time - start_time
        logger.info("Cron Job Finished in {} seconds".format(duration.total_seconds()))
        
    except Exception as e:
        logger.error("Cron Job Failed: {}".format(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")
