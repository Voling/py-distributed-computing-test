from celery import Celery
from dotenv import load_dotenv
from os import getenv
import logging
from time import sleep

load_dotenv()
REDIS_URL = getenv("REDIS_URL", "redis://redis:6379/0")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Celery("Demo", broker=f"{REDIS_URL}", backend=f"{REDIS_URL}")
@app.task(name="hello")
def hello(data):
    logger.info(f"Message received {data}")
    return True