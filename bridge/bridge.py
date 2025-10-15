from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from os import getenv
import signal
import threading
import json
from celery import Celery
from Producer import Producer
import time
load_dotenv()
BROKER_NAME = getenv("BROKER_NAME", "kafka:9092")
TOPIC_NAME = getenv("TOPIC_NAME", "quickstart-events")
GROUP_ID = getenv("GROUP_ID", "vetology-test")
REDIS_URL= getenv("REDIS_URL", "redis://redis:6379/0")
config = {
    "bootstrap.servers": BROKER_NAME,
    "group.id": GROUP_ID,
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest"
}
stop = threading.Event() #Stop event
celery_app = Celery("bridge", broker=REDIS_URL, backend=REDIS_URL)
def run_consumer(stop_event: threading.Event):
    consumer = Consumer(config)
    consumer.subscribe([TOPIC_NAME])
    try:
        while not stop_event.is_set(): #loop over stop
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("Kafka error:", msg.error())
                continue

            val = msg.value()
            try:
                payload = json.loads(val.decode("utf-8"))
            except Exception:
                payload = {"raw": val.decode("utf-8", "ignore")}
            celery_app.send_task("hello", args=[payload])
            consumer.commit(msg) 
    finally:
        consumer.close()
def _halt(*_): stop.set() #stop signal handler
if __name__ == "__main__":
    producer = Producer(TOPIC_NAME, BROKER_NAME)
    for i in range(5):
        msg = json.dumps({"message": f"HELLO {i}", "ts": time.time()})
        producer.send(msg, repetitions=1)
        time.sleep(0.5)
    signal.signal(signal.SIGINT, _halt)
    signal.signal(signal.SIGTERM, _halt)
    t = threading.Thread(target=run_consumer, args=[stop], name="kafka-consumer") #start new thread for consumer
    t.start()
    t.join()