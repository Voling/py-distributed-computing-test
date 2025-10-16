from prometheus_client import Counter, Histogram, Gauge, start_http_server
from dotenv import load_dotenv
import os
load_dotenv()
METRICS_PORT = os.getenv("METRICS_PORT", 9108)
start_http_server(METRICS_PORT)
kafka_msgs_total = Counter("kafka_messages_total", "Consumed msgs")
kafka_errors_total = Counter("kafka_errors_total", "Consume errors")
enqueue_total = Counter("celery_enqueue_total", "Tasks enqueued")
enqueue_fail_total = Counter("celery_enqueue_fail_total", "Enqueue failures")
enqueue_seconds = Histogram("celery_enqueue_seconds", "Enqueue latency (s)")
consumer_lag = Gauge("kafka_consumer_lag", "Consumer lag", ["topic","partition"])

