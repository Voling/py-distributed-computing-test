from confluent_kafka import Producer as KafkaProducer

class Producer:
    def __init__(self, topic_name, broker_name):
        self.topic_name = topic_name
        self.producer = KafkaProducer({'bootstrap.servers': broker_name})
        
    def send(self, msg, repetitions=20):
        for i in range(repetitions):
            try:
                message = f"{msg}{i}".encode('utf-8')
                self.producer.produce(self.topic_name, value=message)
                self.producer.poll(0)
            except Exception as e:
                print(f"Error sending message {e}")
        self.producer.flush()