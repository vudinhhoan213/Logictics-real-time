import json
import os
from confluent_kafka import Producer

class GPSProducer:
    def __init__(self):
        # Ưu tiên lấy IP từ mạng ảo của Docker, nếu chạy ngoài thì dùng localhost
        conf = {
            'bootstrap.servers': os.getenv('KAFKA_BROKER', 'localhost:9092'),
            'client.id': 'python-producer'
        }
        self.producer = Producer(conf)
        self.topic = 'gps_stream'

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"[LỖI] Gửi thất bại: {err}")

    def produce_message(self, data_dict):
        json_data = json.dumps(data_dict).encode('utf-8')
        # Gửi dữ liệu đi
        self.producer.produce(
            topic=self.topic,
            key=data_dict['entity_id'].encode('utf-8'),
            value=json_data,
            callback=self.delivery_report
        )
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()