import json
from confluent_kafka import Producer
import time

class GPSProducer:
    def __init__(self):
        # Kết nối tới Kafka ảo đang chạy ở Bước 1
        conf = {
            'bootstrap.servers': 'localhost:9092',
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

# --- CHẠY THỬ XEM CÓ LỖI KHÔNG ---
if __name__ == "__main__":
    my_producer = GPSProducer()
    
    # 1 data mẫu đúng y hệt file Docx
    test_data = {
        "entity_id": "Bot_0001",
        "entity_type": "Bot",
        "latitude": 21.012345,
        "longitude": 105.812345,
        "speed": 35.5,
        "timestamp": int(time.time() * 1000)
    }
    
    print("Đang bắn thử 1 tọa độ lên Kafka...")
    my_producer.produce_message(test_data)
    my_producer.flush()
    print("THÀNH CÔNG! Đường ống Kafka đã thông suốt!")