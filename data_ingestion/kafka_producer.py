import json
from confluent_kafka import Producer
import time
import os

class GPSProducer:
    def __init__(self):
        # SỬA TẠI ĐÂY: Lấy từ biến môi trường của Docker Compose
        # Nếu không có biến môi trường, mặc định dùng 'kafka:29092' (tên service trong compose)
        broker = os.getenv('KAFKA_BROKER', 'kafka:29092')
        
        conf = {
            'bootstrap.servers': broker,
            'client.id': 'python-producer'
        }
        
        print(f"--- Đang kết nối tới Kafka Broker tại: {broker} ---")
        self.producer = Producer(conf)
        self.topic = 'gps_stream'

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"[LỖI] Gửi thất bại: {err}")
        else:
            print(f"[OK] Đã gửi tới topic {msg.topic()} [{msg.partition()}]")

    def produce_message(self, data_dict):
        json_data = json.dumps(data_dict).encode('utf-8')
        try:
            self.producer.produce(
                topic=self.topic,
                key=str(data_dict['entity_id']).encode('utf-8'),
                value=json_data,
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"Lỗi khi produce: {e}")

    def flush(self):
        self.producer.flush()

if __name__ == "__main__":
    my_producer = GPSProducer()
    
    # Gửi thử 10 tin nhắn để kiểm tra độ ổn định
    for i in range(10):
        test_data = {
            "entity_id": f"Bot_{i:04d}",
            "entity_type": "Bot",
            "latitude": 21.012345,
            "longitude": 105.812345,
            "speed": 35.5,
            "timestamp": int(time.time() * 1000)
        }
        my_producer.produce_message(test_data)
        time.sleep(0.5)
        
    my_producer.flush()
    print("THÀNH CÔNG! Luồng dữ liệu đã thông suốt!")