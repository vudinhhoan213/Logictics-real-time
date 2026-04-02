import json
import time
import random
import os
from kafka_producer import GPSProducer

# --- 1. HÀM ĐỌC BẢN ĐỒ VÀ TẠO GRAPH ---
def load_map_graph(edges_filepath):
    print("Đang nạp bản đồ vào bộ nhớ...")
    with open(edges_filepath, 'r', encoding='utf-8') as f:
        edges = json.load(f)
    
    # Tạo dictionary để tra cứu nhanh các đường đi tiếp theo từ 1 nút (Ngã tư)
    graph = {}
    for edge in edges:
        start = edge['start_node']['node_id']
        if start not in graph:
            graph[start] = []
        graph[start].append(edge)
    
    print(f"Đã nạp xong {len(edges)} đoạn đường.")
    return edges, graph

# --- 2. XÂY DỰNG CLASS THỰC THỂ XE ---
class Vehicle:
    def __init__(self, v_id, v_type, all_edges, graph):
        self.entity_id = v_id
        self.entity_type = v_type
        self.graph = graph
        
        # Bắt đầu ngẫu nhiên tại 1 đoạn đường
        self.current_edge = random.choice(all_edges)
        self.latitude = self.current_edge['start_node']['lat']
        self.longitude = self.current_edge['start_node']['lon']
        
        # Biến trạng thái di chuyển
        self.progress_meters = 0.0 # Đã đi được bao nhiêu mét trên đoạn đường này
        self.speed = random.uniform(15.0, self.current_edge['max_speed_kmh'])

    def move(self):
        """Logic di chuyển đơn giản: Cập nhật tọa độ tuyến tính dọc theo đoạn đường"""
        # Chuyển đổi vận tốc từ km/h sang m/s
        speed_ms = self.speed * (1000 / 3600)
        self.progress_meters += speed_ms
        
        length = self.current_edge['length_meters']
        
        if self.progress_meters >= length:
            # Nếu đi hết đường -> Chuyển sang đường mới (tới ngã tư)
            end_node_id = self.current_edge['end_node']['node_id']
            
            # Tìm các đường rẽ tiếp theo từ nút này
            next_edges = self.graph.get(end_node_id, [])
            if next_edges:
                self.current_edge = random.choice(next_edges) # Rẽ ngẫu nhiên
            else:
                pass # Ngõ cụt, đứng im (Hoặc bạn có thể cho nó biến mất sinh ra ở chỗ khác)
            
            self.progress_meters = 0.0
            self.latitude = self.current_edge['start_node']['lat']
            self.longitude = self.current_edge['start_node']['lon']
            
        else:
            # Nhích tọa độ dần tới đích (Interpolation)
            ratio = self.progress_meters / length
            lat1, lon1 = self.current_edge['start_node']['lat'], self.current_edge['start_node']['lon']
            lat2, lon2 = self.current_edge['end_node']['lat'], self.current_edge['end_node']['lon']
            
            self.latitude = lat1 + (lat2 - lat1) * ratio
            self.longitude = lon1 + (lon2 - lon1) * ratio

    def to_json_message(self):
        """Xuất dữ liệu chuẩn Schema Kafka"""
        return {
            "entity_id": self.entity_id,
            "entity_type": self.entity_type,
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "speed": round(self.speed, 2),
            "timestamp": int(time.time() * 1000)
        }

# --- 3. VÒNG LẶP ĐIỀU PHỐI (MAIN LOOP) ---
if __name__ == "__main__":
    # Nạp bản đồ
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    edges_path = os.path.join(BASE_DIR, "data", "edges_schema.json")
    all_edges, graph = load_map_graph(edges_path)
    
    # Khởi tạo Kafka Producer
    producer = GPSProducer()
    
    # Sinh ra 10.000 xe
    print("Đang khởi tạo 9.900 Bots và 100 Trucks...")
    vehicles = []
    for i in range(1, 9901):
        vehicles.append(Vehicle(f"Bot_{i:04d}", "Bot", all_edges, graph))
    for i in range(1, 101):
        vehicles.append(Vehicle(f"Truck_{i:03d}", "Truck", all_edges, graph))
    
    print("Bắt đầu mô phỏng giao thông thời gian thực! (Nhấn Ctrl+C để dừng)")
    try:
        while True:
            start_time = time.time()
            
            # Cho từng xe di chuyển và bắn lên Kafka
            for v in vehicles:
                v.move()
                producer.produce_message(v.to_json_message())
            
            # Xả hàng đợi
            producer.flush()
            
            # Tính toán thời gian xử lý để bù trừ (đảm bảo đúng 1 giây/chu kỳ)
            elapsed = time.time() - start_time
            sleep_time = max(0, 1.0 - elapsed)
            print(f"Đã gửi 10.000 tọa độ. Mất {elapsed:.2f}s xử lý. Chờ {sleep_time:.2f}s...")
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nĐã dừng mô phỏng an toàn.")