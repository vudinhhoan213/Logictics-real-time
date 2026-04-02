import json
import time
import random
import os
from kafka_producer import GPSProducer

# --- BIẾN TOÀN CỤC MÔ PHỎNG VẬT LÝ ---
RHO_MAX = 130  # Mật độ kẹt cứng: 130 xe/km
V_MIN = 2.5    # Vận tốc trườn khi tắc đường (km/h)
edge_vehicle_count = {} # RAM lưu trữ số lượng xe trên từng đường (Mô phỏng Redis nội bộ cho Bot)

def load_map_graph(edges_filepath):
    print("Đang nạp bản đồ vào bộ nhớ...")
    with open(edges_filepath, 'r', encoding='utf-8') as f:
        edges = json.load(f)
    
    graph = {}
    for edge in edges:
        edge_id = edge['edge_id']
        edge_vehicle_count[edge_id] = 0  # Khởi tạo đếm xe = 0
        
        start = edge['start_node']['node_id']
        if start not in graph:
            graph[start] = []
        graph[start].append(edge)
        
    return edges, graph

class Vehicle:
    def __init__(self, v_id, v_type, all_edges, graph):
        self.entity_id = v_id
        self.entity_type = v_type
        self.graph = graph
        self.all_edges = all_edges
        self._spawn() # Gọi hàm sinh ra xe

    def _spawn(self):
        """Logic Khai tử & Tái sinh (Source and Sink)"""
        # Random điểm xuất phát và điểm đến
        self.current_edge = random.choice(self.all_edges)
        self.target_edge = random.choice(self.all_edges)
        
        self.latitude = self.current_edge['start_node']['lat']
        self.longitude = self.current_edge['start_node']['lon']
        self.progress_meters = 0.0
        self.speed = self.current_edge['max_speed_kmh']
        
        # Đăng ký xe vào đoạn đường hiện tại
        edge_vehicle_count[self.current_edge['edge_id']] += 1

    def move(self):
        edge_id = self.current_edge['edge_id']
        length_m = self.current_edge['length_meters']
        max_speed = self.current_edge['max_speed_kmh']
        
        # --- 1. ÁP DỤNG CÔNG THỨC GREENSHIELDS ---
        n_vehicles = edge_vehicle_count[edge_id]
        density = n_vehicles / (length_m / 1000) if length_m > 0 else 0
        
        if density >= RHO_MAX:
            self.speed = V_MIN
        else:
            self.speed = max(V_MIN, max_speed * (1 - (density / RHO_MAX)))
            
        # Di chuyển
        speed_ms = self.speed * (1000 / 3600)
        self.progress_meters += speed_ms
        
        # --- 2. XỬ LÝ KHI ĐI ĐẾN NGÃ TƯ ---
        if self.progress_meters >= length_m:
            # Rút xe khỏi đường cũ
            edge_vehicle_count[edge_id] -= 1
            
            # Kiểm tra xem đã tới đích chưa
            if self.current_edge['edge_id'] == self.target_edge['edge_id']:
                self._spawn() # Hoàn thành nhiệm vụ -> Tái sinh xe mới
                return
            
            end_node = self.current_edge['end_node']['node_id']
            next_edges = self.graph.get(end_node, [])
            
            if next_edges:
                # --- 3. ĐỊNH TUYẾN THAM LAM (Greedy Routing) ---
                target_lat = self.target_edge['start_node']['lat']
                target_lon = self.target_edge['start_node']['lon']
                
                best_edge = next_edges[0]
                min_dist = float('inf')
                
                # Tìm ngã rẽ có hướng gần với điểm đến nhất (tính bằng bình phương khoảng cách Euclidean để tiết kiệm CPU)
                for edge in next_edges:
                    n_lat, n_lon = edge['end_node']['lat'], edge['end_node']['lon']
                    dist = (n_lat - target_lat)**2 + (n_lon - target_lon)**2
                    if dist < min_dist:
                        min_dist = dist
                        best_edge = edge
                
                # --- 4. GATEKEEPING TẠI NGÃ TƯ ---
                next_length = best_edge['length_meters']
                next_capacity = RHO_MAX * (next_length / 1000)
                
                if edge_vehicle_count[best_edge['edge_id']] >= next_capacity:
                    # Đường phía trước kẹt cứng -> Đứng chờ ở mép ngã tư
                    self.progress_meters = length_m
                    self.speed = 0.0
                    edge_vehicle_count[edge_id] += 1 # Đăng ký lại vào đường cũ vì chưa thoát được
                else:
                    # Đường thoáng -> Tiến vào đường mới
                    self.current_edge = best_edge
                    self.progress_meters = 0.0
                    self.latitude = self.current_edge['start_node']['lat']
                    self.longitude = self.current_edge['start_node']['lon']
                    edge_vehicle_count[self.current_edge['edge_id']] += 1
            else:
                # Ngõ cụt -> Tái sinh
                self._spawn()
        else:
            # Nội suy tọa độ trên đoạn đường
            ratio = self.progress_meters / length_m
            lat1, lon1 = self.current_edge['start_node']['lat'], self.current_edge['start_node']['lon']
            lat2, lon2 = self.current_edge['end_node']['lat'], self.current_edge['end_node']['lon']
            self.latitude = lat1 + (lat2 - lat1) * ratio
            self.longitude = lon1 + (lon2 - lon1) * ratio

    def to_json_message(self):
        return {
            "entity_id": self.entity_id,
            "entity_type": self.entity_type,
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "speed": round(self.speed, 2),
            "timestamp": int(time.time() * 1000)
        }

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    edges_path = os.path.join(BASE_DIR, "data", "edges_schema.json")
    all_edges, graph = load_map_graph(edges_path)
    
    producer = GPSProducer()
    
    print("Đang khởi tạo 9.900 Bots và 100 Trucks với mô hình Vật lý giao thông...")
    vehicles = [Vehicle(f"Bot_{i:04d}", "Bot", all_edges, graph) for i in range(1, 9901)]
    vehicles.extend([Vehicle(f"Truck_{i:03d}", "Truck", all_edges, graph) for i in range(1, 101)])
    
    print("Bắt đầu mô phỏng giao thông thời gian thực! (Nhấn Ctrl+C để dừng)")
    try:
        while True:
            start_time = time.time()
            for v in vehicles:
                v.move()
                producer.produce_message(v.to_json_message())
            
            producer.flush()
            
            elapsed = time.time() - start_time
            sleep_time = max(0, 1.0 - elapsed)
            print(f"Đã gửi 10k tọa độ. CPU xử lý: {elapsed:.2f}s. Nghỉ: {sleep_time:.2f}s...")
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nĐã dừng mô phỏng an toàn.")