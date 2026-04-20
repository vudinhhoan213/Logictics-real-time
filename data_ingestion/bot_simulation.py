import json
import time
import random
import os
from kafka_producer import GPSProducer

# --- BIẾN TOÀN CỤC MÔ PHỎNG VẬT LÝ ---
RHO_MAX = 130  
V_MIN = 2.5    
edge_vehicle_count = {} 

# Danh sách các "Đích đến" giờ cao điểm (Khu văn phòng, trung tâm thương mại)
ATTRACTOR_EDGES = [] 

def load_map_graph(edges_filepath):
    print("Đang nạp bản đồ vào bộ nhớ...")
    with open(edges_filepath, 'r', encoding='utf-8') as f:
        edges = json.load(f)
    
    graph = {}
    for edge in edges:
        edge_id = edge['edge_id']
        edge_vehicle_count[edge_id] = 0  
        
        start = edge['start_node']['node_id']
        if start not in graph:
            graph[start] = []
        graph[start].append(edge)
    
    # CHUẨN THỰC TẾ: Chọn ra 5 con đường làm "Trung tâm" (ví dụ: các toà nhà văn phòng lớn)
    global ATTRACTOR_EDGES
    ATTRACTOR_EDGES = random.sample(edges, min(5, len(edges)))
    print(f"Đã thiết lập {len(ATTRACTOR_EDGES)} trung tâm thu hút giao thông giờ cao điểm.")
        
    return edges, graph

class Vehicle:
    cached_edge_lengths = []

    def __init__(self, v_id, v_type, all_edges, graph):
        self.entity_id = v_id
        self.entity_type = v_type
        self.graph = graph
        self.all_edges = all_edges
        
        if not Vehicle.cached_edge_lengths:
            Vehicle.cached_edge_lengths = [max(e['length_meters'], 1.0) for e in all_edges]
            
        self._spawn()

    def _spawn(self):
        """Logic Khởi tạo xe lần đầu hoặc khi đã hoàn thành toàn bộ nhiệm vụ"""
        self.current_edge = random.choices(self.all_edges, weights=Vehicle.cached_edge_lengths, k=1)[0]
        
        # --- PHÂN TÁCH LOGIC TRUCK VÀ BOT ---
        if self.entity_type == "Truck":
            # Bốc 10 đường ngẫu nhiên
            raw_edges = random.choices(self.all_edges, k=10)
            
            self.customer_route = []          # Dành để xuất JSON 
            self._internal_route_edges = []   # Dành cho logic chạy ngầm của xe tải
            
            for idx, edge in enumerate(raw_edges):
                # 1. Format chuẩn JSON cho khách hàng
                self.customer_route.append({
                    "cust_id": f"Cust_{self.entity_id}_{idx+1}",
                    "latitude": edge['start_node']['lat'],
                    "longitude": edge['start_node']['lon']
                })
                # 2. Lưu object bản đồ vào mảng ẩn để xe biết đường chạy
                self._internal_route_edges.append(edge)
            
            self.current_route_index = 0
            self.target_edge = self._internal_route_edges[self.current_route_index]
        else:
            # Bot thì chỉ cần 1 đích đến, 70% lao vào điểm nóng
            if random.random() < 0.70:
                self.target_edge = random.choice(ATTRACTOR_EDGES)
            else:
                self.target_edge = random.choice(self.all_edges)
        
        self.latitude = self.current_edge['start_node']['lat']
        self.longitude = self.current_edge['start_node']['lon']
        self.progress_meters = 0.0
        self.speed = self.current_edge['max_speed_kmh']
        
        edge_vehicle_count[self.current_edge['edge_id']] += 1

    def move(self):
        edge_id = self.current_edge['edge_id']
        length_m = self.current_edge['length_meters']
        max_speed = self.current_edge['max_speed_kmh']
        
        # 1. Greenshields tính vận tốc
        n_vehicles = edge_vehicle_count[edge_id]
        density = n_vehicles / (length_m / 1000) if length_m > 0 else 0
        if density >= RHO_MAX:
            self.speed = V_MIN
        else:
            self.speed = max(V_MIN, max_speed * (1 - (density / RHO_MAX)))
            
        speed_ms = self.speed * (1000 / 3600)
        self.progress_meters += speed_ms
        
        # 2. Xử lý khi đi hết đoạn đường hiện tại (đến ngã tư)
        if self.progress_meters >= length_m:
            edge_vehicle_count[edge_id] -= 1
            
            # KIỂM TRA ĐÃ TỚI ĐÍCH CHƯA?
            if self.current_edge['edge_id'] == self.target_edge['edge_id']:
                if self.entity_type == "Truck":
                    # Đã giao xong cho khách hiện tại -> Chuyển sang khách tiếp theo
                    self.current_route_index += 1
                    if self.current_route_index < len(self.customer_route):
                        # Lấy tọa độ khách tiếp theo, KHÔNG gọi _spawn() để giữ nguyên vị trí xe
                        self.target_edge = self._internal_route_edges[self.current_route_index]
                        edge_vehicle_count[edge_id] += 1 # Xe vẫn đang đứng ở đây
                        return
                    else:
                        # Giao xong cả 10 khách -> Quay về kho (hoặc làm ca mới)
                        self._spawn() 
                        return
                else:
                    # Là Bot -> Đi đến nơi thì tan biến và sinh ra bot mới
                    self._spawn() 
                    return
            
            end_node = self.current_edge['end_node']['node_id']
            next_edges = self.graph.get(end_node, [])
            
            if next_edges:
                # 3. Định tuyến tham lam tới mục tiêu (target_edge)
                target_lat = self.target_edge['start_node']['lat']
                target_lon = self.target_edge['start_node']['lon']
                
                best_edge = next_edges[0]
                min_dist = float('inf')
                
                for edge in next_edges:
                    n_lat, n_lon = edge['end_node']['lat'], edge['end_node']['lon']
                    dist = (n_lat - target_lat)**2 + (n_lon - target_lon)**2
                    if dist < min_dist:
                        min_dist = dist
                        best_edge = edge
                
                # 4. Gatekeeping tại ngã tư
                next_length = best_edge['length_meters']
                next_capacity = RHO_MAX * (next_length / 1000)
                
                if edge_vehicle_count[best_edge['edge_id']] >= next_capacity:
                    self.progress_meters = length_m
                    self.speed = 0.0
                    edge_vehicle_count[edge_id] += 1 
                else:
                    self.current_edge = best_edge
                    self.progress_meters = 0.0
                    self.latitude = self.current_edge['start_node']['lat']
                    self.longitude = self.current_edge['start_node']['lon']
                    edge_vehicle_count[self.current_edge['edge_id']] += 1
            else:
                self._spawn()
        else:
            ratio = self.progress_meters / length_m
            lat1, lon1 = self.current_edge['start_node']['lat'], self.current_edge['start_node']['lon']
            lat2, lon2 = self.current_edge['end_node']['lat'], self.current_edge['end_node']['lon']
            self.latitude = lat1 + (lat2 - lat1) * ratio
            self.longitude = lon1 + (lon2 - lon1) * ratio

    # Giữ nguyên hàm to_json_message...
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
    
    print("Đang khởi tạo 9.900 Bots và 100 Trucks...")
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