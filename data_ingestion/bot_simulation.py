import json
import time
import random
from collections import deque
import os
import heapq
from kafka_producer import GPSProducer

# --- BIẾN TOÀN CỤC MÔ PHỎNG VẬT LÝ ---
RHO_MAX = 250  
V_MIN = 2.5    
edge_vehicle_count = {} 
STUCK_TIMEOUT = 3  # Số tick bị kẹt tối đa trước khi tìm target mới

# Danh sách các "Đích đến" giờ cao điểm (Khu văn phòng, trung tâm thương mại)
ATTRACTOR_EDGES = [] 

# --- EDGE LOOKUP (dùng cho Dijkstra) ---
edge_by_id = {}  # edge_id → edge object
MAIN_COMPONENT_EDGES = []  # Edges thuộc component lớn nhất (reachable)


def load_map_graph(edges_filepath):
    print("Đang nạp bản đồ vào bộ nhớ...")
    with open(edges_filepath, 'r', encoding='utf-8') as f:
        edges = json.load(f)
    
    graph = {}
    for edge in edges:
        edge_id = edge['edge_id']
        edge_vehicle_count[edge_id] = 0
        edge_by_id[edge_id] = edge
        
        start = edge['start_node']['node_id']
        if start not in graph:
            graph[start] = []
        graph[start].append(edge)
    
    # Tìm connected component lớn nhất (để Truck chỉ chọn customer reachable)
    visited_nodes = set()
    components = []
    for start_n in graph:
        if start_n in visited_nodes:
            continue
        component = set()
        q = deque([start_n])
        while q:
            node = q.popleft()
            if node in visited_nodes:
                continue
            visited_nodes.add(node)
            component.add(node)
            for e in graph.get(node, []):
                if e['end_node']['node_id'] not in visited_nodes:
                    q.append(e['end_node']['node_id'])
        components.append(component)
    
    main_comp = max(components, key=len)
    global MAIN_COMPONENT_EDGES
    MAIN_COMPONENT_EDGES = [e for e in edges if e['start_node']['node_id'] in main_comp]
    print(f"Main component: {len(main_comp)} nodes, {len(MAIN_COMPONENT_EDGES)} edges (dùng cho Truck routing)")
    
    # CHUẨN THỰC TẾ: Chọn ra 15 con đường làm "Trung tâm"
    global ATTRACTOR_EDGES
    ATTRACTOR_EDGES = random.sample(edges, min(15, len(edges)))
    print(f"Đã thiết lập {len(ATTRACTOR_EDGES)} trung tâm thu hút giao thông giờ cao điểm (phân tán).")
        
    return edges, graph


# ============================================================
# DIJKSTRA SHORTEST PATH cho Truck routing
# ============================================================

def dijkstra(graph, start_node_id, end_node_id):
    """
    Tìm đường ngắn nhất từ start_node_id → end_node_id.
    Trả về list edge objects liên tục hoặc [] nếu không tìm được.
    """
    if start_node_id == end_node_id:
        return []
    
    # dist[node_id] = (cost, prev_node_id, via_edge)
    dist = {start_node_id: 0}
    prev = {}  # node_id → (parent_node_id, edge_object)
    visited = set()
    heap = [(0, start_node_id)]
    
    while heap:
        current_cost, current_node = heapq.heappop(heap)
        
        if current_node in visited:
            continue
        visited.add(current_node)
        
        if current_node == end_node_id:
            break
        
        for edge in graph.get(current_node, []):
            next_node = edge['end_node']['node_id']
            if next_node in visited:
                continue
            
            # Cost = travel time (seconds) dựa trên length / speed
            length_m = edge['length_meters']
            speed_kmh = max(edge['max_speed_kmh'], 1.0)
            cost = length_m / (speed_kmh * 1000 / 3600)
            
            new_cost = current_cost + cost
            if new_cost < dist.get(next_node, float('inf')):
                dist[next_node] = new_cost
                prev[next_node] = (current_node, edge)
                heapq.heappush(heap, (new_cost, next_node))
    
    # Trace back
    if end_node_id not in prev and start_node_id != end_node_id:
        return []
    
    path_edges = []
    cur = end_node_id
    while cur != start_node_id:
        if cur not in prev:
            return []
        parent_node, edge = prev[cur]
        path_edges.append(edge)
        cur = parent_node
    
    path_edges.reverse()
    return path_edges


def nearest_node(graph, lat, lon):
    """Tìm node_id gần nhất trong graph với tọa độ (lat, lon)."""
    best_node = None
    best_dist = float('inf')
    # Lấy tất cả nodes đã biết (từ edges)
    for node_id in graph:
        edges_from = graph[node_id]
        if edges_from:
            n = edges_from[0]['start_node']
            d = (n['lat'] - lat) ** 2 + (n['lon'] - lon) ** 2
            if d < best_dist:
                best_dist = d
                best_node = node_id
    return best_node


def build_truck_route(graph, all_edges, start_edge):
    """
    Xây dựng lộ trình Truck: chọn 5-8 customers ngẫu nhiên, 
    tính Dijkstra shortest-path liên tục qua tất cả
    (chỉ chọn từ MAIN_COMPONENT_EDGES để đảm bảo reachable).
    Trả về: (customer_route_json, route_edge_sequence)
    """
    num_customers = random.randint(5, 8)
    
    # Chọn customers từ main component (đảm bảo Dijkstra tìm được đường)
    source = MAIN_COMPONENT_EDGES if MAIN_COMPONENT_EDGES else all_edges
    customer_edges = random.sample(source, min(num_customers, len(source)))
    
    customer_route = []
    for idx, edge in enumerate(customer_edges):
        customer_route.append({
            "cust_id": f"Cust_placeholder_{idx+1}",
            "latitude": edge['start_node']['lat'],
            "longitude": edge['start_node']['lon']
        })
    
    # Tính shortest path liên tục: start_edge.end_node → customer1 → customer2 → ...
    route_edge_sequence = []
    current_node = start_edge['end_node']['node_id']
    
    # Greedy nearest-customer-first ordering (để route hợp lý hơn random)
    unvisited = list(range(len(customer_edges)))
    ordered_customers = []
    
    temp_node = current_node
    while unvisited:
        best_idx = None
        best_dist = float('inf')
        for i in unvisited:
            target = customer_edges[i]['start_node']
            if temp_node in graph and graph[temp_node]:
                src = graph[temp_node][0]['start_node']
            else:
                break
            d = (src['lat'] - target['lat']) ** 2 + (src['lon'] - target['lon']) ** 2
            if d < best_dist:
                best_dist = d
                best_idx = i
        if best_idx is None:
            break
        ordered_customers.append(best_idx)
        unvisited.remove(best_idx)
        temp_node = customer_edges[best_idx]['start_node']['node_id']
    
    # Nếu ordering thất bại, dùng random order
    if not ordered_customers:
        ordered_customers = list(range(len(customer_edges)))
    
    # Xây dựng route liên tục
    final_customer_route = []
    current_node = start_edge['end_node']['node_id']
    
    for cust_idx in ordered_customers:
        target_edge = customer_edges[cust_idx]
        target_node = target_edge['start_node']['node_id']
        
        # Dijkstra từ current_node → target_node
        partial = dijkstra(graph, current_node, target_node)
        
        if partial:
            route_edge_sequence.extend(partial)
            current_node = target_node
            final_customer_route.append(customer_route[cust_idx])
        else:
            # Không tìm được đường → bỏ qua customer này
            pass
    
    # Nếu route quá ngắn, thử lại
    if len(route_edge_sequence) < 3:
        # Fallback: lấy vài edge liên tục từ graph
        node = start_edge['end_node']['node_id']
        for _ in range(20):
            edges_from = graph.get(node, [])
            if not edges_from:
                break
            e = random.choice(edges_from)
            route_edge_sequence.append(e)
            node = e['end_node']['node_id']
        if not final_customer_route:
            final_customer_route.append({
                "cust_id": "Cust_fallback",
                "latitude": route_edge_sequence[-1]['end_node']['lat'] if route_edge_sequence else start_edge['end_node']['lat'],
                "longitude": route_edge_sequence[-1]['end_node']['lon'] if route_edge_sequence else start_edge['end_node']['lon'],
            })
    
    return final_customer_route, route_edge_sequence


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
        if self.entity_type == "Truck" and MAIN_COMPONENT_EDGES:
            self.current_edge = random.choice(MAIN_COMPONENT_EDGES)
        else:
            self.current_edge = random.choices(self.all_edges, weights=Vehicle.cached_edge_lengths, k=1)[0]
        
        # --- PHÂN TÁCH LOGIC TRUCK VÀ BOT ---
        if self.entity_type == "Truck":
            # === TRUCK: Tính route liên tục bằng Dijkstra ===
            self.customer_route, self._route_edge_sequence = build_truck_route(
                self.graph, self.all_edges, self.current_edge
            )
            self._route_index = 0  # Vị trí hiện tại trong route sequence
            
            # Cust ID sửa lại cho đúng truck
            for idx, cust in enumerate(self.customer_route):
                cust["cust_id"] = f"Cust_{self.entity_id}_{idx+1}"
            
        else:
            # Bot thì chỉ cần 1 đích đến, 40% lao vào điểm nóng
            if random.random() < 0.40:
                self.target_edge = random.choice(ATTRACTOR_EDGES)
            else:
                self.target_edge = random.choice(self.all_edges)
        
        self.latitude = self.current_edge['start_node']['lat']
        self.longitude = self.current_edge['start_node']['lon']
        self.progress_meters = 0.0
        self.speed = self.current_edge['max_speed_kmh']
        self.stuck_count = 0
        
        edge_vehicle_count[self.current_edge['edge_id']] += 1

    def _move_truck(self):
        """Logic di chuyển cho Truck: đi tuần tự theo route_edge_sequence."""
        edge_id = self.current_edge['edge_id']
        length_m = self.current_edge['length_meters']
        max_speed = self.current_edge['max_speed_kmh']
        
        # 1. Greenshields tính vận tốc (vẫn giữ traffic physics)
        n_vehicles = edge_vehicle_count[edge_id]
        density = n_vehicles / (length_m / 1000) if length_m > 0 else 0
        if density >= RHO_MAX:
            self.speed = V_MIN
        else:
            self.speed = max(V_MIN, max_speed * (1 - (density / RHO_MAX)))
            
        speed_ms = self.speed * (1000 / 3600)
        self.progress_meters += speed_ms
        
        # 2. Đi hết edge hiện tại → chuyển sang edge tiếp theo trong route
        if self.progress_meters >= length_m:
            edge_vehicle_count[edge_id] -= 1
            
            self._route_index += 1
            
            # Kiểm tra đã đi hết route chưa
            if self._route_index >= len(self._route_edge_sequence):
                # Hoàn thành toàn bộ lộ trình → spawn lại
                self._spawn()
                return
            
            # Chuyển sang edge tiếp theo trong route (đã tính sẵn, liên tục)
            next_edge = self._route_edge_sequence[self._route_index]
            
            # Gatekeeping: kiểm tra capacity
            next_length = next_edge['length_meters']
            next_capacity = RHO_MAX * (next_length / 1000)
            
            if edge_vehicle_count[next_edge['edge_id']] >= next_capacity:
                # Edge đầy → chờ (giữ nguyên vị trí cuối edge hiện tại)
                self.progress_meters = length_m
                self.speed = V_MIN
                self.stuck_count += 1
                edge_vehicle_count[edge_id] += 1
                self._route_index -= 1  # Rollback index, thử lại tick sau
                
                # Anti-deadlock: kẹt quá lâu → skip edge này
                if self.stuck_count >= STUCK_TIMEOUT * 2:
                    self.stuck_count = 0
                    # Force di chuyển
                    self.current_edge = next_edge
                    self.progress_meters = 0.0
                    self.latitude = next_edge['start_node']['lat']
                    self.longitude = next_edge['start_node']['lon']
                    edge_vehicle_count[next_edge['edge_id']] += 1
                    self._route_index += 1
            else:
                # Di chuyển thành công
                self.current_edge = next_edge
                self.progress_meters = 0.0
                self.latitude = next_edge['start_node']['lat']
                self.longitude = next_edge['start_node']['lon']
                edge_vehicle_count[next_edge['edge_id']] += 1
                self.stuck_count = 0
        else:
            # Đang di chuyển trên edge → interpolate vị trí
            ratio = self.progress_meters / length_m
            lat1, lon1 = self.current_edge['start_node']['lat'], self.current_edge['start_node']['lon']
            lat2, lon2 = self.current_edge['end_node']['lat'], self.current_edge['end_node']['lon']
            self.latitude = lat1 + (lat2 - lat1) * ratio
            self.longitude = lon1 + (lon2 - lon1) * ratio

    def _move_bot(self):
        """Logic di chuyển cho Bot: giữ nguyên greedy routing."""
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
        
        # 2. Xử lý khi đi hết đoạn đường hiện tại
        if self.progress_meters >= length_m:
            edge_vehicle_count[edge_id] -= 1
            
            # KIỂM TRA ĐÃ TỚI ĐÍCH CHƯA?
            if self.current_edge['edge_id'] == self.target_edge['edge_id']:
                self._spawn()
                return
            
            end_node = self.current_edge['end_node']['node_id']
            next_edges = self.graph.get(end_node, [])
            
            if next_edges:
                # Định tuyến tham lam tới mục tiêu (target_edge)
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
                
                # Gatekeeping tại ngã tư
                next_length = best_edge['length_meters']
                next_capacity = RHO_MAX * (next_length / 1000)
                
                if edge_vehicle_count[best_edge['edge_id']] >= next_capacity:
                    self.progress_meters = length_m
                    self.speed = V_MIN
                    self.stuck_count += 1
                    edge_vehicle_count[edge_id] += 1 
                    
                    if self.stuck_count >= STUCK_TIMEOUT:
                        self.stuck_count = 0
                        self.target_edge = random.choice(self.all_edges)
                else:
                    self.current_edge = best_edge
                    self.progress_meters = 0.0
                    self.latitude = self.current_edge['start_node']['lat']
                    self.longitude = self.current_edge['start_node']['lon']
                    edge_vehicle_count[self.current_edge['edge_id']] += 1
                    self.stuck_count = 0
            else:
                self._spawn()
        else:
            ratio = self.progress_meters / length_m
            lat1, lon1 = self.current_edge['start_node']['lat'], self.current_edge['start_node']['lon']
            lat2, lon2 = self.current_edge['end_node']['lat'], self.current_edge['end_node']['lon']
            self.latitude = lat1 + (lat2 - lat1) * ratio
            self.longitude = lon1 + (lon2 - lon1) * ratio

    def move(self):
        """Dispatch move logic dựa trên entity_type."""
        if self.entity_type == "Truck":
            self._move_truck()
        else:
            self._move_bot()

    def to_json_message(self):
        msg = {
            "entity_id": self.entity_id,
            "entity_type": self.entity_type,
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "speed": round(self.speed, 2),
            "edge_id": self.current_edge['edge_id'],  # Thêm edge_id cho dashboard
            "timestamp": int(time.time() * 1000)
        }
        # Truck: gửi thêm route info
        if self.entity_type == "Truck":
            msg["route_index"] = self._route_index
            msg["route_total"] = len(self._route_edge_sequence)
        return msg


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    edges_path = os.path.join(BASE_DIR, "data", "edges_schema.json")
    all_edges, graph = load_map_graph(edges_path)
    
    producer = GPSProducer()
    
    print("Đang khởi tạo 9.900 Bots và 100 Trucks...")
    print("  → Trucks: đang tính route Dijkstra cho từng xe...")
    vehicles = [Vehicle(f"Bot_{i:04d}", "Bot", all_edges, graph) for i in range(1, 9901)]
    vehicles.extend([Vehicle(f"Truck_{i:03d}", "Truck", all_edges, graph) for i in range(1, 101)])
    print(f"  → Hoàn tất khởi tạo {len(vehicles)} xe.")
    
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
            print(f"Đã gửi 10k tọa độ. CPU: {elapsed:.2f}s. Nghỉ: {sleep_time:.2f}s")
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nĐã dừng mô phỏng an toàn.")
