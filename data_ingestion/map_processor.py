import xml.etree.ElementTree as ET
import json
import math
import os

def haversine(lat1, lon1, lat2, lon2):
    """Tính khoảng cách thực tế (mét) giữa 2 tọa độ GPS"""
    R = 6371000 # Bán kính trái đất (mét)
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def process_osm_map(osm_file_path, output_edges_path):
    print(f"Đang đọc file bản đồ thô từ: {osm_file_path}...")
    try:
        tree = ET.parse(osm_file_path)
        root = tree.getroot()
    except Exception as e:
        print(f"[LỖI] Không thể đọc file OSM. Hãy chắc chắn file Hanoi_map.osm đã có trong thư mục data. Chi tiết: {e}")
        return

    nodes = {}
    # 1. Trích xuất toàn bộ các Nút (Ngã tư, điểm gãy)
    for node in root.findall('node'):
        nodes[node.attrib['id']] = {
            'lat': float(node.attrib['lat']),
            'lon': float(node.attrib['lon'])
        }

    edges = []
    # 2. Trích xuất các Cạnh (Đường đi)
    for way in root.findall('way'):
        is_highway = False
        road_name = "Đường không tên"
        max_speed = 40.0 # Vận tốc mặc định nếu OSM không ghi

        # Lọc các thẻ tag để tìm đường giao thông
        for tag in way.findall('tag'):
            if tag.attrib['k'] == 'highway':
                is_highway = True
            if tag.attrib['k'] == 'name':
                road_name = tag.attrib['v']
            if tag.attrib['k'] == 'maxspeed':
                try:
                    max_speed = float(tag.attrib['v'])
                except:
                    pass

        # Nếu đúng là đường giao thông, tiến hành chẻ nhỏ thành các đoạn (Edge)
        if is_highway:
            nd_refs = [nd.attrib['ref'] for nd in way.findall('nd')]
            for i in range(len(nd_refs) - 1):
                start_node = nd_refs[i]
                end_node = nd_refs[i+1]

                if start_node in nodes and end_node in nodes:
                    lat1, lon1 = nodes[start_node]['lat'], nodes[start_node]['lon']
                    lat2, lon2 = nodes[end_node]['lat'], nodes[end_node]['lon']
                    
                    # Tính khoảng cách và nhúng vào data
                    length = haversine(lat1, lon1, lat2, lon2)

                    # Bỏ qua các đoạn đường lỗi (khoảng cách = 0)
                    if length > 0:
                        edges.append({
                            "edge_id": f"E_{start_node}_{end_node}",
                            "road_name": road_name,
                            "max_speed_kmh": max_speed,
                            "start_node": {"node_id": start_node, "lat": lat1, "lon": lon1},
                            "end_node": {"node_id": end_node, "lat": lat2, "lon": lon2},
                            "length_meters": round(length, 2)
                        })

    # 3. Lưu ra file JSON chuẩn Schema
    print(f"Đã xử lý xong. Tìm thấy {len(edges)} đoạn đường hợp lệ.")
    with open(output_edges_path, 'w', encoding='utf-8') as f:
        json.dump(edges, f, ensure_ascii=False, indent=4)
    print(f"Đã lưu danh sách đường đi tại: {output_edges_path}")

if __name__ == "__main__":
    # Đảm bảo đường dẫn chính xác dựa trên cấu trúc thư mục của bạn
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    osm_path = os.path.join(BASE_DIR, "data", "Hanoi_map.osm")
    output_path = os.path.join(BASE_DIR, "data", "edges_schema.json")
    
    process_osm_map(osm_path, output_path)