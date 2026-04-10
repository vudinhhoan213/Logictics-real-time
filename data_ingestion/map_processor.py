import xml.etree.ElementTree as ET
import json
import math
import os

# Chỉ dùng để lọc nội bộ, KHÔNG xuất ra file JSON
ALLOWED_HIGHWAY_TYPES = {
    'motorway', 'motorway_link', 'trunk', 'trunk_link', 
    'primary', 'primary_link', 'secondary', 'secondary_link', 
    'tertiary', 'tertiary_link', 'unclassified', 'residential'
}

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def process_osm_map(osm_file_path, output_edges_path):
    print(f"Đang phân tích dữ liệu bản đồ từ: {osm_file_path}...")
    try:
        tree = ET.parse(osm_file_path)
        root = tree.getroot()
    except Exception as e:
        print(f"[LỖI] Không thể đọc file OSM. Chi tiết: {e}")
        return

    nodes = {}
    for node in root.findall('node'):
        node_id = node.get('id')
        lat = float(node.get('lat'))
        lon = float(node.get('lon'))
        nodes[node_id] = {'lat': lat, 'lon': lon}

    edges = []
    
    for way in root.findall('way'):
        road_name = "Đường không tên"
        max_speed = 40.0
        highway_type = None
        oneway = "no" 
        
        for tag in way.findall('tag'):
            k = tag.get('k')
            v = tag.get('v')
            if k == 'name': road_name = v
            elif k == 'maxspeed': 
                try: max_speed = float(v)
                except ValueError: pass
            elif k == 'highway': highway_type = v
            elif k == 'oneway': oneway = v

        # Lọc bỏ ngõ hẻm/vỉa hè để tạo đồ thị chuẩn cho xe tải
        if highway_type not in ALLOWED_HIGHWAY_TYPES:
            continue

        nd_refs = [nd.get('ref') for nd in way.findall('nd')]
        
        for i in range(len(nd_refs) - 1):
            start_node = nd_refs[i]
            end_node = nd_refs[i + 1]

            if start_node in nodes and end_node in nodes:
                lat1, lon1 = nodes[start_node]['lat'], nodes[start_node]['lon']
                lat2, lon2 = nodes[end_node]['lat'], nodes[end_node]['lon']
                length = haversine(lat1, lon1, lat2, lon2)

                if length > 0:
                    # Cấu trúc đã được đưa về đúng chuẩn 100% của bạn
                    def create_edge(src, dst, lat_s, lon_s, lat_d, lon_d):
                        return {
                            "edge_id": f"E_{src}_{dst}",
                            "road_name": road_name,
                            "max_speed_kmh": max_speed,
                            "start_node": {"node_id": src, "lat": lat_s, "lon": lon_s},
                            "end_node": {"node_id": dst, "lat": lat_d, "lon": lon_d},
                            "length_meters": round(length, 2)
                        }

                    if oneway == 'yes':
                        edges.append(create_edge(start_node, end_node, lat1, lon1, lat2, lon2))
                    elif oneway == '-1':
                        edges.append(create_edge(end_node, start_node, lat2, lon2, lat1, lon1))
                    else:
                        edges.append(create_edge(start_node, end_node, lat1, lon1, lat2, lon2))
                        edges.append(create_edge(end_node, start_node, lat2, lon2, lat1, lon1))

    print(f"Đã xử lý xong! Tìm thấy {len(edges)} vector đường đi.")
    with open(output_edges_path, 'w', encoding='utf-8') as f:
        json.dump(edges, f, ensure_ascii=False, indent=4)
    print(f"Đã lưu danh sách đường đi tại: {output_edges_path}")

if __name__ == "__main__":
    # Lấy thư mục hiện tại chứa file map_processor.py (thư mục data_ingestion)
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Lùi lại 1 cấp để ra thư mục gốc (Logictics-real-time)
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    
    # Trỏ vào thư mục data (ngang hàng với data_ingestion)
    osm_path = os.path.join(PROJECT_ROOT, "data", "hanoi_map.osm")
    edges_path = os.path.join(PROJECT_ROOT, "data", "edges_schema.json")
    
    process_osm_map(osm_path, edges_path)