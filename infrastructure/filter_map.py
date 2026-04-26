import json
import os
import shutil

# Tọa độ khung chữ nhật (Bounding Box) của các Quận
DISTRICTS = {
    "DongDa": {
        "min_lat": 21.0000, "max_lat": 21.0320,
        "min_lon": 105.8000, "max_lon": 105.8450
    },
    "BaDinh": {
        "min_lat": 21.0170, "max_lat": 21.0480,
        "min_lon": 105.7960, "max_lon": 105.8500
    }
}

# CHỌN QUẬN BẠN MUỐN Ở ĐÂY
SELECTED_DISTRICT = "DongDa" 
BOUNDS = DISTRICTS[SELECTED_DISTRICT]

def is_in_bounds(lat, lon):
    return (BOUNDS["min_lat"] <= lat <= BOUNDS["max_lat"]) and \
           (BOUNDS["min_lon"] <= lon <= BOUNDS["max_lon"])

if __name__ == "__main__":
    # Lấy thư mục chứa file filter_map.py (chính là thư mục infrastructure)
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Lùi ra ngoài 1 cấp để lấy thư mục gốc của dự án
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

    # Đường dẫn chuẩn trỏ vào thư mục data
    target_file = os.path.join(PROJECT_ROOT, "data", "edges_schema.json")
    backup_file = os.path.join(PROJECT_ROOT, "data", "edges_schema_full_backup.json")

    # 1. TỰ ĐỘNG BACKUP ĐỂ BẢO VỆ DATA GỐC
    if not os.path.exists(backup_file):
        if os.path.exists(target_file):
            import shutil
            shutil.copy2(target_file, backup_file)
            print(f"[OK] Đã tự động tạo bản sao lưu an toàn tại: {backup_file}")
        else:
            print(f"[LỖI] Không tìm thấy file {target_file}")
            exit()

    # 2. ĐỌC TỪ FILE BACKUP
    print("Đang đọc bản đồ gốc...")
    with open(backup_file, 'r', encoding='utf-8') as f:
        all_edges = json.load(f)

    # 3. LỌC ĐƯỜNG
    filtered_edges = []
    for edge in all_edges:
        start = edge['start_node']
        end = edge['end_node']
        if is_in_bounds(start['lat'], start['lon']) and is_in_bounds(end['lat'], end['lon']):
            filtered_edges.append(edge)

    # 4. GHI ĐÈ TRỰC TIẾP
    with open(target_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_edges, f, ensure_ascii=False, indent=2)

    print(f"[THÀNH CÔNG] Đã GHI ĐÈ vào edges_schema.json!")
    print(f"Số lượng đường của {SELECTED_DISTRICT} hiện tại là: {len(filtered_edges)}")