"""
Map Matcher

"""

import json
import math
import logging
from typing import Dict, List

logger = logging.getLogger("MapMatcher")


class MapMatcher:
    """
    Snap GPS tọa độ vào edge gần nhất.
    Load dữ liệu từ edges_schema.json — cùng file mà bot_simulation.py dùng.
    """

    def __init__(self, edges_json_path: str):
        logger.info(f"Đang load edges từ: {edges_json_path}")
        self._edges: List[dict] = []
        self._edge_map: Dict[str, dict] = {}

        try:
            with open(edges_json_path, "r", encoding="utf-8") as f:
                raw_edges = json.load(f)

            for edge in raw_edges:
                eid = edge.get("edge_id")
                if not eid:
                    continue
                self._edges.append(edge)
                self._edge_map[eid] = edge

            logger.info(f"Đã load {len(self._edges)} edges.")

        except FileNotFoundError:
            logger.warning(
                f"Không tìm thấy '{edges_json_path}'. "
                "Chạy data_ingestion/map_processor.py trước. Dùng MOCK tạm thời."
            )
        except json.JSONDecodeError as e:
            logger.error(f"File JSON bị lỗi: {e}")

    def snap_to_edge(self, lat: float, lon: float) -> str:
        """Trả về edge_id gần nhất với (lat, lon). Fallback → 'UNKNOWN'."""
        if not self._edges:
            return "UNKNOWN"

        best_eid  = "UNKNOWN"
        best_dist = math.inf

        for edge in self._edges:
            try:
                s = edge["start_node"]
                e = edge["end_node"]
                d = _point_to_segment_dist(
                    lat, lon,
                    s["lat"], s["lon"],
                    e["lat"], e["lon"],
                )
                if d < best_dist:
                    best_dist = d
                    best_eid  = edge["edge_id"]
            except (KeyError, TypeError):
                continue

        return best_eid

    def get_edge_length(self, edge_id: str) -> float:
        """length_meters từ edges_schema.json. Fallback = 500.0."""
        edge = self._edge_map.get(edge_id)
        if edge:
            return float(edge.get("length_meters", 500.0))
        return 500.0

    def get_edge_max_speed(self, edge_id: str) -> float:
        """max_speed_kmh. Fallback = 40.0 (default của map_processor)."""
        edge = self._edge_map.get(edge_id)
        if edge:
            return float(edge.get("max_speed_kmh", 40.0))
        return 40.0

    def get_edge_road_name(self, edge_id: str) -> str:
        edge = self._edge_map.get(edge_id)
        if edge:
            return edge.get("road_name", "Đường không tên")
        return "UNKNOWN"

    def get_all_edge_ids(self) -> List[str]:
        return list(self._edge_map.keys())

    def is_loaded(self) -> bool:
        return len(self._edges) > 0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _point_to_segment_dist(
    px: float, py: float,
    ax: float, ay: float,
    bx: float, by: float,
) -> float:
    dx = bx - ax
    dy = by - ay
    if dx == 0 and dy == 0:
        return _haversine_m(px, py, ax, ay)
    t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    return _haversine_m(px, py, ax + t * dx, ay + t * dy)