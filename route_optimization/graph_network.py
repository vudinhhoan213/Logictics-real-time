import heapq
import json
from typing import Dict, List, Tuple, Optional


class GraphNetwork:
    def __init__(self):
        self.nodes: Dict[str, dict] = {}
        self.edges: Dict[str, dict] = {}
        self.adjacency: Dict[str, List[dict]] = {}
        self.traffic_adapter = None

    def set_traffic_adapter(self, adapter):
        self.traffic_adapter = adapter

    def load_from_schema(self, path: str):
        with open(path, "r", encoding="utf-8") as f:
            raw_edges = json.load(f)

        self.nodes.clear()
        self.edges.clear()
        self.adjacency.clear()

        for item in raw_edges:
            edge_id = item["edge_id"]
            start_node = item["start_node"]
            end_node = item["end_node"]

            start_id = start_node["node_id"]
            end_id = end_node["node_id"]

            self.nodes[start_id] = {
                "node_id": start_id,
                "lat": float(start_node["lat"]),
                "lon": float(start_node["lon"]),
            }
            self.nodes[end_id] = {
                "node_id": end_id,
                "lat": float(end_node["lat"]),
                "lon": float(end_node["lon"]),
            }

            self.edges[edge_id] = {
                "edge_id": edge_id,
                "road_name": item.get("road_name", "UNKNOWN"),
                "max_speed_kmh": float(item.get("max_speed_kmh", 40.0)),
                "length_meters": float(item.get("length_meters", 1.0)),
                "start_node_id": start_id,
                "end_node_id": end_id,
            }

            self.adjacency.setdefault(start_id, []).append({
                "to_node": end_id,
                "edge_id": edge_id,
            })
            self.adjacency.setdefault(end_id, [])

    def edge_start_node(self, edge_id: str) -> Optional[str]:
        edge = self.edges.get(edge_id)
        return edge["start_node_id"] if edge else None

    def edge_end_node(self, edge_id: str) -> Optional[str]:
        edge = self.edges.get(edge_id)
        return edge["end_node_id"] if edge else None

    def edge_cost(self, edge_id: str, blocked_edges: Optional[List[str]] = None) -> float:
        if edge_id not in self.edges:
            return float("inf")

        blocked_set = set(blocked_edges or [])
        if edge_id in blocked_set:
            return float("inf")

        if self.traffic_adapter is not None:
            try:
                state = self.traffic_adapter.get_edge_state(edge_id)
                if state:
                    if state.get("is_congested") is True:
                        return float("inf")
                    ett = state.get("estimated_travel_time")
                    if ett is not None:
                        ett = float(ett)
                        if ett > 0:
                            return ett
            except Exception:
                pass

        edge = self.edges[edge_id]
        length_m = edge["length_meters"]
        speed_kmh = max(edge["max_speed_kmh"], 1.0)
        speed_mps = speed_kmh * 1000.0 / 3600.0
        return length_m / speed_mps

    def shortest_path(
        self,
        start_node: str,
        end_node: str,
        blocked_edges: Optional[List[str]] = None
    ) -> Tuple[List[str], float]:
        if start_node not in self.nodes or end_node not in self.nodes:
            return [], float("inf")

        if start_node == end_node:
            return [], 0.0

        dist = {node_id: float("inf") for node_id in self.nodes}
        prev = {}

        dist[start_node] = 0.0
        heap = [(0.0, start_node)]

        while heap:
            current_cost, current_node = heapq.heappop(heap)

            if current_cost > dist[current_node]:
                continue

            if current_node == end_node:
                break

            for neighbor in self.adjacency.get(current_node, []):
                next_node = neighbor["to_node"]
                edge_id = neighbor["edge_id"]

                cost = self.edge_cost(edge_id, blocked_edges=blocked_edges)
                if cost == float("inf"):
                    continue

                new_cost = current_cost + cost
                if new_cost < dist[next_node]:
                    dist[next_node] = new_cost
                    prev[next_node] = (current_node, edge_id)
                    heapq.heappush(heap, (new_cost, next_node))

        if dist[end_node] == float("inf"):
            return [], float("inf")

        path_edges = []
        cur = end_node
        while cur != start_node:
            parent_node, via_edge = prev[cur]
            path_edges.append(via_edge)
            cur = parent_node

        path_edges.reverse()
        return path_edges, dist[end_node]

    def nearest_node_to_point(self, lat: float, lon: float) -> Optional[str]:
        best_node = None
        best_dist = float("inf")

        for node_id, node in self.nodes.items():
            dlat = node["lat"] - lat
            dlon = node["lon"] - lon
            d2 = dlat * dlat + dlon * dlon
            if d2 < best_dist:
                best_dist = d2
                best_node = node_id

        return best_node