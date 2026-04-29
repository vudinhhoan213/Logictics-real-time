"""
optimizer_input_adapter.py

Input normalization for the Route Optimization module.

This version supports both naming styles:
- entity_id / entity_type from data ingestion and Kafka GPS messages
- vehicle_id from optimization, MongoDB and dashboard schemas

Important mapping:
    vehicle_id = vehicle_doc["vehicle_id"] if present
              = vehicle_doc["entity_id"] otherwise
"""

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set
import time


@dataclass
class Customer:
    cust_id: str
    latitude: float
    longitude: float


@dataclass
class EdgeTraffic:
    edge_id: str
    avg_speed: float = 0.0
    estimated_travel_time: float = 0.0
    vehicle_count: int = 0
    distance: float = 0.0
    max_speed: float = 0.0
    is_congested: bool = False
    last_updated: Optional[int] = None


@dataclass
class OptimizationInput:
    request_id: str
    vehicle_id: str
    entity_id: str
    entity_type: str
    current_edge_id: str
    distance_on_edge: float
    assigned_route: List[str]
    remaining_customers: List[Customer]
    blocked_edges: List[str]
    traffic_snapshot: Dict[str, EdgeTraffic]
    created_at: int


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _to_str(value: Any, default: str = "") -> str:
    value = _decode(value)
    if value is None:
        return default
    return str(value)


def _to_float(value: Any, default: float = 0.0) -> float:
    value = _decode(value)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    value = _decode(value)
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    value = _decode(value)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def _normalize_redis_hash(raw_hash: Dict[Any, Any]) -> Dict[str, Any]:
    result = {}
    for key, value in raw_hash.items():
        result[_to_str(key)] = _decode(value)
    return result


def get_vehicle_id(vehicle_doc: Dict[str, Any]) -> str:
    return _to_str(vehicle_doc.get("vehicle_id") or vehicle_doc.get("entity_id"))


def get_entity_id(vehicle_doc: Dict[str, Any]) -> str:
    return _to_str(vehicle_doc.get("entity_id") or vehicle_doc.get("vehicle_id"))


def get_entity_type(vehicle_doc: Dict[str, Any]) -> str:
    return _to_str(vehicle_doc.get("entity_type"), default="Truck")


def get_current_edge_id(vehicle_doc: Dict[str, Any]) -> str:
    return _to_str(vehicle_doc.get("edge_id") or vehicle_doc.get("current_edge_id"))


def parse_customers(raw_customers: Any) -> List[Customer]:
    customers: List[Customer] = []
    if not isinstance(raw_customers, list):
        return customers

    for item in raw_customers:
        if not isinstance(item, dict):
            continue

        cust_id = _to_str(item.get("cust_id"))
        lat = _to_float(item.get("latitude"))
        lon = _to_float(item.get("longitude"))

        if not cust_id:
            continue

        customers.append(Customer(cust_id=cust_id, latitude=lat, longitude=lon))

    return customers


def parse_assigned_route(raw_route: Any) -> List[str]:
    if not isinstance(raw_route, list):
        return []

    route: List[str] = []
    for edge_id in raw_route:
        edge_id_str = _to_str(edge_id)
        if edge_id_str:
            route.append(edge_id_str)

    return route


def get_blocked_edges(redis_client: Any) -> List[str]:
    try:
        raw_edges = redis_client.smembers("blocked_edges")
    except Exception:
        return []

    blocked_edges: List[str] = []
    for edge_id in raw_edges:
        edge_id_str = _to_str(edge_id)
        if edge_id_str:
            blocked_edges.append(edge_id_str)

    return sorted(set(blocked_edges))


def get_edge_traffic(redis_client: Any, edge_id: str) -> Optional[EdgeTraffic]:
    key = f"edge:{edge_id}"

    try:
        raw = redis_client.hgetall(key)
    except Exception:
        return None

    if not raw:
        return None

    data = _normalize_redis_hash(raw)

    return EdgeTraffic(
        edge_id=edge_id,
        avg_speed=_to_float(data.get("avg_speed")),
        estimated_travel_time=_to_float(data.get("estimated_travel_time")),
        vehicle_count=_to_int(data.get("vehicle_count")),
        distance=_to_float(data.get("distance")),
        max_speed=_to_float(data.get("max_speed")),
        is_congested=_to_bool(data.get("is_congested")),
        last_updated=_to_int(data.get("last_updated"), default=0),
    )


def build_traffic_snapshot(redis_client: Any, edge_ids: List[str]) -> Dict[str, EdgeTraffic]:
    snapshot: Dict[str, EdgeTraffic] = {}

    for edge_id in sorted(set(edge_ids)):
        if not edge_id:
            continue

        traffic = get_edge_traffic(redis_client, edge_id)
        if traffic is not None:
            snapshot[edge_id] = traffic

    return snapshot


def build_optimization_input(
    vehicle_doc: Dict[str, Any],
    redis_client: Any,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    vehicle_id = get_vehicle_id(vehicle_doc)
    entity_id = get_entity_id(vehicle_doc)
    entity_type = get_entity_type(vehicle_doc)
    current_edge_id = get_current_edge_id(vehicle_doc)

    assigned_route = parse_assigned_route(vehicle_doc.get("assigned_route", []))
    remaining_customers = parse_customers(vehicle_doc.get("remaining_customers", []))
    blocked_edges = get_blocked_edges(redis_client)

    important_edges: Set[str] = set()
    important_edges.add(current_edge_id)
    important_edges.update(assigned_route)
    important_edges.update(blocked_edges)

    traffic_snapshot = build_traffic_snapshot(redis_client, list(important_edges))

    now = int(time.time() * 1000)

    normalized = OptimizationInput(
        request_id=request_id or f"OPT_{vehicle_id}_{now}",
        vehicle_id=vehicle_id,
        entity_id=entity_id,
        entity_type=entity_type,
        current_edge_id=current_edge_id,
        distance_on_edge=_to_float(vehicle_doc.get("distance_on_edge")),
        assigned_route=assigned_route,
        remaining_customers=remaining_customers,
        blocked_edges=blocked_edges,
        traffic_snapshot=traffic_snapshot,
        created_at=now,
    )

    return optimization_input_to_dict(normalized)


def optimization_input_to_dict(data: OptimizationInput) -> Dict[str, Any]:
    return asdict(data)


def validate_optimization_input(data: Dict[str, Any]) -> None:
    required_fields = [
        "request_id",
        "vehicle_id",
        "entity_id",
        "entity_type",
        "current_edge_id",
        "assigned_route",
        "remaining_customers",
        "blocked_edges",
        "traffic_snapshot",
    ]

    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing field: {field}")

    if not data["vehicle_id"]:
        raise ValueError("vehicle_id must not be empty")

    if not data["current_edge_id"]:
        raise ValueError("current_edge_id must not be empty")

    if not isinstance(data["assigned_route"], list):
        raise ValueError("assigned_route must be a list")

    if not isinstance(data["remaining_customers"], list):
        raise ValueError("remaining_customers must be a list")

    if not isinstance(data["blocked_edges"], list):
        raise ValueError("blocked_edges must be a list")

    if not isinstance(data["traffic_snapshot"], dict):
        raise ValueError("traffic_snapshot must be a dict")

    for customer in data["remaining_customers"]:
        if "cust_id" not in customer:
            raise ValueError("Each customer must have cust_id")
        if "latitude" not in customer or "longitude" not in customer:
            raise ValueError("Each customer must have latitude and longitude")


def should_trigger_reroute(
    normalized_input: Dict[str, Any],
    min_avg_speed: float = 5.0,
) -> bool:
    assigned_route = set(normalized_input.get("assigned_route", []))
    blocked_edges = set(normalized_input.get("blocked_edges", []))
    traffic_snapshot = normalized_input.get("traffic_snapshot", {})

    if assigned_route.intersection(blocked_edges):
        return True

    for edge_id in assigned_route:
        traffic = traffic_snapshot.get(edge_id)
        if not traffic:
            continue

        avg_speed = _to_float(traffic.get("avg_speed"))
        is_congested = _to_bool(traffic.get("is_congested"))

        if is_congested:
            return True

        if avg_speed > 0 and avg_speed <= min_avg_speed:
            return True

    return False


class FakeRedis:
    def __init__(self):
        self.sets = {
            "blocked_edges": {"E_106_TaySon"}
        }
        self.hashes = {
            "edge:E_105_NgTrai": {
                "avg_speed": "25.0",
                "estimated_travel_time": "30.5",
                "vehicle_count": "20",
                "distance": "210.0",
                "max_speed": "50",
                "is_congested": "false",
                "last_updated": "1711864800000",
            },
            "edge:E_106_TaySon": {
                "avg_speed": "3.5",
                "estimated_travel_time": "200.0",
                "vehicle_count": "120",
                "distance": "700.0",
                "max_speed": "50",
                "is_congested": "true",
                "last_updated": "1711864800000",
            },
        }

    def smembers(self, key: str):
        return self.sets.get(key, set())

    def hgetall(self, key: str):
        return self.hashes.get(key, {})


if __name__ == "__main__":
    vehicle_doc = {
        "entity_id": "Truck_001",
        "entity_type": "Truck",
        "edge_id": "E_105_NgTrai",
        "distance_on_edge": 150.5,
        "assigned_route": ["E_105_NgTrai", "E_106_TaySon"],
        "remaining_customers": [
            {
                "cust_id": "Cust_001",
                "latitude": 21.012345,
                "longitude": 105.812345,
            },
            {
                "cust_id": "Cust_002",
                "latitude": 21.022345,
                "longitude": 105.822345,
            },
        ],
    }

    redis_client = FakeRedis()
    opt_input = build_optimization_input(vehicle_doc, redis_client)
    validate_optimization_input(opt_input)

    print("Normalized optimization input:")
    print(opt_input)

    print("\nShould trigger reroute?")
    print(should_trigger_reroute(opt_input))
