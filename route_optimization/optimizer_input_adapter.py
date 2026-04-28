"""
optimizer_input_adapter.py

Lop chuan hoa input cho module Route Optimization.

Muc tieu:
- Khong de Genetic Algorithm doc truc tiep MongoDB / Redis.
- Gom du lieu tu MongoDB vehicle_doc va Redis traffic state ve mot object thong nhat.
- Giup module GA de test offline bang du lieu fake.

Cach dung co ban:

    from optimizer_input_adapter import build_optimization_input, validate_optimization_input

    opt_input = build_optimization_input(vehicle_doc, redis_client)
    validate_optimization_input(opt_input)

    # Sau do dua opt_input vao GA:
    # result = run_genetic_algorithm(opt_input)

Redis schema ky vong:
- Set blocked_edges:
    blocked_edges = {"E_001", "E_002", ...}

- Hash edge status:
    key = "edge:{edge_id}"
    fields:
        avg_speed
        estimated_travel_time
        vehicle_count
        distance
        max_speed
        is_congested
        last_updated
"""

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set
import time


# =========================
# Dataclass definitions
# =========================

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
    current_edge_id: str
    distance_on_edge: float
    assigned_route: List[str]
    remaining_customers: List[Customer]
    blocked_edges: List[str]
    traffic_snapshot: Dict[str, EdgeTraffic]
    created_at: int


# =========================
# Small conversion helpers
# =========================

def _decode(value: Any) -> Any:
    """Redis may return bytes. Convert bytes to string."""
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

    value = str(value).strip().lower()
    return value in ("1", "true", "yes", "y")


def _normalize_redis_hash(raw_hash: Dict[Any, Any]) -> Dict[str, Any]:
    """
    Convert Redis hash from {b'key': b'value'} to {'key': 'value'}.
    """
    result = {}
    for key, value in raw_hash.items():
        result[_to_str(key)] = _decode(value)
    return result


# =========================
# MongoDB vehicle parsing
# =========================

def parse_customers(raw_customers: Any) -> List[Customer]:
    """
    Convert MongoDB remaining_customers to List[Customer].
    Invalid customers are skipped.
    """
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

        customers.append(
            Customer(
                cust_id=cust_id,
                latitude=lat,
                longitude=lon,
            )
        )

    return customers


def parse_assigned_route(raw_route: Any) -> List[str]:
    """
    Convert assigned_route to List[str].
    """
    if not isinstance(raw_route, list):
        return []

    route: List[str] = []
    for edge_id in raw_route:
        edge_id_str = _to_str(edge_id)
        if edge_id_str:
            route.append(edge_id_str)

    return route


# =========================
# Redis parsing
# =========================

def get_blocked_edges(redis_client: Any) -> List[str]:
    """
    Read blocked edges from Redis set named 'blocked_edges'.

    If Redis is unavailable or key does not exist, return empty list.
    """
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
    """
    Read traffic state for one edge from Redis hash: edge:{edge_id}.
    """
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
    """
    Build traffic snapshot for important edges only.

    Nen lay:
    - cac edge trong assigned_route
    - cac edge trong blocked_edges
    - current_edge_id
    """
    snapshot: Dict[str, EdgeTraffic] = {}

    for edge_id in sorted(set(edge_ids)):
        if not edge_id:
            continue

        traffic = get_edge_traffic(redis_client, edge_id)
        if traffic is not None:
            snapshot[edge_id] = traffic

    return snapshot


# =========================
# Main adapter
# =========================

def build_optimization_input(
    vehicle_doc: Dict[str, Any],
    redis_client: Any,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build normalized input for Genetic Algorithm.

    vehicle_doc expected fields:
        vehicle_id
        edge_id
        distance_on_edge
        assigned_route
        remaining_customers
    """
    vehicle_id = _to_str(vehicle_doc.get("vehicle_id"))
    current_edge_id = _to_str(vehicle_doc.get("edge_id"))

    assigned_route = parse_assigned_route(vehicle_doc.get("assigned_route", []))
    remaining_customers = parse_customers(vehicle_doc.get("remaining_customers", []))
    blocked_edges = get_blocked_edges(redis_client)

    important_edges: Set[str] = set()
    important_edges.add(current_edge_id)
    important_edges.update(assigned_route)
    important_edges.update(blocked_edges)

    traffic_snapshot = build_traffic_snapshot(redis_client, list(important_edges))

    now_ms = int(time.time() * 1000)

    normalized = OptimizationInput(
        request_id=request_id or f"OPT_{vehicle_id}_{now_ms}",
        vehicle_id=vehicle_id,
        current_edge_id=current_edge_id,
        distance_on_edge=_to_float(vehicle_doc.get("distance_on_edge")),
        assigned_route=assigned_route,
        remaining_customers=remaining_customers,
        blocked_edges=blocked_edges,
        traffic_snapshot=traffic_snapshot,
        created_at=now_ms,
    )

    return optimization_input_to_dict(normalized)


def optimization_input_to_dict(data: OptimizationInput) -> Dict[str, Any]:
    """
    Convert dataclass object to plain dict.
    This is easier to pass into existing GA code.
    """
    return asdict(data)


# =========================
# Validation
# =========================

def validate_optimization_input(data: Dict[str, Any]) -> None:
    """
    Raise ValueError if normalized input is invalid.
    """
    required_fields = [
        "request_id",
        "vehicle_id",
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


# =========================
# Reroute trigger helper
# =========================

def should_trigger_reroute(
    normalized_input: Dict[str, Any],
    min_avg_speed: float = 5.0,
) -> bool:
    """
    Decide whether GA should run.

    Trigger if:
    - current assigned_route contains blocked edge
    - any edge in assigned_route has avg_speed <= min_avg_speed
    - any edge in assigned_route has is_congested = True
    """
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


# =========================
# Demo without real Redis
# =========================

class FakeRedis:
    """
    Simple fake Redis for local testing.
    Remove this class in production if not needed.
    """
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
    # Local test example
    vehicle_doc = {
        "vehicle_id": "Truck_001",
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
