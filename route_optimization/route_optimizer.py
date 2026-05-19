from typing import Any, Dict, Optional, List
import os
import sys
import time
import traceback

# Allow this file to be executed both from project root and from route_optimization/
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from optimizer_input_adapter import (
    build_optimization_input,
    validate_optimization_input,
    should_trigger_reroute,
)

from genetic_algorithm import run_genetic_algorithm
from graph_network import GraphNetwork
from route_builder import build_route


DEFAULT_MIN_AVG_SPEED = 5.0
DEFAULT_POPULATION_SIZE = 30
DEFAULT_GENERATIONS = 60
DEFAULT_MUTATION_RATE = 0.15

DEFAULT_EDGES_JSON = os.getenv(
    "EDGES_JSON",
    os.path.join(PROJECT_ROOT, "data", "edges_schema.json"),
)

_GRAPH_CACHE: Optional[GraphNetwork] = None
_GRAPH_CACHE_PATH: Optional[str] = None


def now_ms() -> int:
    return int(time.time() * 1000)


def build_no_reroute_response(vehicle_id: str, reason: str) -> Dict[str, Any]:
    return {
        "status": "skipped",
        "vehicle_id": vehicle_id,
        "reason": reason,
        "optimized": False,
        "updated_mongo": False,
        "timestamp": now_ms(),
    }


def build_error_response(vehicle_id: Optional[str], error: Exception) -> Dict[str, Any]:
    return {
        "status": "error",
        "vehicle_id": vehicle_id,
        "reason": str(error),
        "traceback": traceback.format_exc(),
        "optimized": False,
        "updated_mongo": False,
        "timestamp": now_ms(),
    }


def build_success_response(
    vehicle_id: str,
    old_assigned_route: list,
    new_assigned_route: list,
    ga_result: Dict[str, Any],
    mongo_updated: bool,
    route_generation_status: str,
    route_generation_reason: str = "",
) -> Dict[str, Any]:
    return {
        "status": "optimized",
        "vehicle_id": vehicle_id,
        "old_assigned_route": old_assigned_route,
        "new_assigned_route": new_assigned_route,
        "estimated_total_travel_time": ga_result.get("estimated_total_cost", 0),
        "optimized_customer_order": ga_result.get("optimized_customer_order", []),
        "optimized_customers": ga_result.get("optimized_customers", []),
        "generation_count": ga_result.get("generation_count"),
        "route_generation_status": route_generation_status,
        "route_generation_reason": route_generation_reason,
        "optimized": True,
        "updated_mongo": mongo_updated,
        "timestamp": now_ms(),
    }


def get_graph(edges_json: str = DEFAULT_EDGES_JSON) -> GraphNetwork:
    """
    Load graph once and reuse it across optimization cycles.
    """
    global _GRAPH_CACHE, _GRAPH_CACHE_PATH

    if _GRAPH_CACHE is not None and _GRAPH_CACHE_PATH == edges_json:
        return _GRAPH_CACHE

    graph = GraphNetwork()
    graph.load_from_schema(edges_json)

    _GRAPH_CACHE = graph
    _GRAPH_CACHE_PATH = edges_json

    print(
        f"✅ Loaded graph from {edges_json}: "
        f"{len(graph.nodes)} nodes, {len(graph.edges)} edges"
    )

    return graph


def _normalize_customer_objects(customers: Any) -> List[Dict[str, Any]]:
    """
    GA returns optimized_customers as a list of dictionaries.
    This helper is defensive in case customer data is dataclass-like or malformed.
    """
    result: List[Dict[str, Any]] = []

    if not isinstance(customers, list):
        return result

    for item in customers:
        if isinstance(item, dict):
            cust_id = item.get("cust_id")
            lat = item.get("latitude")
            lon = item.get("longitude")
        else:
            cust_id = getattr(item, "cust_id", None)
            lat = getattr(item, "latitude", None)
            lon = getattr(item, "longitude", None)

        if cust_id is None or lat is None or lon is None:
            continue

        try:
            result.append(
                {
                    "cust_id": str(cust_id),
                    "latitude": float(lat),
                    "longitude": float(lon),
                }
            )
        except (TypeError, ValueError):
            continue

    return result


def build_new_assigned_route(
    old_assigned_route: list,
    ga_result: Dict[str, Any],
    opt_input: Dict[str, Any],
    edges_json: str = DEFAULT_EDGES_JSON,
) -> Dict[str, Any]:
    """
    Build a new edge-level route after GA optimizes customer order.

    Steps:
    1. Get current edge from normalized optimization input.
    2. Get optimized customer order from GA.
    3. Load road graph from edges_schema.json.
    4. Use route_builder.build_route() to create edge path with shortest paths.
    5. Fallback to old route if graph route generation fails.

    Return:
        {
            "new_assigned_route": [...],
            "status": "graph_shortest_path" | "fallback_old_route" | "empty",
            "reason": "..."
        }
    """
    start_edge = (
        opt_input.get("current_edge_id")
        or opt_input.get("edge_id")
        or (old_assigned_route[0] if old_assigned_route else "")
    )

    if not start_edge:
        return {
            "new_assigned_route": [],
            "status": "empty",
            "reason": "Missing current edge id",
        }

    optimized_customers = _normalize_customer_objects(
        ga_result.get("optimized_customers", [])
    )

    # Fallback: if GA only returns IDs, use original customer objects in optimized order.
    if not optimized_customers:
        id_order = ga_result.get("optimized_customer_order", [])
        original_customers = _normalize_customer_objects(
            opt_input.get("remaining_customers", [])
        )
        by_id = {c["cust_id"]: c for c in original_customers}
        optimized_customers = [by_id[cid] for cid in id_order if cid in by_id]

    if not optimized_customers:
        return {
            "new_assigned_route": old_assigned_route or [],
            "status": "fallback_old_route",
            "reason": "No optimized customer coordinates available",
        }

    blocked_edges = opt_input.get("blocked_edges", [])

    try:
        graph = get_graph(edges_json)

        new_route = build_route(
            graph=graph,
            start_edge=start_edge,
            customer_order=optimized_customers,
            blocked_edges=blocked_edges,
        )

        if new_route:
            return {
                "new_assigned_route": new_route,
                "status": "graph_shortest_path",
                "reason": "Generated by graph shortest path after GA customer ordering",
            }

        return {
            "new_assigned_route": old_assigned_route or [],
            "status": "fallback_old_route",
            "reason": "Graph route generation returned empty route",
        }

    except Exception as error:
        return {
            "new_assigned_route": old_assigned_route or [],
            "status": "fallback_old_route",
            "reason": f"Graph route generation failed: {error}",
        }


def save_optimization_result_to_mongo(
    mongo_collection: Any,
    vehicle_id: str,
    ga_result: Dict[str, Any],
    old_assigned_route: list,
    new_assigned_route: list,
    route_generation_status: str,
    route_generation_reason: str,
) -> bool:
    """
    Update optimization result into MongoDB collection `assigned_routes`.

    Important fields consumed by backend/frontend:
    - vehicle_id
    - new_assigned_route
    - estimated_total_travel_time
    """
    estimated_total_travel_time = ga_result.get("estimated_total_cost", 0)

    update_doc = {
        "$set": {
            "vehicle_id": vehicle_id,
            "new_assigned_route": new_assigned_route,
            "estimated_total_travel_time": estimated_total_travel_time,
            "optimized_customer_order": ga_result.get("optimized_customer_order", []),
            "optimized_customers": ga_result.get("optimized_customers", []),
            "optimization_result": {
                "old_assigned_route": old_assigned_route,
                "new_assigned_route": new_assigned_route,
                "optimized_customer_order": ga_result.get(
                    "optimized_customer_order", []
                ),
                "optimized_customers": ga_result.get("optimized_customers", []),
                "estimated_total_cost": ga_result.get("estimated_total_cost"),
                "generation_count": ga_result.get("generation_count"),
                "route_generation_status": route_generation_status,
                "route_generation_reason": route_generation_reason,
                "optimized_at": now_ms(),
                "algorithm": "Genetic Algorithm + Graph Shortest Path",
            },
            "last_optimized_at": now_ms(),
            "route_status": "optimized",
        }
    }

    result = mongo_collection.update_one(
        {"vehicle_id": vehicle_id},
        update_doc,
        upsert=True,
    )

    return (
        getattr(result, "modified_count", 0) > 0
        or getattr(result, "matched_count", 0) > 0
        or getattr(result, "upserted_id", None) is not None
    )


def optimize_vehicle(
    vehicle_doc: Dict[str, Any],
    redis_client: Any,
    mongo_collection: Optional[Any] = None,
    min_avg_speed: float = DEFAULT_MIN_AVG_SPEED,
    population_size: int = DEFAULT_POPULATION_SIZE,
    generations: int = DEFAULT_GENERATIONS,
    mutation_rate: float = DEFAULT_MUTATION_RATE,
    force: bool = False,
    random_seed: Optional[int] = None,
    edges_json: str = DEFAULT_EDGES_JSON,
) -> Dict[str, Any]:
    vehicle_id = str(vehicle_doc.get("vehicle_id") or vehicle_doc.get("entity_id") or "")

    try:
        opt_input = build_optimization_input(
            vehicle_doc=vehicle_doc,
            redis_client=redis_client,
        )

        validate_optimization_input(opt_input)

        old_assigned_route = opt_input.get("assigned_route", [])

        reroute_needed = should_trigger_reroute(
            normalized_input=opt_input,
            min_avg_speed=min_avg_speed,
        )

        if not force and not reroute_needed:
            return build_no_reroute_response(
                vehicle_id=vehicle_id,
                reason="No blocked or congested edge detected",
            )

        ga_result = run_genetic_algorithm(
            opt_input=opt_input,
            population_size=population_size,
            generations=generations,
            mutation_rate=mutation_rate,
            random_seed=random_seed,
        )

        route_result = build_new_assigned_route(
            old_assigned_route=old_assigned_route,
            ga_result=ga_result,
            opt_input=opt_input,
            edges_json=edges_json,
        )

        new_assigned_route = route_result["new_assigned_route"]
        route_generation_status = route_result["status"]
        route_generation_reason = route_result["reason"]

        mongo_updated = False
        if mongo_collection is not None:
            mongo_updated = save_optimization_result_to_mongo(
                mongo_collection=mongo_collection,
                vehicle_id=vehicle_id,
                ga_result=ga_result,
                old_assigned_route=old_assigned_route,
                new_assigned_route=new_assigned_route,
                route_generation_status=route_generation_status,
                route_generation_reason=route_generation_reason,
            )

        return build_success_response(
            vehicle_id=vehicle_id,
            old_assigned_route=old_assigned_route,
            new_assigned_route=new_assigned_route,
            ga_result=ga_result,
            mongo_updated=mongo_updated,
            route_generation_status=route_generation_status,
            route_generation_reason=route_generation_reason,
        )

    except Exception as error:
        return build_error_response(vehicle_id=vehicle_id, error=error)


def optimize_many_vehicles(
    vehicle_docs: list,
    redis_client: Any,
    mongo_collection: Optional[Any] = None,
    min_avg_speed: float = DEFAULT_MIN_AVG_SPEED,
    population_size: int = DEFAULT_POPULATION_SIZE,
    generations: int = DEFAULT_GENERATIONS,
    mutation_rate: float = DEFAULT_MUTATION_RATE,
    force: bool = False,
    edges_json: str = DEFAULT_EDGES_JSON,
) -> Dict[str, Any]:
    results = []

    for vehicle_doc in vehicle_docs:
        new_route = vehicle_doc.get("new_assigned_route")
        needs_bootstrap = not (isinstance(new_route, list) and len(new_route) > 0)
        effective_force = bool(force or needs_bootstrap)

        result = optimize_vehicle(
            vehicle_doc=vehicle_doc,
            redis_client=redis_client,
            mongo_collection=mongo_collection,
            min_avg_speed=min_avg_speed,
            population_size=population_size,
            generations=generations,
            mutation_rate=mutation_rate,
            force=effective_force,
            edges_json=edges_json,
        )
        results.append(result)

    optimized_count = sum(1 for item in results if item.get("status") == "optimized")
    skipped_count = sum(1 for item in results if item.get("status") == "skipped")
    error_count = sum(1 for item in results if item.get("status") == "error")

    return {
        "total": len(results),
        "optimized_count": optimized_count,
        "skipped_count": skipped_count,
        "error_count": error_count,
        "results": results,
        "timestamp": now_ms(),
    }


def find_vehicle_by_id(mongo_collection: Any, vehicle_id: str) -> Optional[Dict[str, Any]]:
    return mongo_collection.find_one({"vehicle_id": vehicle_id})


def optimize_vehicle_by_id(
    vehicle_id: str,
    redis_client: Any,
    mongo_collection: Any,
    **kwargs: Any,
) -> Dict[str, Any]:
    vehicle_doc = find_vehicle_by_id(mongo_collection, vehicle_id)

    if not vehicle_doc:
        return {
            "status": "error",
            "vehicle_id": vehicle_id,
            "reason": "Vehicle not found in MongoDB",
            "optimized": False,
            "updated_mongo": False,
            "timestamp": now_ms(),
        }

    return optimize_vehicle(
        vehicle_doc=vehicle_doc,
        redis_client=redis_client,
        mongo_collection=mongo_collection,
        **kwargs,
    )


class FakeMongoUpdateResult:
    def __init__(
        self,
        matched_count: int = 1,
        modified_count: int = 1,
        upserted_id: Optional[str] = None,
    ):
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.upserted_id = upserted_id


class FakeMongoCollection:
    def __init__(self):
        self.docs = {}

    def insert_one_doc(self, doc: Dict[str, Any]) -> None:
        self.docs[doc["vehicle_id"]] = doc

    def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        vehicle_id = query.get("vehicle_id")
        return self.docs.get(vehicle_id)

    def find(self, query: Optional[Dict[str, Any]] = None):
        return list(self.docs.values())

    def update_one(
        self,
        query: Dict[str, Any],
        update_doc: Dict[str, Any],
        upsert: bool = False,
    ) -> FakeMongoUpdateResult:
        vehicle_id = query.get("vehicle_id")

        if vehicle_id not in self.docs:
            if not upsert:
                return FakeMongoUpdateResult(matched_count=0, modified_count=0)

            self.docs[vehicle_id] = {"vehicle_id": vehicle_id}
            set_doc = update_doc.get("$set", {})
            self.docs[vehicle_id].update(set_doc)
            return FakeMongoUpdateResult(
                matched_count=0,
                modified_count=0,
                upserted_id=vehicle_id,
            )

        set_doc = update_doc.get("$set", {})
        self.docs[vehicle_id].update(set_doc)

        return FakeMongoUpdateResult(matched_count=1, modified_count=1)


def run_worker_loop() -> None:
    """
    Long-running worker loop used in Docker/Kubernetes.
    """
    from pymongo import MongoClient
    import redis

    print("🚀 Khởi động Route Optimization Worker...")

    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://mongodb.default.svc.cluster.local:27017/",
    )
    redis_host = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")
    redis_port = int(os.getenv("REDIS_PORT_NUM", os.getenv("REDIS_PORT", 6379)))
    edges_json = os.getenv("EDGES_JSON", DEFAULT_EDGES_JSON)

    print(f"🔗 Đang kết nối MongoDB: {mongo_uri}")
    mongo_client = MongoClient(mongo_uri)
    db = mongo_client["traffic_system"]
    real_mongo_collection = db["assigned_routes"]

    print(f"🔗 Đang kết nối Redis: {redis_host}:{redis_port}")
    real_redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        decode_responses=True,
    )

    print(f"🗺️ Graph file: {edges_json}")
    # Load graph once at startup so route generation errors show early.
    get_graph(edges_json)

    print("✅ Đã kết nối thành công! Bắt đầu giám sát giao thông 24/7...")

    while True:
        try:
            trucks_cursor = real_mongo_collection.find({})
            vehicle_docs = list(trucks_cursor)

            if vehicle_docs:
                result = optimize_many_vehicles(
                    vehicle_docs=vehicle_docs,
                    redis_client=real_redis_client,
                    mongo_collection=real_mongo_collection,
                    force=False,
                    edges_json=edges_json,
                )

                if result["optimized_count"] > 0:
                    print(
                        f"[CẬP NHẬT] Đã tính toán lại đường đi cho "
                        f"{result['optimized_count']} xe."
                    )

                if result["error_count"] > 0:
                    print(
                        f"[CẢNH BÁO] Có {result['error_count']} lỗi trong vòng tối ưu."
                    )

        except Exception as e:
            print(f"❌ Lỗi vòng lặp: {e}")
            traceback.print_exc()

        time.sleep(5)


if __name__ == "__main__":
    run_worker_loop()