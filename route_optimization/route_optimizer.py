"""
route_optimizer.py

Integration layer cho module Route Optimization.

Backend dashboard nghe MongoDB collection `assigned_routes`
va emit event `route_optimized` voi format:
    {
        vehicle_id: updatedData.vehicle_id,
        path: updatedData.new_assigned_route,
        time: updatedData.estimated_total_travel_time
    }

File nay ghi cac field:
- vehicle_id
- new_assigned_route
- estimated_total_travel_time

Ban fix nay:
- GA toi uu thu tu khach hang (`optimized_customer_order`).
- Convert thu tu khach hang -> route edge_id bang graph + shortest path.
- Chi fallback ve old_assigned_route neu graph khong sinh duoc route hop le.
"""

from typing import Any, Dict, Optional
import os
import time
import traceback

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

_ROUTE_GRAPH = None


class _TrafficSnapshotAdapter:
    """
    Adapter nho de GraphNetwork.edge_cost() doc duoc traffic_snapshot
    tu Redis snapshot da normalize trong optimizer_input_adapter.
    """

    def __init__(self, traffic_snapshot: Dict[str, Any]):
        self.traffic_snapshot = traffic_snapshot or {}

    def get_edge_state(self, edge_id: str) -> Optional[Dict[str, Any]]:
        state = self.traffic_snapshot.get(edge_id)
        if state is None:
            return None

        if isinstance(state, dict):
            return state

        # Safety fallback neu sau nay ai do truyen dataclass/object.
        if hasattr(state, "__dict__"):
            return dict(state.__dict__)

        return None


def get_route_graph() -> GraphNetwork:
    """
    Load graph 1 lan trong worker. Trong container K8s, file data nam o:
        /app/data/edges_schema.json
    """
    global _ROUTE_GRAPH

    if _ROUTE_GRAPH is None:
        schema_path = os.getenv("EDGES_JSON", "/app/data/edges_schema.json")
        graph = GraphNetwork()
        graph.load_from_schema(schema_path)
        _ROUTE_GRAPH = graph

    return _ROUTE_GRAPH


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
        "optimized": True,
        "updated_mongo": mongo_updated,
        "timestamp": now_ms(),
    }


def _customer_order_from_ga(
    ga_result: Dict[str, Any],
    opt_input: Dict[str, Any],
) -> list:
    optimized_customers = ga_result.get("optimized_customers") or []
    optimized_customer_order = ga_result.get("optimized_customer_order") or []

    if optimized_customer_order and optimized_customers:
        customer_by_id = {
            str(customer.get("cust_id")): customer
            for customer in optimized_customers
            if isinstance(customer, dict)
        }

        ordered_customers = [
            customer_by_id[cust_id]
            for cust_id in optimized_customer_order
            if cust_id in customer_by_id
        ]

        if ordered_customers:
            return ordered_customers

    return optimized_customers or opt_input.get("remaining_customers", [])


def build_new_assigned_route(
    old_assigned_route: list,
    ga_result: Dict[str, Any],
    opt_input: Dict[str, Any],
) -> list:
    """
    Sinh edge route that tu thu tu customer do GA toi uu.

    Flow:
    - Lay current_edge_id lam diem xuat phat.
    - Lay optimized_customers theo thu tu GA.
    - Map tung customer ve nearest graph node.
    - Dung shortest_path de noi cac diem giao.
    - Tra ve list edge_id moi cho dashboard.
    """
    start_edge = (
        opt_input.get("current_edge_id")
        or (old_assigned_route[0] if old_assigned_route else "")
    )

    if not start_edge:
        return old_assigned_route or []

    customer_order = _customer_order_from_ga(ga_result, opt_input)
    if not customer_order:
        return old_assigned_route or []

    graph = get_route_graph()
    graph.set_traffic_adapter(
        _TrafficSnapshotAdapter(opt_input.get("traffic_snapshot", {}))
    )

    blocked_edges = opt_input.get("blocked_edges", [])

    route_after_current_edge = build_route(
        graph=graph,
        start_edge=start_edge,
        customer_order=customer_order,
        blocked_edges=blocked_edges,
    )

    if not route_after_current_edge:
        return old_assigned_route or []

    return [start_edge] + route_after_current_edge


def _static_edge_cost_seconds(graph: GraphNetwork, edge_id: str) -> Optional[float]:
    edge = graph.edges.get(edge_id)
    if not edge:
        return None

    length_m = float(edge.get("length_meters", 0.0))
    speed_kmh = max(float(edge.get("max_speed_kmh", 40.0)), 1.0)
    speed_mps = speed_kmh * 1000.0 / 3600.0

    if length_m <= 0 or speed_mps <= 0:
        return None

    return length_m / speed_mps


def estimate_route_travel_time_seconds(
    route_edges: list,
    opt_input: Dict[str, Any],
) -> Optional[float]:
    """
    Dashboard dang hien ETA bang `time / 60`, nen gia tri luu Mongo nen la giay.

    GraphNetwork.edge_cost():
    - Uu tien estimated_travel_time tu traffic snapshot neu co.
    - Neu edge bi congested/blocked va cost = inf, fallback static cost de ETA
      khong bi vo trong UI.
    """
    if not route_edges:
        return None

    graph = get_route_graph()
    graph.set_traffic_adapter(
        _TrafficSnapshotAdapter(opt_input.get("traffic_snapshot", {}))
    )

    total = 0.0
    blocked_edges = opt_input.get("blocked_edges", [])

    for edge_id in route_edges:
        cost = graph.edge_cost(edge_id, blocked_edges=blocked_edges)

        if cost == float("inf") or cost is None:
            static_cost = _static_edge_cost_seconds(graph, edge_id)
            if static_cost is None:
                continue
            cost = static_cost

        try:
            cost = float(cost)
        except (TypeError, ValueError):
            continue

        if cost > 0:
            total += cost

    return total if total > 0 else None


def save_optimization_result_to_mongo(
    mongo_collection: Any,
    vehicle_id: str,
    ga_result: Dict[str, Any],
    old_assigned_route: list,
    new_assigned_route: list,
) -> bool:
    """
    Cap nhat ket qua optimization vao MongoDB theo schema dashboard.

    Collection ky vong:
        traffic_system.assigned_routes

    Fields quan trong cho backend/frontend:
        vehicle_id
        new_assigned_route
        estimated_total_travel_time
    """
    estimated_total_travel_time = ga_result.get("estimated_total_cost", 0)

    update_doc = {
        "$set": {
            "vehicle_id": vehicle_id,
            "new_assigned_route": new_assigned_route,
            "estimated_total_travel_time": estimated_total_travel_time,
            "optimized_customer_order": ga_result.get("optimized_customer_order", []),
            "optimization_result": {
                "old_assigned_route": old_assigned_route,
                "new_assigned_route": new_assigned_route,
                "optimized_customer_order": ga_result.get("optimized_customer_order", []),
                "estimated_total_cost": ga_result.get("estimated_total_cost"),
                "estimated_total_cost_source": ga_result.get("estimated_total_cost_source"),
                "generation_count": ga_result.get("generation_count"),
                "optimized_at": now_ms(),
                "algorithm": "Genetic Algorithm + Graph shortest path",
                "note": "new_assigned_route is generated from GA customer order using graph shortest path",
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

        new_assigned_route = build_new_assigned_route(
            old_assigned_route=old_assigned_route,
            ga_result=ga_result,
            opt_input=opt_input,
        )

        route_travel_time_seconds = estimate_route_travel_time_seconds(
            route_edges=new_assigned_route,
            opt_input=opt_input,
        )

        if route_travel_time_seconds is not None:
            ga_result = {
                **ga_result,
                "estimated_total_cost": route_travel_time_seconds,
                "estimated_total_cost_source": "graph_edge_cost_seconds",
            }

        mongo_updated = False
        if mongo_collection is not None:
            mongo_updated = save_optimization_result_to_mongo(
                mongo_collection=mongo_collection,
                vehicle_id=vehicle_id,
                ga_result=ga_result,
                old_assigned_route=old_assigned_route,
                new_assigned_route=new_assigned_route,
            )

        return build_success_response(
            vehicle_id=vehicle_id,
            old_assigned_route=old_assigned_route,
            new_assigned_route=new_assigned_route,
            ga_result=ga_result,
            mongo_updated=mongo_updated,
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


if __name__ == "__main__":
    from pymongo import MongoClient
    import redis

    print("🚀 Khởi động Route Optimization Worker...")

    # 1. Kết nối thật vào các hệ thống
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb.default.svc.cluster.local:27017/")
    redis_host = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")
    redis_port = int(os.getenv("REDIS_PORT_NUM", os.getenv("REDIS_PORT", 6379)))

    print(f"🔗 Đang kết nối MongoDB: {mongo_uri}")
    mongo_client = MongoClient(mongo_uri)
    db = mongo_client["traffic_system"]
    real_mongo_collection = db["assigned_routes"]

    print(f"🔗 Đang kết nối Redis: {redis_host}:{redis_port}")
    real_redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    print("✅ Đã kết nối thành công! Bắt đầu giám sát giao thông 24/7...")

    # 2. Vòng lặp vĩnh cửu của Kubernetes
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
                )

                if result["optimized_count"] > 0:
                    print(
                        f"[CẬP NHẬT] Đã tính toán lại đường đi cho "
                        f"{result['optimized_count']} xe do kẹt xe!"
                    )

                if result["error_count"] > 0:
                    print(f"[CẢNH BÁO] Có {result['error_count']} xe lỗi khi tối ưu.")
                    for item in result["results"]:
                        if item.get("status") == "error":
                            print(f"  - {item.get('vehicle_id')}: {item.get('reason')}")

        except Exception as e:
            print(f"❌ Lỗi vòng lặp: {e}")

        time.sleep(5)
