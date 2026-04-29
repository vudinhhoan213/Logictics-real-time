"""
route_optimizer.py

Integration layer cho module Route Optimization.

Vai tro:
- Lay vehicle_doc tu MongoDB.
- Lay traffic state tu Redis thong qua optimizer_input_adapter.py.
- Kiem tra co can reroute hay khong.
- Neu can, chay Genetic Algorithm.
- Cap nhat ket qua route moi vao MongoDB.

File nay dong vai tro "bo dieu phoi" cua module C.

Cau truc khuyen dung trong folder route_optimization:

    route_optimization/
    ├── optimizer_input_adapter.py
    ├── genetic_algorithm.py
    └── route_optimizer.py
"""

from typing import Any, Dict, Optional
import time
import traceback

from optimizer_input_adapter import (
    build_optimization_input,
    validate_optimization_input,
    should_trigger_reroute,
)

from genetic_algorithm import run_genetic_algorithm


DEFAULT_MIN_AVG_SPEED = 5.0
DEFAULT_POPULATION_SIZE = 30
DEFAULT_GENERATIONS = 60
DEFAULT_MUTATION_RATE = 0.15


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
    ga_result: Dict[str, Any],
    mongo_updated: bool,
) -> Dict[str, Any]:
    return {
        "status": "optimized",
        "vehicle_id": vehicle_id,
        "old_assigned_route": old_assigned_route,
        "optimized_customer_order": ga_result.get("optimized_customer_order", []),
        "optimized_customers": ga_result.get("optimized_customers", []),
        "estimated_total_cost": ga_result.get("estimated_total_cost"),
        "generation_count": ga_result.get("generation_count"),
        "optimized": True,
        "updated_mongo": mongo_updated,
        "timestamp": now_ms(),
    }


def save_optimization_result_to_mongo(
    mongo_collection: Any,
    vehicle_id: str,
    ga_result: Dict[str, Any],
    old_assigned_route: list,
) -> bool:
    """
    Cap nhat ket qua optimization vao MongoDB.

    GA hien tai toi uu thu tu khach hang. Khi chua co graph/pathfinding that,
    file nay chua ghi de assigned_route bang edge route moi.
    """
    optimized_customer_order = ga_result.get("optimized_customer_order", [])

    update_doc = {
        "$set": {
            "optimized_customer_order": optimized_customer_order,
            "optimization_result": {
                "old_assigned_route": old_assigned_route,
                "optimized_customer_order": optimized_customer_order,
                "estimated_total_cost": ga_result.get("estimated_total_cost"),
                "generation_count": ga_result.get("generation_count"),
                "optimized_at": now_ms(),
                "algorithm": "Genetic Algorithm",
            },
            "last_optimized_at": now_ms(),
            "route_status": "optimized",
        }
    }

    result = mongo_collection.update_one(
        {"vehicle_id": vehicle_id},
        update_doc,
    )

    return result.modified_count > 0 or result.matched_count > 0


def save_edge_route_to_mongo(
    mongo_collection: Any,
    vehicle_id: str,
    new_assigned_route: list,
    ga_result: Dict[str, Any],
    old_assigned_route: list,
) -> bool:
    """
    Dung ham nay khi da co new_assigned_route that su la list edge_id.
    """
    update_doc = {
        "$set": {
            "assigned_route": new_assigned_route,
            "optimized_customer_order": ga_result.get("optimized_customer_order", []),
            "optimization_result": {
                "old_assigned_route": old_assigned_route,
                "new_assigned_route": new_assigned_route,
                "optimized_customer_order": ga_result.get("optimized_customer_order", []),
                "estimated_total_cost": ga_result.get("estimated_total_cost"),
                "generation_count": ga_result.get("generation_count"),
                "optimized_at": now_ms(),
                "algorithm": "Genetic Algorithm",
            },
            "last_optimized_at": now_ms(),
            "route_status": "optimized",
        }
    }

    result = mongo_collection.update_one(
        {"vehicle_id": vehicle_id},
        update_doc,
    )

    return result.modified_count > 0 or result.matched_count > 0


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
    """
    Toi uu route cho mot xe.

    Return status:
    - skipped: khong can toi uu
    - optimized: da chay GA
    - error: loi validate/chay thuat toan/ghi DB
    """
    vehicle_id = str(vehicle_doc.get("vehicle_id", ""))

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

        mongo_updated = False
        if mongo_collection is not None:
            mongo_updated = save_optimization_result_to_mongo(
                mongo_collection=mongo_collection,
                vehicle_id=vehicle_id,
                ga_result=ga_result,
                old_assigned_route=old_assigned_route,
            )

        return build_success_response(
            vehicle_id=vehicle_id,
            old_assigned_route=old_assigned_route,
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
        result = optimize_vehicle(
            vehicle_doc=vehicle_doc,
            redis_client=redis_client,
            mongo_collection=mongo_collection,
            min_avg_speed=min_avg_speed,
            population_size=population_size,
            generations=generations,
            mutation_rate=mutation_rate,
            force=force,
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
    def __init__(self, matched_count: int = 1, modified_count: int = 1):
        self.matched_count = matched_count
        self.modified_count = modified_count


class FakeMongoCollection:
    """
    Fake MongoDB collection de test local khong can chay MongoDB.
    """
    def __init__(self):
        self.docs = {}

    def insert_one_doc(self, doc: Dict[str, Any]) -> None:
        self.docs[doc["vehicle_id"]] = doc

    def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        vehicle_id = query.get("vehicle_id")
        return self.docs.get(vehicle_id)

    def update_one(self, query: Dict[str, Any], update_doc: Dict[str, Any]) -> FakeMongoUpdateResult:
        vehicle_id = query.get("vehicle_id")

        if vehicle_id not in self.docs:
            return FakeMongoUpdateResult(matched_count=0, modified_count=0)

        set_doc = update_doc.get("$set", {})
        self.docs[vehicle_id].update(set_doc)

        return FakeMongoUpdateResult(matched_count=1, modified_count=1)


if __name__ == "__main__":
    from optimizer_input_adapter import FakeRedis

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
            {
                "cust_id": "Cust_003",
                "latitude": 21.032345,
                "longitude": 105.802345,
            },
        ],
    }

    redis_client = FakeRedis()
    mongo_collection = FakeMongoCollection()
    mongo_collection.insert_one_doc(vehicle_doc)

    result = optimize_vehicle(
        vehicle_doc=vehicle_doc,
        redis_client=redis_client,
        mongo_collection=mongo_collection,
        force=False,
        random_seed=42,
    )

    print("Optimization result:")
    print(result)

    print("\nMongoDB document after update:")
    print(mongo_collection.find_one({"vehicle_id": "Truck_001"}))
