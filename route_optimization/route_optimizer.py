"""
route_optimizer.py

Integration layer cho module Route Optimization.

Backend dashboard hien dang nghe MongoDB collection `assigned_routes`
va emit event `route_optimized` voi format:
    {
        path: updatedData.new_assigned_route,
        time: updatedData.estimated_total_travel_time
    }

Vi vay file nay ghi cac field:
- vehicle_id
- new_assigned_route
- estimated_total_travel_time

Ghi chu:
- GA hien tai moi toi uu thu tu khach hang (`optimized_customer_order`).
- GA chua sinh edge route moi that su tu graph.
- Tam thoi `new_assigned_route` dung `old_assigned_route` de dashboard co path edge_id hop le.
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


def build_new_assigned_route(
    old_assigned_route: list,
    ga_result: Dict[str, Any],
    opt_input: Dict[str, Any],
) -> list:
    """
    Tao route edge_id moi cho dashboard.

    Hien tai:
    - GA chi tra ve optimized_customer_order.
    - Chua co module convert customer order -> shortest path edge list.
    - Do do tam thoi tra ve old_assigned_route de dashboard nhan duoc
      new_assigned_route hop le va ve duoc Polyline.

    Sau nay nang cap:
    - Load graph edges_schema.json.
    - Map customer toa do -> nearest edge/node.
    - Dung NetworkX shortest_path voi cost tu Redis.
    - Tra ve list edge_id moi.
    """
    if old_assigned_route:
        return old_assigned_route

    return []


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
        assigned_routes

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
                "generation_count": ga_result.get("generation_count"),
                "optimized_at": now_ms(),
                "algorithm": "Genetic Algorithm",
                "note": "new_assigned_route currently falls back to old_assigned_route until graph pathfinding is implemented",
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

        new_assigned_route = build_new_assigned_route(
            old_assigned_route=old_assigned_route,
            ga_result=ga_result,
            opt_input=opt_input,
        )

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
