"""
genetic_algorithm.py

Genetic Algorithm cho module Route Optimization.

Y tuong chinh:
- Moi individual la mot thu tu giao khach.
  Vi du: [Cust_003, Cust_001, Cust_002]
- GA khong doc truc tiep MongoDB/Redis.
- GA chi nhan normalized_input tu optimizer_input_adapter.py.

Input ky vong:
    opt_input = {
        "vehicle_id": "...",
        "current_edge_id": "...",
        "remaining_customers": [
            {"cust_id": "Cust_001", "latitude": 21.0, "longitude": 105.8},
            ...
        ],
        "blocked_edges": ["E_106_TaySon"],
        "traffic_snapshot": {
            "E_105_NgTrai": {
                "avg_speed": 25.0,
                "estimated_travel_time": 30.5,
                ...
            }
        }
    }

Output:
    {
        "vehicle_id": "...",
        "optimized_customer_order": ["Cust_002", "Cust_001"],
        "estimated_total_cost": 123.45,
        "generation_count": 50
    }

Ghi chu:
- File nay chua phu thuoc NetworkX bat buoc, de de test.
- Neu co graph/pathfinding that, ban co the thay ham estimate_travel_cost().
"""

from typing import Any, Dict, List, Tuple
import random
import math


# =========================
# Config mac dinh
# =========================

DEFAULT_POPULATION_SIZE = 30
DEFAULT_GENERATIONS = 60
DEFAULT_MUTATION_RATE = 0.15
DEFAULT_ELITE_SIZE = 2

BLOCKED_EDGE_PENALTY = 1_000_000.0
CONGESTED_EDGE_PENALTY = 10_000.0
MISSING_CUSTOMER_PENALTY = 1_000_000.0


# =========================
# Helper functions
# =========================

def _customer_id(customer: Dict[str, Any]) -> str:
    return str(customer.get("cust_id", ""))


def _customer_key(customer: Dict[str, Any]) -> Tuple[str, float, float]:
    """
    Key dung de so sanh customer trong crossover/mutation.
    Tranh loi khi dict object khac instance nhung cung noi dung.
    """
    return (
        str(customer.get("cust_id", "")),
        float(customer.get("latitude", 0.0)),
        float(customer.get("longitude", 0.0)),
    )


def _distance_km(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    """
    Haversine distance don gian, tra ve km.
    Dung lam fallback khi chua co graph/pathfinding.
    """
    radius_km = 6371.0

    lat1 = math.radians(a_lat)
    lon1 = math.radians(a_lon)
    lat2 = math.radians(b_lat)
    lon2 = math.radians(b_lon)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    h = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    )

    return 2.0 * radius_km * math.asin(math.sqrt(h))


def _average_available_speed(opt_input: Dict[str, Any], default_speed: float = 30.0) -> float:
    """
    Lay toc do trung binh tu traffic_snapshot de uoc luong cost fallback.
    """
    snapshot = opt_input.get("traffic_snapshot", {})
    speeds: List[float] = []

    for traffic in snapshot.values():
        try:
            speed = float(traffic.get("avg_speed", 0.0))
            if speed > 0:
                speeds.append(speed)
        except (TypeError, ValueError):
            continue

    if not speeds:
        return default_speed

    return sum(speeds) / len(speeds)


def _route_has_blocked_or_congested_edges(opt_input: Dict[str, Any]) -> float:
    """
    Phat penalty neu assigned_route hien tai chua edge tac.
    Ham nay dung nhu mot thanh phan phu trong fitness.
    """
    assigned_route = set(opt_input.get("assigned_route", []))
    blocked_edges = set(opt_input.get("blocked_edges", []))
    snapshot = opt_input.get("traffic_snapshot", {})

    penalty = 0.0

    if assigned_route.intersection(blocked_edges):
        penalty += BLOCKED_EDGE_PENALTY

    for edge_id in assigned_route:
        traffic = snapshot.get(edge_id)
        if not traffic:
            continue

        is_congested = str(traffic.get("is_congested", "false")).lower() in (
            "1",
            "true",
            "yes",
        )

        try:
            avg_speed = float(traffic.get("avg_speed", 0.0))
        except (TypeError, ValueError):
            avg_speed = 0.0

        if is_congested:
            penalty += CONGESTED_EDGE_PENALTY

        if avg_speed > 0 and avg_speed <= 5.0:
            penalty += CONGESTED_EDGE_PENALTY

    return penalty


# =========================
# GA core
# =========================

def init_population(
    customers: List[Dict[str, Any]],
    population_size: int = DEFAULT_POPULATION_SIZE,
) -> List[List[Dict[str, Any]]]:
    """
    Tao population ban dau bang cach random thu tu customer.
    """
    if not customers:
        return [[]]

    population: List[List[Dict[str, Any]]] = []

    for _ in range(population_size):
        individual = customers[:]
        random.shuffle(individual)
        population.append(individual)

    return population


def estimate_travel_cost(
    from_customer: Dict[str, Any],
    to_customer: Dict[str, Any],
    opt_input: Dict[str, Any],
) -> float:
    """
    Uoc luong chi phi di chuyen giua 2 customer.

    Ban hien tai:
    - Dung khoang cach Haversine / toc do trung binh tu Redis.
    - Khi co graph that, thay ham nay bang shortest path + edge travel time.

    Return:
    - time estimate theo gio.
    """
    from_lat = float(from_customer.get("latitude", 0.0))
    from_lon = float(from_customer.get("longitude", 0.0))
    to_lat = float(to_customer.get("latitude", 0.0))
    to_lon = float(to_customer.get("longitude", 0.0))

    distance_km = _distance_km(from_lat, from_lon, to_lat, to_lon)
    avg_speed_kmh = _average_available_speed(opt_input)

    if avg_speed_kmh <= 0:
        avg_speed_kmh = 30.0

    return distance_km / avg_speed_kmh


def estimate_start_to_customer_cost(
    customer: Dict[str, Any],
    opt_input: Dict[str, Any],
) -> float:
    """
    Uoc luong cost tu vi tri xe hien tai den customer dau tien.

    Vi current_edge_id khong co toa do trong input normalize hien tai,
    ta dung cost nho neu chua co graph.

    Khi da co graph:
    - map customer -> nearest edge/node
    - shortest path tu current_edge_id -> customer edge
    """
    snapshot = opt_input.get("traffic_snapshot", {})
    current_edge_id = opt_input.get("current_edge_id", "")
    traffic = snapshot.get(current_edge_id)

    if traffic:
        try:
            estimated = float(traffic.get("estimated_travel_time", 0.0))
            if estimated > 0:
                return estimated / 3600.0  # neu Redis luu giay, doi sang gio
        except (TypeError, ValueError):
            pass

    return 0.0


def fitness(individual: List[Dict[str, Any]], opt_input: Dict[str, Any]) -> float:
    """
    Ham fitness can minimize.
    Gia tri cang nho cang tot.

    Thanh phan:
    - Tong thoi gian di chuyen uoc luong.
    - Penalty neu route co edge tac/nghen.
    - Penalty neu individual khong hop le.
    """
    required_customers = opt_input.get("remaining_customers", [])

    if len(individual) != len(required_customers):
        return MISSING_CUSTOMER_PENALTY

    individual_keys = [_customer_key(c) for c in individual]
    required_keys = [_customer_key(c) for c in required_customers]

    if sorted(individual_keys) != sorted(required_keys):
        return MISSING_CUSTOMER_PENALTY

    if not individual:
        return 0.0

    total_cost = 0.0

    # Cost tu xe hien tai den khach dau tien
    total_cost += estimate_start_to_customer_cost(individual[0], opt_input)

    # Cost giua cac khach
    for idx in range(len(individual) - 1):
        total_cost += estimate_travel_cost(individual[idx], individual[idx + 1], opt_input)

    # Penalty neu route hien tai bi tac
    total_cost += _route_has_blocked_or_congested_edges(opt_input)

    return total_cost


def evaluate_population(
    population: List[List[Dict[str, Any]]],
    opt_input: Dict[str, Any],
) -> List[Tuple[List[Dict[str, Any]], float]]:
    """
    Tinh fitness cho toan bo population.
    """
    scored: List[Tuple[List[Dict[str, Any]], float]] = []

    for individual in population:
        score = fitness(individual, opt_input)
        scored.append((individual, score))

    return scored


def select_parents(
    scored_population: List[Tuple[List[Dict[str, Any]], float]],
    elite_size: int = DEFAULT_ELITE_SIZE,
) -> List[List[Dict[str, Any]]]:
    """
    Selection:
    - Sap xep theo fitness tang dan.
    - Giu lai top 50%.
    - Dam bao elite nam trong nhom cha me.
    """
    if not scored_population:
        return []

    sorted_scored = sorted(scored_population, key=lambda item: item[1])
    keep_count = max(elite_size, len(sorted_scored) // 2)

    return [item[0] for item in sorted_scored[:keep_count]]


def order_crossover(
    parent1: List[Dict[str, Any]],
    parent2: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Order Crossover (OX).

    Vi du:
        p1 = [A, B, C, D]
        p2 = [C, D, A, B]
        lay doan [B, C] tu p1
        dien phan con lai theo thu tu p2 -> [D, B, C, A]

    Dam bao:
    - Khong trung khach.
    - Khong thieu khach.
    """
    size = len(parent1)

    if size <= 1:
        return parent1[:]

    start, end = sorted(random.sample(range(size), 2))

    child: List[Any] = [None] * size

    # Copy segment from parent1.
    child[start:end] = parent1[start:end]

    existing = {_customer_key(c) for c in child if c is not None}

    # Fill remaining positions by parent2 order.
    p2_index = 0
    for i in range(size):
        if child[i] is not None:
            continue

        while p2_index < size and _customer_key(parent2[p2_index]) in existing:
            p2_index += 1

        if p2_index < size:
            child[i] = parent2[p2_index]
            existing.add(_customer_key(parent2[p2_index]))

    # Safety fallback: fill any None if something unexpected happens.
    missing = [c for c in parent1 if _customer_key(c) not in existing]
    missing_index = 0
    for i in range(size):
        if child[i] is None:
            child[i] = missing[missing_index]
            missing_index += 1

    return child  # type: ignore


def mutate(
    individual: List[Dict[str, Any]],
    mutation_rate: float = DEFAULT_MUTATION_RATE,
) -> List[Dict[str, Any]]:
    """
    Mutation: doi cho 2 customer trong individual.
    """
    mutated = individual[:]

    if len(mutated) <= 1:
        return mutated

    if random.random() < mutation_rate:
        i, j = random.sample(range(len(mutated)), 2)
        mutated[i], mutated[j] = mutated[j], mutated[i]

    return mutated


def create_next_generation(
    scored_population: List[Tuple[List[Dict[str, Any]], float]],
    population_size: int,
    mutation_rate: float,
    elite_size: int,
) -> List[List[Dict[str, Any]]]:
    """
    Tao generation tiep theo:
    - Giu elite ca the tot nhat.
    - Phan con lai sinh bang crossover + mutation.
    """
    sorted_scored = sorted(scored_population, key=lambda item: item[1])
    elites = [item[0] for item in sorted_scored[:elite_size]]

    parents = select_parents(scored_population, elite_size=elite_size)

    next_population: List[List[Dict[str, Any]]] = elites[:]

    while len(next_population) < population_size:
        parent1 = random.choice(parents)
        parent2 = random.choice(parents)

        child = order_crossover(parent1, parent2)
        child = mutate(child, mutation_rate=mutation_rate)

        next_population.append(child)

    return next_population


def run_genetic_algorithm(
    opt_input: Dict[str, Any],
    population_size: int = DEFAULT_POPULATION_SIZE,
    generations: int = DEFAULT_GENERATIONS,
    mutation_rate: float = DEFAULT_MUTATION_RATE,
    elite_size: int = DEFAULT_ELITE_SIZE,
    random_seed: int | None = None,
) -> Dict[str, Any]:
    """
    Chay GA va tra ve ket qua tot nhat.
    """
    if random_seed is not None:
        random.seed(random_seed)

    customers = opt_input.get("remaining_customers", [])

    if not isinstance(customers, list):
        raise ValueError("opt_input['remaining_customers'] must be a list")

    if not customers:
        return {
            "vehicle_id": opt_input.get("vehicle_id"),
            "optimized_customer_order": [],
            "estimated_total_cost": 0.0,
            "generation_count": 0,
            "message": "No remaining customers",
        }

    population = init_population(customers, population_size=population_size)

    best_individual: List[Dict[str, Any]] = []
    best_score = float("inf")

    for _generation in range(generations):
        scored_population = evaluate_population(population, opt_input)

        current_best, current_score = min(scored_population, key=lambda item: item[1])

        if current_score < best_score:
            best_score = current_score
            best_individual = current_best

        population = create_next_generation(
            scored_population=scored_population,
            population_size=population_size,
            mutation_rate=mutation_rate,
            elite_size=elite_size,
        )

    return {
        "vehicle_id": opt_input.get("vehicle_id"),
        "optimized_customer_order": [_customer_id(c) for c in best_individual],
        "optimized_customers": best_individual,
        "estimated_total_cost": best_score,
        "generation_count": generations,
    }


# =========================
# Demo local
# =========================

if __name__ == "__main__":
    demo_input = {
        "vehicle_id": "Truck_001",
        "current_edge_id": "E_105_NgTrai",
        "assigned_route": ["E_105_NgTrai", "E_106_TaySon"],
        "blocked_edges": ["E_106_TaySon"],
        "traffic_snapshot": {
            "E_105_NgTrai": {
                "avg_speed": 25.0,
                "estimated_travel_time": 30.5,
                "vehicle_count": 20,
                "distance": 210.0,
                "max_speed": 50,
                "is_congested": False,
            },
            "E_106_TaySon": {
                "avg_speed": 3.5,
                "estimated_travel_time": 200.0,
                "vehicle_count": 120,
                "distance": 700.0,
                "max_speed": 50,
                "is_congested": True,
            },
        },
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

    result = run_genetic_algorithm(
        demo_input,
        population_size=20,
        generations=40,
        mutation_rate=0.2,
        random_seed=42,
    )

    print("GA result:")
    print(result)
