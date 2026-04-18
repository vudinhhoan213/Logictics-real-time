from optimizer import RouteOptimizer
from graph_network import GraphNetwork
from traffic_adapter import TrafficAdapter


def run_case(title, graph, vehicle_state, customers, blocked_edges):
    optimizer = RouteOptimizer(graph)
    result = optimizer.optimize_route(vehicle_state, customers, blocked_edges)

    print(f"\n=== {title} ===")
    print("vehicle_id:", result["vehicle_id"])
    print("optimized_customer_order:", [c["cust_id"] for c in result["optimized_customer_order"]])
    print("new_assigned_route length:", len(result["new_assigned_route"]))
    print("new_assigned_route preview:", result["new_assigned_route"][:10])

    return result


def main():
    graph = GraphNetwork()
    graph.load_from_schema("../data/edges_schema.json")

    vehicle_state = {
        "vehicle_id": "Truck_001",
        "edge_id": "E_1497964601_8130248907"
    }

    customers = [
        {"cust_id": "C1", "latitude": 21.0103947, "longitude": 105.8380761},
        {"cust_id": "C2", "latitude": 21.0099930, "longitude": 105.8388212}
    ]

    run_case(
        title="STATIC BASELINE",
        graph=graph,
        vehicle_state=vehicle_state,
        customers=customers,
        blocked_edges=[]
    )

    traffic = None
    try:
        traffic = TrafficAdapter()
        graph.set_traffic_adapter(traffic)
        blocked_edges = traffic.get_blocked_edges()

        run_case(
            title="REDIS TRAFFIC",
            graph=graph,
            vehicle_state=vehicle_state,
            customers=customers,
            blocked_edges=blocked_edges
        )
    except Exception as e:
        print("\n=== REDIS TRAFFIC ===")
        print("Redis unavailable:", e)
    finally:
        if traffic is not None:
            traffic.close()


if __name__ == "__main__":
    main()