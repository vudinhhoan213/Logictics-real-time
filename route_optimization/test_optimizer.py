
from optimizer import RouteOptimizer
from graph_network import GraphNetwork


def main():
    graph = GraphNetwork()

    optimizer = RouteOptimizer(graph)

    vehicle_state = {
        "vehicle_id": "Truck_001",
        "current_edge_id": "E_1_2"
    }

    customers = [
        {"cust_id": "C1", "latitude": 21.01, "longitude": 105.81},
        {"cust_id": "C2", "latitude": 21.02, "longitude": 105.82}
    ]

    blocked_edges = []

    result = optimizer.optimize_route(vehicle_state, customers, blocked_edges)

    print(result)


if __name__ == "__main__":
    main()
