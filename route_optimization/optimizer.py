
from genetic_algorithm import GeneticAlgorithm
from graph_network import GraphNetwork
from route_builder import build_route


class RouteOptimizer:
    def __init__(self, graph: GraphNetwork):
        self.graph = graph

    def optimize_route(self, vehicle_state, remaining_customers, blocked_edges):
        ga = GeneticAlgorithm(
            graph=self.graph,
            customers=remaining_customers,
            blocked_edges=blocked_edges,
            start_edge=vehicle_state["current_edge_id"]
        )

        best_order = ga.run()
        new_route = build_route(self.graph, vehicle_state["current_edge_id"], best_order)

        return {
            "vehicle_id": vehicle_state["vehicle_id"],
            "optimized_customer_order": best_order,
            "new_assigned_route": new_route
        }
#####################################################################################
import time

if __name__ == "__main__":
    print("GA Optimizer Service is running...")
    while True:
        time.sleep(10) # Dừng 10 giây rồi lặp lại, giữ cho container sống
#####################################################################################
