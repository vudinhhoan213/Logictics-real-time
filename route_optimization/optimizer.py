from graph_network import GraphNetwork
from route_builder import build_route


class RouteOptimizer:
    def __init__(self, graph: GraphNetwork):
        self.graph = graph

    def _get_start_edge(self, vehicle_state):
        return vehicle_state.get("current_edge_id") or vehicle_state.get("edge_id")

    def _greedy_customer_order(self, start_edge, customers, blocked_edges):
        if not customers:
            return []

        current_node = self.graph.edge_end_node(start_edge)
        if current_node is None:
            return customers[:]

        unvisited = customers[:]
        ordered = []

        while unvisited:
            best_customer = None
            best_node = None
            best_cost = float("inf")

            for c in unvisited:
                target_node = self.graph.nearest_node_to_point(
                    c["latitude"],
                    c["longitude"]
                )

                if target_node is None:
                    continue

                _, cost = self.graph.shortest_path(
                    current_node,
                    target_node,
                    blocked_edges=blocked_edges
                )

                if cost < best_cost:
                    best_cost = cost
                    best_customer = c
                    best_node = target_node

            if best_customer is None:
                ordered.extend(unvisited)
                break

            ordered.append(best_customer)
            unvisited.remove(best_customer)
            current_node = best_node

        return ordered

    def optimize_route(self, vehicle_state, remaining_customers, blocked_edges):
        start_edge = self._get_start_edge(vehicle_state)
        if not start_edge:
            return {
                "vehicle_id": vehicle_state.get("vehicle_id"),
                "optimized_customer_order": remaining_customers,
                "new_assigned_route": []
            }

        best_order = self._greedy_customer_order(
            start_edge,
            remaining_customers,
            blocked_edges
        )

        new_route = build_route(
            self.graph,
            start_edge,
            best_order,
            blocked_edges=blocked_edges
        )

        return {
            "vehicle_id": vehicle_state.get("vehicle_id"),
            "optimized_customer_order": best_order,
            "new_assigned_route": new_route
        }