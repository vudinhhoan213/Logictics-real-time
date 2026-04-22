def build_route(graph, start_edge, customer_order, blocked_edges=None):
    blocked_edges = blocked_edges or []

    if start_edge not in graph.edges:
        return []

    route_edges = []
    current_node = graph.edge_end_node(start_edge)

    for customer in customer_order:
        target_node = graph.nearest_node_to_point(
            customer["latitude"],
            customer["longitude"]
        )

        if current_node is None or target_node is None:
            continue

        partial_route, _ = graph.shortest_path(
            current_node,
            target_node,
            blocked_edges=blocked_edges
        )

        if not partial_route:
            continue

        route_edges.extend(partial_route)
        current_node = target_node

    return route_edges