
class GeneticAlgorithm:
    def __init__(self, graph, customers, blocked_edges, start_edge):
        self.graph = graph
        self.customers = customers
        self.blocked_edges = blocked_edges
        self.start_edge = start_edge

    def initialize_population(self):
        pass

    def fitness(self, chromosome):
        pass

    def selection(self):
        pass

    def crossover(self, parent1, parent2):
        pass

    def mutation(self, chromosome):
        pass

    def run(self):
        # TODO: Implement GA loop
        # Temporary return customer order as-is
        return [c["cust_id"] for c in self.customers]
