import sys

class TSPSolver:
    def __init__(self, graph):
        self.graph = graph
        self.n = len(graph)
        self.visited = [False] * self.n
        self.min_cost = sys.maxsize
        self.path = []

    def tsp(self, curr_pos, count, cost, path):
        if count == self.n and self.graph[curr_pos][0]:
            if cost + self.graph[curr_pos][0] < self.min_cost:
                self.min_cost = cost + self.graph[curr_pos][0]
                self.path = path + [0]
            return

        for i in range(self.n):
            if not self.visited[i] and self.graph[curr_pos][i]:
                self.visited[i] = True
                self.tsp(i, count + 1, cost + self.graph[curr_pos][i], path + [i])
                self.visited[i] = False

    def solve(self):
        self.visited[0] = True
        self.tsp(0, 1, 0, [0])
        return self.min_cost, self.path

# Ejemplo de uso
graph = [
    [0, 10, 15, 20],
    [10, 0, 35, 25],
    [15, 35, 0, 30],
    [20, 25, 30, 0]
]

solver = TSPSolver(graph)
min_cost, path = solver.solve()
print(f"Costo mínimo: {min_cost}")
print(f"Camino: {path}")
