from pyspark import SparkContext
from queue import PriorityQueue
import time

# Función para leer datos del archivo txt y guardarlos en un arreglo
def leer_datos_archivo(nombre_archivo):
    datos = []
    with open(nombre_archivo, 'r') as archivo:
        for linea in archivo:
            numeros = list(map(int, linea.split())) 
            datos.append(numeros)
    return datos


nombre_archivo = 'problema_simple_10.txt'
distances = leer_datos_archivo(nombre_archivo)

# Limite simple
def calculate_bound(route, visited, distances):
    # Implementa el cálculo del límite inferSior.
    n = len(distances)
    bound = 0
    # Coste de la ruta parcial actual
    for i in range(len(route) - 1):
        bound += distances[route[i]][route[i + 1]]
    # Coste mínimo de salida y entrada para cada nodo no visitado
    for i in range(n):
        if i not in visited:
            min_out = min([distances[i][j] for j in range(n) if j != i and j not in visited], default=0)
            min_in = min([distances[j][i] for j in range(n) if j != i and j not in visited], default=0)
            bound += (min_out + min_in) / 2
    return bound

# Función para generar subproblemas
def branch(subproblem, distances):
    route, visited_nodes, bound = subproblem
    next_nodes = [n for n in range(len(distances)) if n not in visited_nodes]
    subproblems = []
    for node in next_nodes:
        new_route = route + [node]
        new_visited = visited_nodes | {node}
        new_bound = calculate_bound(new_route, new_visited, distances)
        subproblems.append((new_route, new_visited, new_bound))
    return subproblems

# Función para filtrar y mantener los mejores subproblemas
def filter_subproblems(subproblems, global_bound):
    filtered = [s for s in subproblems if s[2] < global_bound]
    return filtered

# Configuración de Spark
sc = SparkContext("local", "TSP_Branch_and_Bound")

# Datos iniciales
initial_bound = float('inf')
distances = [...]  # Matriz de distancias.
initial_state = ([0], {0}, 0)  # Ruta parcial, nodos visitados, límite inferior.

Tiempo_i = time.time()
# Crear RDD inicial
rdd = sc.parallelize([initial_state])

# Algoritmo Branch and Bound
global_bound = initial_bound
queue = PriorityQueue()

while not rdd.isEmpty():
    # Generar nuevos subproblemas
    subproblems = rdd.flatMap(lambda x: branch(x, distances)).collect()
    # Filtrar subproblemas con límite mejorado
    filtered_subproblems = filter_subproblems(subproblems, global_bound)
    # Actualizar límite global
    for _, _, bound in filtered_subproblems:
        if bound < global_bound:
            global_bound = bound
            Tiempo_f = time.time()
            print("Mejor solucion: ",global_bound,"Tiempo de ejecucion: " ,Tiempo_f-Tiempo_i) # Actualizacion del mejor resultado
    # Actualizar RDD con los subproblemas seleccionados
    rdd = sc.parallelize(filtered_subproblems)

print(f"Mejor solución encontrada: {global_bound}")
