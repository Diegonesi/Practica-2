import sys
import math

class Nodo:
    def __init__(self, nivel, costo, camino, bound):
        self.nivel = nivel
        self.costo = costo
        self.camino = camino
        self.bound = bound

def calcular_bound(nodo, n, distancias):
    bound = nodo.costo
    visitados = [False] * n
    for ciudad in nodo.camino:
        visitados[ciudad] = True

    for i in range(n):
        if not visitados[i]:
            min_costo = sys.maxsize
            for j in range(n):
                if not visitados[j] and distancias[i][j] < min_costo:
                    min_costo = distancias[i][j]
            bound += min_costo
    return bound

def branch_and_bound_tsp(n, distancias):
    cola = []
    nodo_inicial = Nodo(0, 0, [0], 0)
    nodo_inicial.bound = calcular_bound(nodo_inicial, n, distancias)
    cola.append(nodo_inicial)
    costo_minimo = sys.maxsize
    mejor_camino = []

    while cola:
        nodo_actual = cola.pop(0)
        if nodo_actual.bound < costo_minimo:
            for i in range(1, n):
                if i not in nodo_actual.camino:
                    nuevo_camino = nodo_actual.camino + [i]
                    nuevo_costo = nodo_actual.costo + distancias[nodo_actual.camino[-1]][i]
                    if len(nuevo_camino) == n:
                        nuevo_costo += distancias[i][0]
                        if nuevo_costo < costo_minimo:
                            print(costo_minimo)
                            print(mejor_camino)
                            costo_minimo = nuevo_costo
                            mejor_camino = nuevo_camino
                    else:
                        nuevo_nodo = Nodo(nodo_actual.nivel + 1, nuevo_costo, nuevo_camino, 0)
                        nuevo_nodo.bound = calcular_bound(nuevo_nodo, n, distancias)
                        if nuevo_nodo.bound < costo_minimo:
                            cola.append(nuevo_nodo)
    return costo_minimo, mejor_camino

def leer_datos_archivo(nombre_archivo):
    datos = []
    with open(nombre_archivo, 'r') as archivo:
        for linea in archivo:
            numeros = list(map(int, linea.split()))
            datos.append(numeros)
    return datos


nombre_archivo = 'Problema3.txt'

datos = leer_datos_archivo(nombre_archivo)

def calcular_matriz_distancias(datos):
    n = len(datos)
    distancias = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j:
                distancias[i][j] = math.sqrt((datos[i][1] - datos[j][1]) ** 2 + (datos[i][2] - datos[j][2]) ** 2)
    return distancias

datos = leer_datos_archivo(nombre_archivo)
matriz_distancias = calcular_matriz_distancias(datos)

n = len(matriz_distancias)
print("comienza B&B")

costo_minimo, mejor_camino = branch_and_bound_tsp(n, matriz_distancias)

print(f"El costo mínimo es {costo_minimo}")
print(f"El mejor camino es {mejor_camino}")