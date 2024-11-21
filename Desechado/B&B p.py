import numpy as np
import time

# Matriz de distancias
distancias = np.array([
    [0, 12, 10, 19, 8, 25, 17, 18, 6, 21],
    [12, 0, 3, 5, 9, 15, 10, 12, 14, 11],
    [10, 3, 0, 8, 7, 14, 12, 13, 15, 16],
    [19, 5, 8, 0, 6, 20, 11, 15, 18, 13],
    [8, 9, 7, 6, 0, 10, 14, 9, 10, 12],
    [25, 15, 14, 20, 10, 0, 19, 17, 20, 22],
    [17, 10, 12, 11, 14, 19, 0, 4, 13, 15],
    [18, 12, 13, 15, 9, 17, 4, 0, 8, 10],
    [6, 14, 15, 18, 10, 20, 13, 8, 0, 16],
    [21, 11, 16, 13, 12, 22, 15, 10, 16, 0]
])

# Función para encontrar el costo mínimo
def tsp_branch_and_bound(distancias):
    Tiempo_i = time.time()
    n = len(distancias)
    visitado = [False] * n
    visitado[0] = True
    camino = [0]
    costo_minimo = float('inf')

    def branch_and_bound(ciudad_actual, costo_actual, nivel):
        nonlocal costo_minimo
        if nivel == n:
            costo_total = costo_actual + distancias[ciudad_actual][0]
            #print(costo_total)
            if costo_total < costo_minimo:
                costo_minimo = costo_total
                print("Costo Actualizado: ",costo_minimo, "En el tiempo", time.time()-Tiempo_i)
            return

        for i in range(n):
            if not visitado[i] and distancias[ciudad_actual][i] > 0:
                visitado[i] = True
                camino.append(i)
                branch_and_bound(i, costo_actual + distancias[ciudad_actual][i], nivel + 1)
                visitado[i] = False
                #print(camino)
                camino.pop()

    branch_and_bound(0, 0, 1)
    return costo_minimo

# Llamada a la función

costo_minimo = tsp_branch_and_bound(distancias)
print(f"El costo mínimo del recorrido es: {costo_minimo}")
