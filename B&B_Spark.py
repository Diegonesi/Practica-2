import numpy as np
import time
from pyspark.sql import SparkSession

# Función para calcular la distancia euclidiana entre dos ciudades
def distancia(ciudad1, ciudad2):
    return np.sqrt((ciudad1[1] - ciudad2[1])**2 + (ciudad1[2] - ciudad2[2])**2 + (ciudad1[3] - ciudad2[3])**2)

# Función para encontrar la ruta mínima usando Branch & Bound con Spark
def tsp_branch_and_bound_spark(ciudades, num_nodos):
    Tiempo_i = time.time()
    n = len(ciudades)
    distancias = np.zeros((n, n))
    
    # Crear la matriz de distancias
    for i in range(n):
        for j in range(n):
            if i != j:
                distancias[i][j] = distancia(ciudades[i], ciudades[j])
            else:
                distancias[i][j] = np.inf
    
    # Inicializar Spark
    spark = SparkSession.builder \
        .appName("TSP Branch and Bound") \
        .master(f"local[{num_nodos}]") \
        .getOrCreate()
    
    mejor_ruta = None
    mejor_costo = np.inf
    
    # Función recursiva para explorar rutas
    def branch_and_bound(ruta_actual, costo_actual):
        nonlocal mejor_ruta, mejor_costo
        
        if len(ruta_actual) == n:
            costo_actual += distancias[ruta_actual[-1]][ruta_actual[0]]
            if costo_actual < mejor_costo:
                mejor_costo = costo_actual
                mejor_ruta = ruta_actual[:]
                tiempo_actual = time.time() - Tiempo_i
                
                # Guardar los resultados en el archivo
                with open("resultados.txt", "a") as archivo_resultados:
                    archivo_resultados.write(
                        f"Mejor costo: {mejor_costo}, Mejor ruta actual: {mejor_ruta}, Tiempo actual: {tiempo_actual:.2f} segundos\n"
                    )
            return
        
        for i in range(n):
            if i not in ruta_actual:
                nuevo_costo = costo_actual + distancias[ruta_actual[-1]][i]
                if nuevo_costo < mejor_costo:
                    ruta_actual.append(i)
                    branch_and_bound(ruta_actual, nuevo_costo)
                    ruta_actual.pop()
    
    # Crear un RDD con las rutas iniciales
    rutas_iniciales = spark.sparkContext.parallelize([[0]], numSlices=num_nodos)
    
    # Ejecutar el algoritmo en paralelo
    rutas_iniciales.foreach(lambda ruta: branch_and_bound(ruta, 0))
    
    spark.stop()
    
    return mejor_ruta, mejor_costo

# Función para leer datos del archivo txt y guardarlos en un arreglo
def leer_datos_archivo(nombre_archivo):
    datos = []
    with open(nombre_archivo, 'r') as archivo:
        for linea in archivo:
            # Separa cada línea en números y convierte cada uno a entero
            numeros = list(map(int, linea.split()))
            # Añade la lista de números al arreglo
            datos.append(numeros)
    return datos

# Nombre del archivo que contiene los datos
#nombre_archivo = 'problema3.txt'

# Llamada a la función y almacenamiento en el arreglo
#ciudades = leer_datos_archivo(nombre_archivo)

ciudades3 = [[1, 37, 52, 7], [2, 49, 49, 30], [3, 52, 64, 16], [4, 20, 26, 9], [5, 40, 30, 21], [6, 21, 47, 15], [7, 17, 63, 19], [8, 31, 62, 23], [9, 52, 33, 11], [10, 51, 21, 5], [11, 42, 41, 19], [12, 31, 32, 29], [13, 5, 25, 23], [14, 12, 42, 21], [15, 36, 16, 10], [16, 52, 41, 15], [17, 27, 23, 3], [18, 17, 33, 41], [19, 13, 13, 9], [20, 57, 58, 28], [21, 62, 42, 8], [22, 42, 57, 8], [23, 16, 57, 16], [24, 8, 52, 10], [25, 7, 38, 28], [26, 27, 68, 7], [27, 30, 48, 15], [28, 43, 67, 14], [29, 58, 48, 6], [30, 58, 27, 19], [31, 37, 69, 11], [32, 38, 46, 12], [33, 46, 10, 23], [34, 61, 33, 26], [35, 62, 63, 17], [36, 63, 69, 6], [37, 32, 22, 9], [38, 45, 35, 15], [39, 59, 15, 14], [40, 5, 6, 7], [41, 10, 17, 27], [42, 21, 10, 13], [43, 5, 64, 11], [44, 30, 15, 16], [45, 39, 10, 10], [46, 32, 39, 5], [47, 25, 32, 25], [48, 25, 55, 17], [49, 48, 28, 18], [50, 56, 37, 10]]
ciudades2 = [[1, 22, 22, 18], [2, 36, 26, 26], [3, 21, 45, 11], [4, 45, 35, 30], [5, 55, 20, 21], [6, 33, 34, 19], [7, 50, 50, 15], [8, 55, 45, 16], [9, 26, 59, 29], [10, 40, 66, 26], [11, 55, 65, 37], [12, 35, 51, 16], [13, 62, 35, 12], [14, 62, 57, 31], [15, 62, 24, 8], [16, 21, 36, 19], [17, 33, 44, 20], [18, 9, 56, 13], [19, 62, 48, 15], [20, 66, 14, 22], [21, 44, 13, 28], [22, 26, 13, 12], [23, 11, 28, 6], [24, 7, 43, 27], [25, 17, 64, 14], [26, 41, 46, 18], [27, 55, 34, 17], [28, 35, 16, 29], [29, 52, 26, 13], [30, 43, 26, 22], [31, 31, 76, 25], [32, 22, 53, 28], [33, 26, 29, 27], [34, 50, 40, 19], [35, 55, 50, 10], [36, 54, 10, 12], [37, 60, 15, 14], [38, 47, 66, 24], [39, 30, 60, 16], [40, 30, 50, 33], [41, 12, 17, 15], [42, 15, 14, 11], [43, 16, 19, 18], [44, 21, 48, 17], [45, 50, 30, 21], [46, 51, 42, 27], [47, 50, 15, 19], [48, 48, 21, 20], [49, 12, 38, 5], [50, 15, 56, 22], [51, 29, 39, 12], [52, 54, 38, 19], [53, 55, 57, 22], [54, 67, 41, 16], [55, 10, 70, 7], [56, 6, 25, 26], [57, 65, 27, 14], [58, 40, 60, 21], [59, 70, 64, 24], [60, 64, 4, 13], [61, 36, 6, 15], [62, 30, 20, 18], [63, 20, 30, 11], [64, 15, 5, 28], [65, 50, 70, 9], [66, 57, 72, 37], [67, 45, 42, 30], [68, 38, 33, 10], [69, 50, 4, 8], [70, 66, 8, 11], [71, 59, 5, 3], [72, 35, 60, 1], [73, 27, 24, 6], [74, 40, 20, 10], [75, 40, 37, 20]]
ciudades1 = [[1, 41, 49, 10], [2, 35, 17, 7], [3, 55, 45, 13], [4, 55, 20, 19], [5, 15, 30, 26], [6, 25, 30, 3], [7, 20, 50, 5], [8, 10, 43, 9], [9, 55, 60, 16], [10, 30, 60, 16], [11, 20, 65, 12], [12, 50, 35, 19], [13, 30, 25, 23], [14, 15, 10, 20], [15, 30, 5, 8], [16, 10, 20, 19], [17, 5, 30, 2], [18, 20, 40, 12], [19, 15, 60, 17], [20, 45, 65, 9], [21, 45, 20, 11], [22, 45, 10, 18], [23, 55, 5, 29], [24, 65, 35, 3], [25, 65, 20, 6], [26, 45, 30, 17], [27, 35, 40, 16], [28, 41, 37, 16], [29, 64, 42, 9], [30, 40, 60, 21], [31, 31, 52, 27], [32, 35, 69, 23], [33, 53, 52, 11], [34, 65, 55, 14], [35, 63, 65, 8], [36, 2, 60, 5], [37, 20, 20, 8], [38, 5, 5, 16], [39, 60, 12, 31], [40, 40, 25, 9], [41, 42, 7, 5], [42, 24, 12, 5], [43, 23, 3, 7], [44, 11, 14, 18], [45, 6, 38, 16], [46, 2, 48, 1], [47, 8, 56, 27], [48, 13, 52, 36], [49, 6, 68, 30], [50, 47, 47, 13], [51, 49, 58, 10], [52, 27, 43, 9], [53, 37, 31, 14], [54, 57, 29, 18], [55, 63, 23, 2], [56, 53, 12, 6], [57, 32, 12, 7], [58, 36, 26, 18], [59, 21, 24, 28], [60, 17, 34, 3], [61, 12, 24, 13], [62, 24, 58, 19], [63, 27, 69, 10], [64, 15, 77, 9], [65, 62, 77, 20], [66, 49, 73, 25], [67, 67, 5, 25], [68, 56, 39, 36], [69, 37, 47, 6], [70, 37, 56, 5], [71, 57, 68, 15], [72, 47, 16, 25], [73, 44, 17, 9], [74, 46, 13, 8], [75, 49, 11, 18], [76, 49, 42, 13], [77, 53, 43, 14], [78, 61, 52, 3], [79, 57, 48, 23], [80, 56, 37, 6], [81, 55, 54, 26], [82, 15, 47, 16], [83, 14, 37, 11], [84, 11, 31, 7], [85, 16, 22, 41], [86, 4, 18, 35], [87, 28, 18, 26], [88, 26, 52, 9], [89, 26, 35, 15], [90, 31, 67, 3], [91, 15, 19, 1], [92, 22, 22, 2], [93, 18, 24, 22], [94, 26, 27, 27], [95, 25, 24, 20], [96, 22, 27, 11], [97, 25, 21, 12], [98, 19, 21, 10], [99, 20, 26, 9], [100, 18, 18, 17]]
# Número de nodos de Spark
num_nodos = 4

mejor_ruta, mejor_costo = tsp_branch_and_bound_spark(ciudades1, num_nodos)
print("Mejor ruta:", mejor_ruta)
print("Mejor costo:", mejor_costo)

# Guardar el resultado final
with open("resultados.txt", "a") as archivo_resultados:
    archivo_resultados.write(
        f"Resultado final - Mejor costo: {mejor_costo}, Mejor ruta: {mejor_ruta}\n"
    )