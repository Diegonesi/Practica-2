from pyspark.sql import SparkSession
import numpy as np
import time

# Función para calcular la distancia euclidiana entre dos ciudades
def distancia(ciudad1, ciudad2):
    return np.sqrt((ciudad1[1] - ciudad2[1])**2 + (ciudad1[2] - ciudad2[2])**2 + (ciudad1[3] - ciudad2[3])**2)

# Función para explorar rutas en cada rama
def explorar_rama(ciudad_inicial, distancias, n):
    mejor_ruta_local = None
    mejor_costo_local = float('inf')

    def branch_and_bound(ruta_actual, costo_actual):
        nonlocal mejor_ruta_local, mejor_costo_local
        if len(ruta_actual) == n:
            costo_actual += distancias[ruta_actual[-1]][ruta_actual[0]]
            if costo_actual < mejor_costo_local:
                mejor_costo_local = costo_actual
                mejor_ruta_local = ruta_actual[:]
            return

        for i in range(n):
            if i not in ruta_actual:
                nuevo_costo = costo_actual + distancias[ruta_actual[-1]][i]
                if nuevo_costo < mejor_costo_local:
                    ruta_actual.append(i)
                    branch_and_bound(ruta_actual, nuevo_costo)
                    ruta_actual.pop()

    # Comenzar la exploración desde la ciudad inicial
    branch_and_bound([ciudad_inicial], 0)
    return mejor_ruta_local, mejor_costo_local

# Función principal para resolver el problema con Spark
def tsp_branch_and_bound_spark(ciudades):
    Tiempo_i = time.time()
    n = len(ciudades)

    # Crear la matriz de distancias
    distancias = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            if i != j:
                distancias[i][j] = distancia(ciudades[i], ciudades[j])
            else:
                distancias[i][j] = np.inf

    # Inicializar SparkSession
    spark = SparkSession.builder.appName("TSP_BranchAndBound").getOrCreate()
    
    # Crear un RDD con las ciudades iniciales
    ciudades_rdd = spark.sparkContext.parallelize(range(n))

    # Explorar cada rama en paralelo
    resultados = ciudades_rdd.map(lambda ciudad_inicial: explorar_rama(ciudad_inicial, distancias, n)).collect()

    # Encontrar la mejor ruta y costo entre todas las ramas
    mejor_ruta_global = None
    mejor_costo_global = float('inf')

    for ruta, costo in resultados:
        if costo < mejor_costo_global:
            mejor_costo_global = costo
            mejor_ruta_global = ruta

    # Tiempo final
    tiempo_total = time.time() - Tiempo_i
    print(f"Tiempo total: {tiempo_total:.2f} segundos")
    return mejor_ruta_global, mejor_costo_global

# Función para leer datos del archivo txt y guardarlos en un arreglo
def leer_datos_archivo(nombre_archivo):
    datos = []
    with open(nombre_archivo, 'r') as archivo:
        for linea in archivo:
            numeros = list(map(int, linea.split()))
            datos.append(numeros)
    return datos

# Nombre del archivo que contiene los datos
nombre_archivo = 'problema1.txt'

# Leer las ciudades desde el archivo
ciudades = leer_datos_archivo(nombre_archivo)

# Llamar a la función principal
mejor_ruta, mejor_costo = tsp_branch_and_bound_spark(ciudades)

# Imprimir resultados
print("Mejor ruta:", mejor_ruta)
print("Mejor costo:", mejor_costo)

# Guardar el resultado final en un archivo
with open("resultados.txt", "a") as archivo_resultados:
    archivo_resultados.write(
        f"Resultado final - Mejor costo: {mejor_costo}, Mejor ruta: {mejor_ruta}\n"
    )
