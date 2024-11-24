from pyspark import SparkContext, SparkConf
import numpy as np
import time

# Función para calcular la distancia euclidiana entre dos ciudades
def distancia(ciudad1, ciudad2):
    return np.sqrt((ciudad1[1] - ciudad2[1])**2 + (ciudad1[2] - ciudad2[2])**2 + (ciudad1[3] - ciudad2[3])**2)

# Función para leer datos del archivo txt y guardarlos en un arreglo
def leer_datos_archivo(nombre_archivo):
    datos = []
    with open(nombre_archivo, 'r') as archivo:
        for linea in archivo:
            numeros = list(map(int, linea.split()))
            datos.append(numeros)
    return datos

# Función para resolver el TSP usando Spark
def tsp_branch_and_bound_spark(ciudades):
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

    # Crear SparkContext
    conf = SparkConf().setAppName("TSP_Branch_And_Bound").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    # Variables compartidas
    mejor_costo = sc.accumulator(float('inf'))
    mejor_ruta = sc.broadcast([])

    # Función para evaluar subproblemas en nodos esclavos
    def evaluar_ruta(subproblema):
        ruta_actual, costo_actual = subproblema
        if len(ruta_actual) == n:
            costo_actual += distancias[ruta_actual[-1]][ruta_actual[0]]
            if costo_actual < mejor_costo.value:
                mejor_costo.add(costo_actual - mejor_costo.value)
                mejor_ruta.value = ruta_actual
            return []
        
        resultados = []
        for i in range(n):
            if i not in ruta_actual:
                nuevo_costo = costo_actual + distancias[ruta_actual[-1]][i]
                if nuevo_costo < mejor_costo.value:
                    nueva_ruta = ruta_actual + [i]
                    resultados.append((nueva_ruta, nuevo_costo))
        return resultados

    # Iniciar la exploración
    tareas_iniciales = [([0], 0)]  # Ruta inicial con nodo 0
    rdd = sc.parallelize(tareas_iniciales)

    while not rdd.isEmpty():
        rdd = rdd.flatMap(evaluar_ruta)

    # Finalizar SparkContext
    sc.stop()

    tiempo_final = time.time() - Tiempo_i
    return mejor_ruta.value, mejor_costo.value, tiempo_final

# Nombre del archivo que contiene los datos
nombre_archivo = 'problema3.txt'

# Llamada a la función y almacenamiento en el arreglo
ciudades = leer_datos_archivo(nombre_archivo)

mejor_ruta, mejor_costo, tiempo = tsp_branch_and_bound_spark(ciudades)
print("Mejor ruta:", mejor_ruta)
print("Mejor costo:", mejor_costo)
print("Tiempo total:", tiempo)

# Guardar resultados
with open("resultados.txt", "w") as archivo_resultados:
    archivo_resultados.write(
        f"Resultado final - Mejor costo: {mejor_costo}, Mejor ruta: {mejor_ruta}, Tiempo total: {tiempo:.2f} segundos\n"
    )
