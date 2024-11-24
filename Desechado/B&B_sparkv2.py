import numpy as np
import time
from pyspark import SparkConf, SparkContext

# Función para calcular la distancia euclidiana entre dos ciudades
def distancia(ciudad1, ciudad2):
    return np.sqrt((ciudad1[1] - ciudad2[1])**2 + (ciudad1[2] - ciudad2[2])**2 + (ciudad1[3] - ciudad2[3])**2)

# Función para encontrar la ruta mínima usando Branch & Bound
def tsp_branch_and_bound(ciudades):
    conf = SparkConf().setAppName("TSP Branch and Bound").setMaster("local[*]")
    sc = SparkContext(conf=conf)

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
    
    # Inicializar variables
    mejor_ruta = None
    mejor_costo = np.inf
    ruta_actual = [0]
    costo_actual = 0
    
    # Archivo para guardar resultados
    with open("resultados.txt", "w") as archivo_resultados:
        archivo_resultados.write("Inicio del algoritmo Branch & Bound\n")

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
    
    # Iniciar el algoritmo
    branch_and_bound(ruta_actual, costo_actual)
    sc.stop()
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
nombre_archivo = 'problema1.txt'

# Llamada a la función y almacenamiento en el arreglo
ciudades = leer_datos_archivo(nombre_archivo)

mejor_ruta, mejor_costo = tsp_branch_and_bound(ciudades)
print("Mejor ruta:", mejor_ruta)
print("Mejor costo:", mejor_costo)

# Guardar el resultado final
with open("resultados.txt", "a") as archivo_resultados:
    archivo_resultados.write(
        f"Resultado final - Mejor costo: {mejor_costo}, Mejor ruta: {mejor_ruta}\n"
    )
