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
nombre_archivo = 'problema3.txt'

# Llamada a la función y almacenamiento en el arreglo
datos = leer_datos_archivo(nombre_archivo)

# Muestra los datos
print("Datos leídos del archivo:")
print(datos)


