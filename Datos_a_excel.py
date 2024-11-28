import pandas as pd
import re

# Función para procesar el archivo txt
def procesar_txt_a_excel(archivo_txt, archivo_excel):
    datos = []
    
    # Expresiones regulares para extraer los datos
    patron = r"Mejor costo: ([\d\.]+),.*?Tiempo actual: ([\d\.]+)"
    
    with open(archivo_txt, 'r') as file:
        for linea in file:
            match = re.search(patron, linea)
            if match:
                mejor_costo = float(match.group(1))
                tiempo_actual = float(match.group(2))
                datos.append({"Mejor costo": mejor_costo, "Tiempo actual (s)": tiempo_actual})
    
    # Crear un DataFrame y exportar a Excel
    df = pd.DataFrame(datos)
    df.to_excel(archivo_excel, index=False, engine='openpyxl')
    print(f"Datos exportados exitosamente a {archivo_excel}")

# Uso de la función
archivo_txt = "resultadosP3_Spark.txt"  # Reemplaza con la ruta de tu archivo txt
archivo_excel = "salida.xlsx"         # Nombre del archivo Excel a generar
procesar_txt_a_excel(archivo_txt, archivo_excel)
