# Practica-2
Las pruebas se realizaron con los siguientes datasets:
El problema # 1 Corresponde a https://people.brunel.ac.uk/~mastjjb/jeb/orlib/files/vrp10.txt
El problema # 2 Corresponde a https://people.brunel.ac.uk/~mastjjb/jeb/orlib/files/vrp9.txt
El problema # 3 Corresponde a https://people.brunel.ac.uk/~mastjjb/jeb/orlib/files/vrp8.txt
Los problemas nodos son (Numero del nodo, Eje X, Eje Y, Eje Z) Para calcular las distancias se hizo por distancias euclidianas

Para ejecutar y ver los resultados con spark hay q ver los datos que genera en el contenedor. 
docker build -t tsp-spark .
docker run tsp-spark
