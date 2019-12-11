# coding: utf-8
# Ejercicio 02 : Peliculas Marvel

# cargamos fichero marvel_names.txt
file_marvel_names = "file:///home/gchacaltanab/datos/marvel_names.txt"
fmn = sc.textFile(file_marvel_names)
#Imprimimos la primera linea del archivo para conocer su contenido.
fmn.first()

# cargamos fichero marvel_graph.txt
file_marvel_graph = "file:///home/gchacaltanab/datos/marvel_graph.txt"
fmg = sc.textFile(file_marvel_graph)
#Imprimimos la primera linea del archivo para conocer su contenido.
fmg.first()


# **Creamos RDD con heroe de marvel (como key) con la cantidad de heroes que participa en pelicula.**

graph_list = fmg.map(lambda x: x.split(' '))
d1 = graph_list.map(lambda x: (x[0], len(x)-1))
d1.take(10)

#Agrupamos los valores según el número de heroe.
data = d1.reduceByKey(lambda x,y:x+y)
data.take(8)


# Mostramos los 10 ID de Super Heroes que tiene mayor cantidad de relación con otros superheroes
data.top(10,lambda x: x[1])

print("El super heroe con ID %s es quién tiene más relación con otros superheroes" % (data.top(10,lambda x: x[1])[0][0]))