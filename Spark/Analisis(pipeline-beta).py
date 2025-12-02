from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, avg, mean, stddev, expr, lit, collect_list, row_number, sum as spark_sum, approx_percentile, coalesce, countDistinct, concat_ws
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalyses").getOrCreate()

# Load data

artists = spark.read.parquet("hdfs://localhost:9000/music/processed/artists")
tracks = spark.read.parquet("hdfs://localhost:9000/music/processed/tracks")
albums = spark.read.parquet("hdfs://localhost:9000/music/processed/albums")
users = spark.read.parquet("hdfs://localhost:9000/music/processed/users")


#1 Top 20 artistas 

result_1 = artists.groupBy("artist") \
    .agg(count("user_id").alias("participacion")) \
    .orderBy(desc("participacion")) \
    .limit(20)

print(f"Top 20 artistas")
result_1.show(20, False)


#2 Top 20 canciones

result_2 = tracks.groupBy("track") \
    .agg(count("user_id").alias("participacion")) \
    .orderBy(desc("participacion")) \
    .limit(20)

print(f"Top 20 canciones")
result_2.show(20, False)


#3 Top 20 albumes

result_3 = albums.groupBy("album") \
    .agg(count("user_id").alias("participacion")) \
    .orderBy(desc("participacion")) \
    .limit(20)

print(f"Top 20 albumes")
result_3.show(20, False)


#4 Artista numero 1

result_4 = artists.filter(col("rank") == 1) \
    .groupBy("artist") \
    .agg(count("user_id").alias("users")) \
    .orderBy(desc("users"))

moda = result_4.first()
moda_artista = moda["artist"]
frecuencia = moda["users"]

print("Artista numero 1, moda, frecuencia")
result_4.show(truncate=False)

print(f"Moda: {moda_artista}")
print(f"Frecuencia: {frecuencia}")


#5 Menciones por artista

result_5 = artists.groupBy("artist") \
    .agg(count("*").alias("menciones")) \
    .orderBy(col("menciones").desc())

print("Menciones por artista")
result_5.show(truncate=False)


stats = result_5.select(
    mean("menciones").alias("media"),
    stddev("menciones").alias("desviacion_estandar")
).first()

media = stats["media"]
desviacion = stats["desviacion_estandar"]

print("\nMedia:", media)
print("Desviación estándar:", desviacion)


mediana = result_5.selectExpr(
    "approx_percentile(menciones, 0.5) as mediana"
).first()["mediana"]

print("Mediana:", mediana)



#6 Long tail

result_6 = artists.groupBy("artist") \
    .agg(count("*").alias("menciones")) \
    .orderBy(col("menciones").desc())


total_menciones = result_6.agg(spark_sum("menciones")).first()[0]


window = Window.orderBy(col("menciones").desc())
cumulative = result_6.withColumn(
    "menciones_acumuladas",
    spark_sum("menciones").over(window)
).withColumn(
    "porc_acumulado",
    col("menciones_acumuladas") / total_menciones
)


cumulative_80 = cumulative.filter(col("porc_acumulado") <= 0.8)

num_artistas_80 = cumulative_80.count()
total_artistas = result_6.count()


porcentaje_long_tail = num_artistas_80 / total_artistas * 100

print("Long tail")
print(f"Artistas necesarios para acumular el 80%: {num_artistas_80}")
print(f"Total de artistas: {total_artistas}")
print(f"Porcentaje de artistas (long tail): {porcentaje_long_tail:.2f}%")



#7 Items por usuario

items_artistas = artists.groupBy("user_id") \
    .agg(count("*").alias("num_artistas")
)

media_artistas = items_artistas.select(mean("num_artistas")).first()[0]

mediana_artistas = items_artistas.selectExpr(
    "approx_percentile(num_artistas, 0.5) AS mediana"
).first()["mediana"]


items_tracks = tracks.groupBy("user_id") \
    .agg(count("*").alias("num_canciones")
)

media_tracks = items_tracks.select(mean("num_canciones")).first()[0]

mediana_tracks = items_tracks.selectExpr(
    "approx_percentile(num_canciones, 0.5) AS mediana"
).first()["mediana"]


items_albums = albums.groupBy("user_id") \
    .agg(count("*").alias("num_albums")
)

media_albums = items_albums.select(mean("num_albums")).first()[0]

mediana_albums = items_albums.selectExpr(
    "approx_percentile(num_albums, 0.5) AS mediana"
).first()["mediana"]


print("Items por usuario \n")

print("Artistas por usuario")
items_artistas.show(20)
print("Media:", media_artistas)
print("Mediana:", mediana_artistas)

print("\n Canciones por usuario")
items_tracks.show(20)
print("Media:", media_tracks)
print("Mediana:", mediana_tracks)

print("\n Albumes por usuario")
items_albums.show(20)
print("Media:", media_albums)
print("Mediana:", mediana_albums)


#8 Artistas, canciones y albumes distintos

num_artistas_unicos = artists.select(countDistinct("artist")).first()[0]

num_canciones_unicas = tracks.select(countDistinct("track")).first()[0]

num_albums_unicos = albums.select(countDistinct("album")).first()[0]

print("Artistas, canciones y albumes distintos \n")
print("Número de artistas únicos:", num_artistas_unicos)
print("Número de canciones únicas:", num_canciones_unicas)
print("Número de álbumes únicos:", num_albums_unicos)



#9 Listas top 3 identicas

result_9 = artists.filter(col("rank") <= 3)

listas_top3 = result_9.groupBy("user_id") \
    .agg(concat_ws(",", collect_list("artist")).alias("top3_lista"))


listas_duplicadas = listas_top3.groupBy("top3_lista") \
    .count() \
    .filter(col("count") > 1) \
    .orderBy(col("count").desc())

listas_duplicadas.show(20, truncate=False)

print("Listas top 3 identicas \n")
print("Número de listas top-3 duplicadas:", listas_duplicadas.count())



 #10 Top 5 mismo artista

top5 = tracks.filter(col("rank") <= 5)

usuarios_artistas_distintos = top5.groupBy("user_id") \
    .agg(countDistinct("artist").alias("num_artistas_distintos"))

usuarios_gustos_concentrados = usuarios_artistas_distintos.filter(
    col("num_artistas_distintos") == 1
)

num_usuarios_concentrados = usuarios_gustos_concentrados.count()

print("Top 5 mismo artista \n")
print("Numero de usuarios con top 5 concentrado en un solo artista:", num_usuarios_concentrados)


'''
 #11 Pares de artistas mas frecuentes

from pyspark.sql.functions import explode, array_sort

user_list = artists.groupBy("user_id") \
    .agg(collect_list("artist").alias("artist_list"))

from itertools import combinations

def pairs(arr):
    return list(combinations(arr, 2))

spark.udf.register("pairs_udf", pairs)

pairs_df = user_list.select(
    explode(expr("pairs_udf(artist_list)")).alias("pair")
)

result_11 = pairs_df.groupBy("pair").agg(count("*").alias("cnt")) \
    .orderBy(desc("cnt")).limit(50)

result_11.show(50, False)
'''

'''
#12 Combinaciones de 3 artistas frecuentes

def triples(arr):
    return list(combinations(arr, 3))

spark.udf.register("triples_udf", triples)

triples_df = user_list.select(
    explode(expr("triples_udf(artist_list)")).alias("triple")
)

result_12 = triples_df.groupBy("triple").agg(count("*").alias("cnt")) \
    .orderBy(desc("cnt"))

result_12.show(20, False)
'''


#13 Solapamiento artista-cancion

top_artist = artists.filter(col("rank") == 1) \
    .select(col("user_id"), col("artist").alias("top_artist"))


top_track = tracks.filter(col("rank") == 1) \
    .select(col("user_id"), col("track"), col("artist").alias("track_artist"))

top_joined = top_artist.join(top_track, on="user_id")

solapamiento = top_joined.filter(col("top_artist") == col("track_artist"))

num_solapamiento = solapamiento.count()
total_usuarios = top_artist.count()

proporcion = num_solapamiento / total_usuarios * 100

print("Solapamiento artista-cancion \n")
print(f"Nomero de usuarios con solapamiento artista-cancion: {num_solapamiento}")
print(f"Proporcion: {proporcion:.2f}%")



#14 Posición promedio por artista

posicion_promedio = artists.groupBy("artist") \
    .agg(mean("rank").alias("posicion_media")) \
    .orderBy(col("posicion_media").desc())

print("Posicion promedio por artista \n")
posicion_promedio.show(20, truncate=False)



#15 Frecuencia de el numero 1 en top 5 global

top1_usuarios = artists.filter(col("rank") == 1).select("user_id", "artist")

artistas_populares = artists.groupBy("artist") \
    .agg(count("*").alias("menciones")) \
    .orderBy(col("menciones").desc())

top5_global = artistas_populares.limit(5).select("artist")

usuarios_top1_en_top5 = top1_usuarios.join(top5_global, on="artist", how="inner")


num_usuarios_total = top1_usuarios.count()
num_usuarios_en_top5 = usuarios_top1_en_top5.count()

proporcion = num_usuarios_en_top5 / num_usuarios_total * 100


print("Frecuencia de el numero 1 en top 5 global")
print(f"Proporcion de usuarios cuyo #1 esta en el top 5 global: {proporcion:.2f}%")


#16 Estabilidad de posiciones

top2 = artists.filter(col("rank") <= 2)

rank1 = top2.filter(col("rank") == 1).select(col("user_id"), col("artist").alias("artist_1"))
rank2 = top2.filter(col("rank") == 2).select(col("user_id"), col("artist").alias("artist_2"))

top2_joined = rank1.join(rank2, on="user_id")

usuarios_mismo_artista = top2_joined.filter(col("artist_1") == col("artist_2"))

num_usuarios_mismo_artista = usuarios_mismo_artista.count()

print("Estabilidad de posiciones \n")
print("Usuarios con el mismo artista en posiciones #1 y #2:", num_usuarios_mismo_artista)


#18 Top artistas entre oyentes

result_18 = tracks.groupBy("user_id") \
    .agg(count("track").alias("num_canciones")
)

oyentes_40 = result_18.filter(col("num_canciones") > 40)

tracks_filtrado = tracks.join(oyentes_40, on="user_id", how="inner")

top_artistas_oyentes40 = tracks_filtrado.groupBy("artist").agg(
    count("*").alias("total_reproducciones")
).orderBy(col("total_reproducciones").desc())

print(f"Top artistas entre oyentes")
top_artistas_oyentes40.show(20, False)



#19 Popularidad cruzada

tracks_counts = tracks.groupBy("artist").agg(
    count("*").alias("tracks_count")
)

artists_counts = artists.groupBy("artist").agg(
    count("*").alias("artists_count")
)

result_19 = tracks_counts.join(
    artists_counts,
    on="artist",
    how="outer"
)

result_19 = result_19.select(
    col("artist"),
    coalesce(col("tracks_count"), lit(0)).alias("tracks_count"),
    coalesce(col("artists_count"), lit(0)).alias("artists_count")
)

result_19 = result_19.withColumn(
    "diferencia",
    col("tracks_count") - col("artists_count")
)


result_19 = result_19.orderBy(col("diferencia").desc())

print(f"Popularidad cruzada")
result_19.show(50, truncate=False)



#20 Artistas diversos

usuarios_por_artista = artists.groupBy("artist").agg(
    countDistinct("user_id").alias("usuarios_distintos")
)

canciones_por_artista = tracks.groupBy("artist").agg(
    countDistinct("track").alias("canciones_distintas")
)

result_20 = usuarios_por_artista.join(
    canciones_por_artista,
    on="artist",
    how="outer"
)

result_20 = result_20.select(
    col("artist"),
    coalesce(col("usuarios_distintos"), lit(0)).alias("usuarios_distintos"),
    coalesce(col("canciones_distintas"), lit(0)).alias("canciones_distintas")
)

result_20 = result_20.orderBy(
    col("usuarios_distintos").desc()
)

print(f"Artistas diversos")
result_20.show(50, truncate=False)



#21 Datos faltantes

result_21 = users.select(
    col("user_id"),
    (col("country").isNull().cast("int")).alias("missing_country"),
    (col("total_scrobbles").isNull().cast("int")).alias("missing_scrobbles")
).withColumn(
    "total_missing",
    col("missing_country") + col("missing_scrobbles")
)

print("Datos faltantes")

num_missing_users = result_21.filter(col("total_missing") > 0).count()
print("Total de usuarios con datos faltantes:", num_missing_users)

minimo = 1

usuarios_con_pocos_items = users.filter(
    col("total_scrobbles") < minimo
)

num_pocos_items = usuarios_con_pocos_items.count()

print(f"Usuarios con menos de {minimo} scrobbles:", num_pocos_items)


#22 Usuarios extremos

result_22 = users.selectExpr(
    "approx_percentile(total_scrobbles, array(0.01, 0.99)) as p"
).first()["p"]

p1 = result_22[0]
p99 = result_22[1]

print(f"Usuarios extremos")
print("Percentil 1:", p1)
print("Percentil 99:", p99)

usuarios_atipicos = users.filter(
    (col("total_scrobbles") <= p1) | 
    (col("total_scrobbles") >= p99)
)

num_atipicos = usuarios_atipicos.count()

print(f"Usuarios atipicos: {num_atipicos}")


#23 Artistas que aparecen menos de 5 veces

result_23 = artists.groupBy("artist") \
    .agg(count("*").alias("menciones")) \
    .orderBy(col("menciones").desc())

artistas_baja_cobertura = result_23.filter(col("menciones") < 5)

num_artistas_baja = artistas_baja_cobertura.count()

print(f"Artistas que aparecen menos de 5 veces")
print(f"Artistas con menos de 5 menciones: {num_artistas_baja}")



print("\nTODOS LOS ANALISIS COMPLETADOS.\n")

spark.stop()
