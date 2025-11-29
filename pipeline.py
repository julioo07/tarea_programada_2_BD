import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, avg, mean, stddev, expr, lit, collect_list, row_number, sum as spark_sum, approx_percentile, coalesce, countDistinct, concat_ws
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

BASE = os.path.dirname(os.path.abspath(__file__))

spark = (
    SparkSession.builder.appName("MusicAnalysesPipeline")
    .config(
        "spark.jars",
        f"{BASE}/libs/mysql-connector-j-9.0.0.jar"
    )
    .getOrCreate()
)

# Funcion para exportar a MySQL
def write_mysql(df: DataFrame, table: str):
    (
        df.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/music_analysis")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", table)
        .option("user", "ProyectoDB")
        .option("password", "PDB")   
        .mode("overwrite")
        .save()
    )

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

write_mysql(
    result_1.select(col("artist").cast("string"), col("participacion").cast("int")),
    "result_1_top_artists"
)

#2 Top 20 canciones

result_2 = tracks.groupBy("track") \
    .agg(count("user_id").alias("participacion")) \
    .orderBy(desc("participacion")) \
    .limit(20)

write_mysql(
    result_2.select(col("track").cast("string"), col("participacion").cast("int")),
    "result_2_top_tracks"
)


#3 Top 20 albumes

result_3 = albums.groupBy("album") \
    .agg(count("user_id").alias("participacion")) \
    .orderBy(desc("participacion")) \
    .limit(20)

write_mysql(
    result_3.select(col("album").cast("string"), col("participacion").cast("int")),
    "result_3_top_albums"
)


#4 Artista numero 1

result_4 = artists.filter(col("rank") == 1) \
    .groupBy("artist") \
    .agg(count("user_id").alias("users")) \
    .orderBy(desc("users"))

write_mysql(
    result_4.select(col("artist").cast("string"), col("users").cast("int")),
    "result_4_top1_artist"
)

#5 Menciones por artista

result_5 = artists.groupBy("artist") \
    .agg(count("*").alias("menciones")) \
    .orderBy(col("menciones").desc())

write_mysql(
    result_5.select(col("artist").cast("string"), col("menciones").cast("int")),
    "result_5_mentions_per_artist"
)

stats = result_5.select(
    mean("menciones").alias("media"),
    stddev("menciones").alias("desviacion_estandar")
).first()

media = stats["media"]
desviacion = stats["desviacion_estandar"]
mediana = result_5.selectExpr(
    "approx_percentile(menciones, 0.5) as mediana"
).first()["mediana"]

write_mysql(
    spark.createDataFrame([{
        "media": float(media),
        "mediana": float(mediana),
        "desviacion_estandar": float(desviacion)
    }]),
    "result_5_stats"
)


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

write_mysql(
    spark.createDataFrame([{
        "artistas_para_80": int(num_artistas_80),
        "total_artistas": int(total_artistas),
        "porcentaje": float(porcentaje_long_tail)
    }]),
    "result_6_long_tail"
)

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


write_mysql(
    items_artistas.select(col("user_id").cast("string"), col("num_artistas").cast("int")),
    "result_7_user_artists"
)
write_mysql(
    items_tracks.select(col("user_id").cast("string"), col("num_canciones").cast("int")),
    "result_7_user_tracks"
)
write_mysql(
    items_albums.select(col("user_id").cast("string"), col("num_albums").cast("int")),
    "result_7_user_albums"
)

write_mysql(
    spark.createDataFrame([{
        "media_artistas": float(media_artistas),
        "mediana_artistas": float(mediana_artistas),
        "media_tracks": float(media_tracks),
        "mediana_tracks": float(mediana_tracks),
        "media_albums": float(media_albums),
        "mediana_albums": float(mediana_albums)
    }]),
    "result_7_stats"
)

#8 Artistas, canciones y albumes distintos

num_artistas_unicos = artists.select(countDistinct("artist")).first()[0]

num_canciones_unicas = tracks.select(countDistinct("track")).first()[0]

num_albums_unicos = albums.select(countDistinct("album")).first()[0]

write_mysql(
    spark.createDataFrame([{
        "artistas_unicos": int(num_artistas_unicos),
        "canciones_unicas": int(num_canciones_unicas),
        "albums_unicos": int(num_albums_unicos),
    }]),
    "result_8_unique_counts"
)


#9 Listas top 3 identicas

result_9 = artists.filter(col("rank") <= 3)

listas_top3 = result_9.groupBy("user_id") \
    .agg(concat_ws(",", collect_list("artist")).alias("top3_lista"))


listas_duplicadas = listas_top3.groupBy("top3_lista") \
    .count() \
    .filter(col("count") > 1) \
    .orderBy(col("count").desc())

write_mysql(
    listas_duplicadas.select(
        col("top3_lista").cast("string"),
        col("count").alias("cantidad").cast("int")
    ),
    "result_9_top3_duplicates"
)

 #10 Top 5 mismo artista

top5 = tracks.filter(col("rank") <= 5)

usuarios_artistas_distintos = top5.groupBy("user_id") \
    .agg(countDistinct("artist").alias("num_artistas_distintos"))

usuarios_gustos_concentrados = usuarios_artistas_distintos.filter(
    col("num_artistas_distintos") == 1
)

num_usuarios_concentrados = usuarios_gustos_concentrados.count()

write_mysql(
    spark.createDataFrame([{"total": int(num_usuarios_concentrados)}]),
    "result_10_concentrated_users"
)

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

write_mysql(
    spark.createDataFrame([{
        "solapamientos": int(num_solapamiento),
        "total_usuarios": int(total_usuarios),
        "proporcion": float(proporcion)
    }]),
    "result_13_overlap"
)


#14 Posición promedio por artista

posicion_promedio = artists.groupBy("artist") \
    .agg(mean("rank").alias("posicion_media")) \
    .orderBy(col("posicion_media").desc())

write_mysql(
    posicion_promedio.select(
        col("artist").cast("string"),
        col("posicion_media").cast("double")
    ),
    "result_14_avg_position"
)

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

write_mysql(
    spark.createDataFrame([{
        "total_usuarios": int(num_usuarios_total),
        "usuarios_en_top5": int(num_usuarios_en_top5),
        "proporcion": float(proporcion)
    }]),
    "result_15_top1_in_global5"
)

#16 Estabilidad de posiciones

top2 = artists.filter(col("rank") <= 2)

rank1 = top2.filter(col("rank") == 1).select(col("user_id"), col("artist").alias("artist_1"))
rank2 = top2.filter(col("rank") == 2).select(col("user_id"), col("artist").alias("artist_2"))

top2_joined = rank1.join(rank2, on="user_id")

usuarios_mismo_artista = top2_joined.filter(col("artist_1") == col("artist_2"))

num_usuarios_mismo_artista = usuarios_mismo_artista.count()

write_mysql(
    spark.createDataFrame([{"total": int(num_usuarios_mismo_artista)}]),
    "result_16_position_stability"
)

#18 Top artistas entre oyentes

result_18 = tracks.groupBy("user_id") \
    .agg(count("track").alias("num_canciones")
)

oyentes_40 = result_18.filter(col("num_canciones") > 40)

tracks_filtrado = tracks.join(oyentes_40, on="user_id", how="inner")

top_artistas_oyentes40 = tracks_filtrado.groupBy("artist").agg(
    count("*").alias("total_reproducciones")
).orderBy(col("total_reproducciones").desc())

write_mysql(
    top_artistas_oyentes40.select(
        col("artist").cast("string"),
        col("total_reproducciones").cast("int")
    ),
    "result_18_heavy_listeners"
)

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

write_mysql(
    result_19.select(
        col("artist").cast("string"),
        col("tracks_count").cast("int"),
        col("artists_count").cast("int"),
        col("diferencia").cast("int")
    ),
    "result_19_cross_popularity"
)

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

write_mysql(
    result_20.select(
        col("artist").cast("string"),
        col("usuarios_distintos").cast("int"),
        col("canciones_distintas").cast("int")
    ),
    "result_20_diverse_artists"
)


#21 Datos faltantes

result_21 = users.select(
    col("user_id"),
    (col("country").isNull().cast("int")).alias("missing_country"),
    (col("total_scrobbles").isNull().cast("int")).alias("missing_scrobbles")
).withColumn(
    "total_missing",
    col("missing_country") + col("missing_scrobbles")
)

num_missing_users = result_21.filter(col("total_missing") > 0).count()

minimo = 1

usuarios_con_pocos_items = users.filter(
    col("total_scrobbles") < minimo
)

num_pocos_items = usuarios_con_pocos_items.count()

write_mysql(
    spark.createDataFrame([{
        "missing_users": int(usuarios_con_pocos_items),
        "pocos_items": int(num_pocos_items)
    }]),
    "result_21_missing_data"
)

#22 Usuarios extremos

result_22 = users.selectExpr(
    "approx_percentile(total_scrobbles, array(0.01, 0.99)) as p"
).first()["p"]

p1 = result_22[0]
p99 = result_22[1]


usuarios_atipicos = users.filter(
    (col("total_scrobbles") <= p1) | 
    (col("total_scrobbles") >= p99)
)

num_atipicos = usuarios_atipicos.count()

write_mysql(
    spark.createDataFrame([{
        "percentil_1": float(p1),
        "percentil_99": float(p99),
        "total_atipicos": int(num_atipicos)
    }]),
    "result_22_extreme_users"
)

#23 Artistas que aparecen menos de 5 veces

result_23 = artists.groupBy("artist") \
    .agg(count("*").alias("menciones")) \
    .orderBy(col("menciones").desc())

artistas_baja_cobertura = result_23.filter(col("menciones") < 5)

write_mysql(
    artistas_baja_cobertura.select(col("artist").cast("string"), col("menciones").cast("int")),
    "result_23_low_coverage_artists"
)

print("\n ===============================")
print("  TODOS LOS RESULTADOS EXPORTADOS A MYSQL")
print(" ===============================\n")

spark.stop()
