from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, avg, stddev, expr, lit, collect_list, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalyses").getOrCreate()

# Load data

artists = spark.read.parquet("hdfs://localhost:9000/music/processed/artists")
tracks = spark.read.parquet("hdfs://localhost:9000/music/processed/tracks")
albums = spark.read.parquet("hdfs://localhost:9000/music/processed/albums")
users = spark.read.parquet("hdfs://localhost:9000/music/processed/users")


# Top 20 artistas 

result_1 = artists.groupBy("artist") \
    .agg(count("*").alias("mentions")) \
    .orderBy(desc("mentions")) \
    .limit(20)

result_1.show(20, False)



# Top 20 canciones

result_2 = tracks.groupBy("track") \
    .agg(count("*").alias("mentions")) \
    .orderBy(desc("mentions")) \
    .limit(20)

result_2.show(20, False)



# Top 20 albumes

result_3 = albums.groupBy("album") \
    .agg(count("*").alias("mentions")) \
    .orderBy(desc("mentions")) \
    .limit(20)

result_3.show(20, False)



# Artista numero 1

result_4 = artists.filter(col("rank") == 1) \
    .groupBy("artist") \
    .agg(count("*").alias("count_users")) \
    .orderBy(desc("count_users"))

result_4.show(20, False)



# Menciones por artista

artist_counts = artists.groupBy("artist").agg(count("*").alias("mentions"))

result_5 = artist_counts.select(
    avg("mentions").alias("mean"),
    expr("percentile_approx(mentions, 0.5)").alias("median"),
    stddev("mentions").alias("stddev")
)

result_5.show()



# Long tail

total_mentions = artist_counts.agg({"mentions": "sum"}).first()[0]

w = Window.orderBy(desc("mentions"))

cumulated = artist_counts.withColumn("cum_sum", expr("sum(mentions) over (order by mentions desc)")) \
                         .withColumn("percentage", col("cum_sum") / total_mentions)

result_6 = cumulated.filter(col("percentage") <= 0.8)

result_6_count = result_6.count()

print("Cantidad de artistas que representan 80% de menciones:", result_6_count)



# Items por usuario

result_7 = artists.groupBy("user_id").agg(count("*").alias("n_artists"))

result_7_stats = result_7.select(
    avg("n_artists").alias("mean"),
    expr("percentile_approx(n_artists, 0.5)").alias("median")
)

result_7_stats.show()



# Artistas, canciones y albumes distintos

result_8 = spark.createDataFrame([
    ("artists", artists.select("artist").distinct().count()),
    ("tracks", tracks.select("track").distinct().count()),
    ("albums", albums.select("album").distinct().count())
], ["category", "unique_count"])

result_8.show()



# Listas top 3 identicas

w2 = Window.partitionBy("user_id").orderBy("rank")

user_top3 = artists.filter(col("rank") <= 3) \
    .groupBy("user_id") \
    .agg(collect_list("artist").alias("top3"))

result_9 = user_top3.groupBy("top3") \
    .agg(count("*").alias("dup_count")) \
    .orderBy(desc("dup_count"))

result_9.show(20, False)



 # Top 5 mismo artista

top5 = artists.filter(col("rank") <= 5)

result_10 = top5.groupBy("user_id", "artist") \
    .agg(count("*").alias("c")) \
    .filter(col("c") == 5)

result_10.show(20, False)



 # Pares de artistas mas frecuentes

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



# Combinaciones de 3 artistas frecuentes

def triples(arr):
    return list(combinations(arr, 3))

spark.udf.register("triples_udf", triples)

triples_df = user_list.select(
    explode(expr("triples_udf(artist_list)")).alias("triple")
)

result_12 = triples_df.groupBy("triple").agg(count("*").alias("cnt")) \
    .orderBy(desc("cnt"))

result_12.show(20, False)



# Solapamiento artista-canción

user_top_track = tracks.filter(col("rank") == 1).select("user_id", col("artist").alias("top_track_artist"))
user_top_artist = artists.filter(col("rank") == 1).select("user_id", col("artist").alias("top_artist"))

merged = user_top_artist.join(user_top_track, "user_id")

result_13 = merged.filter(col("top_artist") == col("top_track_artist")).count()

print("Usuarios donde #1 track pertenece al #1 artist:", result_13)



# Posición promedio por artista

result_14 = artists.groupBy("artist").agg(avg("rank").alias("avg_position")) \
    .orderBy("avg_position")

result_14.show(20, False)



# Frecuencia de el numero 1

top5_global = result_1.limit(5).select("artist").rdd.flatMap(lambda x: x).collect()

result_15 = artists.filter(col("rank") == 1) \
    .filter(col("artist").isin(top5_global)) \
    .count()

print("Usuarios cuyo #1 está en el top5 global:", result_15)



# Estabilidad de posiciones

rank1 = artists.filter(col("rank") == 1).select("user_id", col("artist").alias("a1"))
rank2 = artists.filter(col("rank") == 2).select("user_id", col("artist").alias("a2"))

result_16 = rank1.join(rank2, "user_id") \
    .filter(col("a1") == col("a2"))

print("Usuarios con mismo artista en #1 y #2:", result_16.count())



# Top artistas oyentes

listeners = tracks.groupBy("user_id").agg(count("*").alias("n_tracks")) \
                  .filter(col("n_tracks") > 40) \
                  .select("user_id")

result_18 = listeners.join(artists.filter(col("rank") == 1), "user_id") \
                     .groupBy("artist") \
                     .agg(count("*").alias("count")) \
                     .orderBy(desc("count"))

result_18.show(20, False)



# Popularidad cruzada

artist_counts_tracks = tracks.groupBy("artist").count().withColumnRenamed("count", "track_count")
artist_counts_artists = artists.groupBy("artist").count().withColumnRenamed("count", "artist_count")

result_19 = artist_counts_artists.join(artist_counts_tracks, "artist") \
                                .select("artist", "artist_count", "track_count",
                                        (col("artist_count") - col("track_count")).alias("diff")) \
                                .orderBy(desc("diff"))

result_19.show(20, False)



# Artistas diversos

result_20 = artists.groupBy("artist") \
    .agg(count("*").alias("user_mentions"))

result_20.show(20, False)



# Datos faltantes

result_21 = users.select(
    count("*").alias("rows"),
    count("user_id").alias("user_id_ok"),
    count("country").alias("country_ok")
)

result_21.show()



# Usuarios extremos

user_counts = artists.groupBy("user_id").agg(count("*").alias("n_items"))

p99 = user_counts.agg(expr("percentile_approx(n_items, 0.99)")).first()[0]

result_22 = user_counts.filter(col("n_items") >= p99)

result_22.show(20, False)



# Artistas que aparecen menos de 5 veces

result_23 = artist_counts.filter(col("mentions") < 5)

result_23.show(20, False)



print("\nTODOS LOS ANALISIS COMPLETADOS.\n")

spark.stop()
