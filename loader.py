from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("MusicLoader") \
    .getOrCreate()


# ARTISTS


artists_df = spark.read.csv("hdfs://localhost:9000/music/raw/user_top_artists.csv", header=True)

artists_clean = artists_df.select(
    col("user_id"),
    col("rank"),
    col("artist_name").alias("artist"),
    col("playcount")
)

artists_clean.write.mode("overwrite").parquet("hdfs://localhost:9000/music/processed/artists")


# TRACKS


tracks_df = spark.read.csv("hdfs://localhost:9000/music/raw/user_top_tracks.csv", header=True)

tracks_clean = tracks_df.select(
    col("user_id"),
    col("rank"),
    col("track_name").alias("track"),
    col("artist_name").alias("artist"),
    col("playcount")
)

tracks_clean.write.mode("overwrite").parquet("hdfs://localhost:9000/music/processed/tracks")


# ALBUMS


albums_df = spark.read.csv("hdfs://localhost:9000/music/raw/user_top_albums.csv", header=True)

albums_clean = albums_df.select(
    col("user_id"),
    col("rank"),
    col("album_name").alias("album"),
    col("artist_name").alias("artist"),
    col("playcount")
)

albums_clean.write.mode("overwrite").parquet("hdfs://localhost:9000/music/processed/albums")


# USERS


users_df = spark.read.csv("hdfs://localhost:9000/music/raw/users.csv", header=True)

# Dejamos todas las columnas tal cual
users_df.write.mode("overwrite").parquet("hdfs://localhost:9000/music/processed/users")



print("Loader terminado con exito")
spark.stop()

