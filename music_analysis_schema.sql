DROP DATABASE IF EXISTS music_analysis;
CREATE DATABASE music_analysis;
USE music_analysis;
-- ================================================
-- CREAR USUARIO GENERAL PARA EL PROYECTO
-- ================================================
DROP USER IF EXISTS 'ProyectoDB'@'localhost';
CREATE USER 'ProyectoDB'@'localhost' IDENTIFIED BY 'PDB';

GRANT ALL PRIVILEGES ON music_db.* TO 'ProyectoDB'@'localhost';
FLUSH PRIVILEGES;

-- =========================================================
-- ANALISIS #1 — Top artistas
-- =========================================================
DROP TABLE IF EXISTS result_1_top_artists;
CREATE TABLE result_1_top_artists (
    artist VARCHAR(255) PRIMARY KEY,
    participacion INT
);

-- =========================================================
-- ANALISIS #2 — Top tracks
-- =========================================================
DROP TABLE IF EXISTS result_2_top_tracks;
CREATE TABLE result_2_top_tracks (
    track VARCHAR(255) PRIMARY KEY,
    participacion INT
);

-- =========================================================
-- ANALISIS #3 — Top albums
-- =========================================================
DROP TABLE IF EXISTS result_3_top_albums;
CREATE TABLE result_3_top_albums (
    album VARCHAR(255) PRIMARY KEY,
    participacion INT
);

-- =========================================================
-- ANALISIS #4 — Artista top-1 más repetido
-- =========================================================
DROP TABLE IF EXISTS result_4_top1_artist;
CREATE TABLE result_4_top1_artist (
    artist VARCHAR(255) PRIMARY KEY,
    users INT
);

-- =========================================================
-- ANALISIS #5 — Menciones por artista + estadísticas
-- =========================================================

DROP TABLE IF EXISTS result_5_mentions_per_artist;
CREATE TABLE result_5_mentions_per_artist (
    artist VARCHAR(255) PRIMARY KEY,
    menciones INT
);

DROP TABLE IF EXISTS result_5_stats;
CREATE TABLE result_5_stats (
    media DECIMAL(12,4),
    mediana DECIMAL(12,4),
    desviacion_estandar DECIMAL(12,4)
);

-- =========================================================
-- ANALISIS #6 — Long tail
-- =========================================================
DROP TABLE IF EXISTS result_6_long_tail;
CREATE TABLE result_6_long_tail (
    artistas_para_80 INT,
    total_artistas INT,
    porcentaje DECIMAL(12,4)
);

-- =========================================================
-- ANALISIS #7 — Items por usuario + estadísticas
-- =========================================================

DROP TABLE IF EXISTS result_7_user_artists;
CREATE TABLE result_7_user_artists (
    user_id VARCHAR(255),
    num_artistas INT,
    PRIMARY KEY (user_id)
);

DROP TABLE IF EXISTS result_7_user_tracks;
CREATE TABLE result_7_user_tracks (
    user_id VARCHAR(255),
    num_canciones INT,
    PRIMARY KEY (user_id)
);

DROP TABLE IF EXISTS result_7_user_albums;
CREATE TABLE result_7_user_albums (
    user_id VARCHAR(255),
    num_albums INT,
    PRIMARY KEY (user_id)
);

DROP TABLE IF EXISTS result_7_stats;
CREATE TABLE result_7_stats (
    media_artistas DECIMAL(12,4),
    mediana_artistas DECIMAL(12,4),
    media_tracks DECIMAL(12,4),
    mediana_tracks DECIMAL(12,4),
    media_albums DECIMAL(12,4),
    mediana_albums DECIMAL(12,4)
);

-- =========================================================
-- ANALISIS #8 — Elementos únicos
-- =========================================================
DROP TABLE IF EXISTS result_8_unique_counts;
CREATE TABLE result_8_unique_counts (
    artistas_unicos INT,
    canciones_unicas INT,
    albums_unicos INT
);

-- =========================================================
-- ANALISIS #9 — Top-3 duplicados
-- =========================================================
DROP TABLE IF EXISTS result_9_top3_duplicates;
CREATE TABLE result_9_top3_duplicates (
    top3_lista VARCHAR(500),
    cantidad INT
);

-- =========================================================
-- ANALISIS #10 — Usuarios con top 5 del mismo artista
-- =========================================================
DROP TABLE IF EXISTS result_10_concentrated_users;
CREATE TABLE result_10_concentrated_users (
    total INT
);

-- =========================================================
-- ANALISIS #11 y #12 tienen problemas
-- =========================================================

-- =========================================================
-- ANALISIS #13 — Solapamiento artista-canción
-- =========================================================
DROP TABLE IF EXISTS result_13_overlap;
CREATE TABLE result_13_overlap (
    solapamientos INT,
    total_usuarios INT,
    proporcion DECIMAL(12,4)
);

-- =========================================================
-- ANALISIS #14 — Posición promedio por artista
-- =========================================================
DROP TABLE IF EXISTS result_14_avg_position;
CREATE TABLE result_14_avg_position (
    artist VARCHAR(255) PRIMARY KEY,
    posicion_media DECIMAL(12,4)
);

-- =========================================================
-- ANALISIS #15 — Top 1 está en top 5 global
-- =========================================================
DROP TABLE IF EXISTS result_15_top1_in_global5;
CREATE TABLE result_15_top1_in_global5 (
    total_usuarios INT,
    usuarios_en_top5 INT,
    proporcion DECIMAL(12,4)
);

-- =========================================================
-- ANALISIS #16 — Estabilidad de posiciones
-- =========================================================
DROP TABLE IF EXISTS result_16_position_stability;
CREATE TABLE result_16_position_stability (
    total INT
);

-- =========================================================
-- ANALISIS #18 — Top artistas en oyentes mayores de 40
-- =========================================================
DROP TABLE IF EXISTS result_18_heavy_listeners;
CREATE TABLE result_18_heavy_listeners (
    artist VARCHAR(255) PRIMARY KEY,
    total_reproducciones INT
);

-- =========================================================
-- ANALISIS #19 — Popularidad cruzada
-- =========================================================
DROP TABLE IF EXISTS result_19_cross_popularity;
CREATE TABLE result_19_cross_popularity (
    artist VARCHAR(255) PRIMARY KEY,
    tracks_count INT,
    artists_count INT,
    diferencia INT
);

-- =========================================================
-- ANALISIS #20 — Artistas diversos
-- =========================================================
DROP TABLE IF EXISTS result_20_diverse_artists;
CREATE TABLE result_20_diverse_artists (
    artist VARCHAR(255) PRIMARY KEY,
    usuarios_distintos INT,
    canciones_distintas INT
);

-- =========================================================
-- ANALISIS #21 — Datos faltantes
-- =========================================================
DROP TABLE IF EXISTS result_21_missing_data;
CREATE TABLE result_21_missing_data (
    missing_users INT,
    pocos_items INT
);

-- =========================================================
-- ANALISIS #22 — Usuarios extremos
-- =========================================================
DROP TABLE IF EXISTS result_22_extreme_users;
CREATE TABLE result_22_extreme_users (
    percentil_1 DECIMAL(12,4),
    percentil_99 DECIMAL(12,4),
    total_atipicos INT
);

-- =========================================================
-- ANALISIS #23 — Artistas con <5 menciones
-- =========================================================
DROP TABLE IF EXISTS result_23_low_coverage_artists;
CREATE TABLE result_23_low_coverage_artists (
    artist VARCHAR(255) PRIMARY KEY,
    menciones INT
);