-- ============================================================
-- MovieLens - Hive ETL + Analyses
-- ============================================================
-- Objectif :
-- 1) Créer 2 tables externes (movies + ratings) pointant vers HDFS
-- 2) Construire une table "silver_movies" (parquet) avec :
--    movieId, movie_name, year, num_ratings, genre (1 ligne/genre), avg_rating
-- 3) Répondre aux 4 questions d’analyse en Hive SQL
-- ============================================================

CREATE DATABASE IF NOT EXISTS movielens;
USE movielens;

-- ------------------------------------------------------------
-- IMPORTANT (organisation HDFS) :
-- On préfère pointer chaque table externe vers un dossier HDFS
-- contenant uniquement le fichier attendu.
-- Exemple :
--   /user/root/raw_hive/movies/  -> movies.csv uniquement
--   /user/root/raw_hive/ratings/ -> ratings.csv uniquement
--
-- Pourquoi ?
-- Une table EXTERNAL Hive lit *tous* les fichiers du dossier LOCATION
-- avec le même schéma. Si on mélange plusieurs CSV différents,
-- Hive va essayer de les lire pareil => erreurs / données incohérentes.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- 1) Tables externes RAW : movies.csv et ratings.csv
-- On utilise OpenCSVSerde pour gérer correctement les champs entre guillemets,
-- notamment les titres contenant des virgules.
-- ------------------------------------------------------------

DROP TABLE IF EXISTS movies_raw;
CREATE EXTERNAL TABLE movies_raw (
  movieId INT,
  title   STRING,
  genres  STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\"",
  "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/user/root/raw_hive/movies'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS ratings_raw;
CREATE EXTERNAL TABLE ratings_raw (
  userId INT,
  movieId INT,
  rating DOUBLE,
  ts BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\"",
  "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/user/root/raw_hive/ratings'
TBLPROPERTIES ("skip.header.line.count"="1");

-- ------------------------------------------------------------
-- 2) Table SILVER Hive (stockage Parquet)
-- Parquet est plus efficace pour les requêtes (colonnaire).
-- ------------------------------------------------------------

DROP TABLE IF EXISTS silver_movies;
CREATE TABLE silver_movies (
  movieId INT,
  movie_name STRING,
  year INT,
  num_ratings BIGINT,
  genre STRING,
  avg_rating DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- ------------------------------------------------------------
-- Construction du SILVER :
-- - Extraire year depuis "(YYYY)" dans title
-- - movie_name = title sans l'année
-- - Exploser genres => 1 ligne par genre
-- - Agréger ratings (COUNT, AVG) par movieId
-- - LEFT JOIN pour garder tous les films
-- ------------------------------------------------------------

WITH movies_clean AS (
  SELECT
    movieId,
    regexp_replace(
      regexp_replace(title, '\\s*\\([0-9]{4}\\)\\s*$', ''),
      ',', ''
    ) AS movie_name,
    CASE
      WHEN regexp_extract(title, '\\(([0-9]{4})\\)\\s*$', 1) = '' THEN NULL
      ELSE CAST(regexp_extract(title, '\\(([0-9]{4})\\)\\s*$', 1) AS INT)
    END AS year,
    genres
  FROM movies_raw
),
movies_exploded AS (
  SELECT
    movieId,
    movie_name,
    year,
    genre
  FROM movies_clean
  LATERAL VIEW explode(split(genres, '\\|')) g AS genre
  WHERE genre <> '(no genres listed)'
),
ratings_agg AS (
  SELECT
    movieId,
    COUNT(1) AS num_ratings,
    AVG(rating) AS avg_rating
  FROM ratings_raw
  GROUP BY movieId
)
INSERT OVERWRITE TABLE silver_movies
SELECT
  m.movieId,
  m.movie_name,
  m.year,
  COALESCE(r.num_ratings, 0) AS num_ratings,
  m.genre,
  r.avg_rating
FROM movies_exploded m
LEFT JOIN ratings_agg r
ON m.movieId = r.movieId;

-- Vérification silver :
-- SELECT * FROM silver_movies LIMIT 20;

-- ------------------------------------------------------------
-- 3) Requêtes d’analyse Hive
-- Règle de ranking :
-- 1) avg_rating DESC
-- 2) num_ratings DESC (si égalité)
-- 3) movieId ASC (stabilise les égalités)
-- ------------------------------------------------------------

-- 3.1 Best movie per year (niveau film, pas "une ligne par genre")
-- Comme silver_movies a plusieurs lignes par film (une par genre),
-- on reconstruit une vue au niveau film avant de classer.
WITH movie_level AS (
  SELECT
    movieId, movie_name, year,
    MAX(num_ratings) AS num_ratings,
    MAX(avg_rating) AS avg_rating
  FROM silver_movies
  WHERE year IS NOT NULL
  GROUP BY movieId, movie_name, year
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY year
      ORDER BY avg_rating DESC, num_ratings DESC, movieId ASC
    ) AS rn
  FROM movie_level
)
SELECT year, movieId, movie_name, num_ratings, avg_rating
FROM ranked
WHERE rn = 1
ORDER BY year;

-- 3.2 Best movie per genre
WITH ranked AS (
  SELECT
    genre, year, movieId, movie_name, num_ratings, avg_rating,
    ROW_NUMBER() OVER (
      PARTITION BY genre
      ORDER BY avg_rating DESC, num_ratings DESC, movieId ASC
    ) AS rn
  FROM silver_movies
)
SELECT genre, year, movieId, movie_name, num_ratings, avg_rating
FROM ranked
WHERE rn = 1
ORDER BY genre;

-- 3.3 Best 'Action' movie per year
WITH ranked AS (
  SELECT
    year, movieId, movie_name, genre, num_ratings, avg_rating,
    ROW_NUMBER() OVER (
      PARTITION BY year
      ORDER BY avg_rating DESC, num_ratings DESC, movieId ASC
    ) AS rn
  FROM silver_movies
  WHERE year IS NOT NULL AND genre = 'Action'
)
SELECT year, movieId, movie_name, genre, num_ratings, avg_rating
FROM ranked
WHERE rn = 1
ORDER BY year;

-- 3.4 Best 'Romance' movie per year
WITH ranked AS (
  SELECT
    year, movieId, movie_name, genre, num_ratings, avg_rating,
    ROW_NUMBER() OVER (
      PARTITION BY year
      ORDER BY avg_rating DESC, num_ratings DESC, movieId ASC
    ) AS rn
  FROM silver_movies
  WHERE year IS NOT NULL AND genre = 'Romance'
)
SELECT year, movieId, movie_name, genre, num_ratings, avg_rating
FROM ranked
WHERE rn = 1
ORDER BY year;
