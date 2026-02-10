from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ------------------------------------------------------------
# Paramètres : on met tous les chemins au même endroit.
# Comme ça, si on change l’arborescence HDFS, on modifie ici uniquement.
# ------------------------------------------------------------
RAW_BASE = "hdfs:///user/root/raw"         # données brutes (movies.csv, ratings.csv, etc.)
SILVER_OUT = "hdfs:///user/root/silver"   # dataset silver final (CSV)
RESULTS_OUT = "hdfs:///user/root/results" # résultats des requêtes (CSV)

# ------------------------------------------------------------
# Démarrage Spark
# ------------------------------------------------------------
spark = SparkSession.builder.appName("movielens-step3").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # on masque les logs INFO trop verbeux


# ============================================================
# 1) EXTRACT : lecture des CSV bruts depuis HDFS
# ============================================================
movies = spark.read.option("header", "true").csv(f"{RAW_BASE}/movies.csv")
ratings = spark.read.option("header", "true").csv(f"{RAW_BASE}/ratings.csv")

# On affiche un petit échantillon pour prouver qu’on lit bien les fichiers
print("=== Movies : aperçu (10 lignes) ===")
movies.show(10, truncate=False)

print("=== Ratings : aperçu (10 lignes) ===")
ratings.show(10, truncate=False)


# ============================================================
# 2) CAST : conversion des types (CSV -> Spark)
# Pourquoi ? Parce que sans ça Spark garde tout en 'string',
# et on ne peut pas faire correctement avg() / count() / etc.
# ============================================================
movies = movies.withColumn("movieId", F.col("movieId").cast("int"))

ratings = (ratings
    .withColumn("userId", F.col("userId").cast("int"))
    .withColumn("movieId", F.col("movieId").cast("int"))
    .withColumn("rating", F.col("rating").cast("double"))
    .withColumn("timestamp", F.col("timestamp").cast("long"))
)


# ============================================================
# 3) TRANSFORM : construction du dataset SILVER
# Objectif : obtenir exactement les colonnes suivantes :
# movieId, movie_name, year, num_ratings, genre (1 ligne par genre), avg_rating
# ============================================================

# (a) Nettoyage du titre :
# - extraire l'année si le titre finit par "(YYYY)"
# - créer movie_name = titre sans l'année
year = F.regexp_extract(F.col("title"), r"\((\d{4})\)\s*$", 1)
movie_name = F.regexp_replace(F.col("title"), r"\s*\(\d{4}\)\s*$", "")

movies_clean = (movies
    .withColumn("year", F.when(year == "", F.lit(None)).otherwise(year.cast("int")))
    .withColumn("movie_name", movie_name)
    .drop("title")  # on garde movie_name + year, donc title n'est plus nécessaire
)

# (b) Genres :
# Dans movies.csv, un film peut avoir plusieurs genres dans la même cellule :
# ex: "Action|Adventure|Thriller"
# On utilise split + explode pour obtenir une ligne par genre.
movies_genres = (movies_clean
    .withColumn("genre", F.explode(F.split(F.col("genres"), r"\|")))
    .drop("genres")
    .filter(F.col("genre") != F.lit("(no genres listed)"))
)

# (c) Ratings :
# On calcule 2 statistiques par film :
# - num_ratings : nombre total de notes
# - avg_rating  : moyenne des notes
ratings_agg = (ratings
    .groupBy("movieId")
    .agg(
        F.count(F.lit(1)).alias("num_ratings"),
        F.avg("rating").alias("avg_rating")
    )
)

# (d) Jointure :
# On fusionne les infos films (avec genres) et les stats de ratings.
# On utilise un LEFT JOIN pour garder le film même s'il n'a pas de rating (rare).
silver = (movies_genres
    .join(ratings_agg, on="movieId", how="left")
    .withColumn("num_ratings", F.coalesce(F.col("num_ratings"), F.lit(0)))
    .withColumn("avg_rating", F.col("avg_rating").cast("double"))
    .select("movieId", "movie_name", "year", "num_ratings", "genre", "avg_rating")
)

# Contrôle rapide du résultat final (avant écriture)
print("=== SILVER : aperçu (20 lignes) ===")
silver.show(20, truncate=False)

print("=== SILVER : schéma ===")
silver.printSchema()


# ============================================================
# 4) LOAD : écriture du dataset SILVER en CSV dans HDFS
# Important : Spark écrit un DOSSIER contenant un ou plusieurs part-files.
# On utilise coalesce(1) pour obtenir 1 seul fichier CSV (plus pratique pour le rendu).
# ============================================================
(silver
 .coalesce(1)
 .write.mode("overwrite")
 .option("header", "true")
 .csv(SILVER_OUT)
)

print(f"=== SILVER écrit en CSV dans : {SILVER_OUT} ===")


# ============================================================
# 5) ANALYSE : relecture du SILVER + requêtes DataFrame et SQL
# ============================================================
silver2 = spark.read.option("header", "true").option("inferSchema", "true").csv(SILVER_OUT)

# Remarque importante :
# silver2 contient plusieurs lignes par film (une par genre).
# Pour "best movie per year", on veut le meilleur film par année (pas par genre),
# donc on crée une vue "movie_level" (une ligne par film).
movie_level = (silver2
    .select("movieId", "movie_name", "year", "num_ratings", "avg_rating")
    .dropDuplicates(["movieId"])
)

# On crée des vues temporaires pour pouvoir utiliser Spark SQL
silver2.createOrReplaceTempView("silver_movies")
movie_level.createOrReplaceTempView("movie_level")


def write_result(df, name: str):
    """
    Écrit un résultat sous forme de CSV dans HDFS :
    /user/root/results/<name>/
    """
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", "true")
       .csv(f"{RESULTS_OUT}/{name}"))


# ------------------------------------------------------------
# 5.A) Requêtes avec DataFrames
# ------------------------------------------------------------

# On définit un "classement" : note moyenne décroissante, puis nombre de votes décroissant
# (et movieId pour stabiliser en cas d’égalité)
w_year = Window.partitionBy("year").orderBy(
    F.col("avg_rating").desc_nulls_last(),
    F.col("num_ratings").desc(),
    F.col("movieId").asc()
)

w_genre = Window.partitionBy("genre").orderBy(
    F.col("avg_rating").desc_nulls_last(),
    F.col("num_ratings").desc(),
    F.col("movieId").asc()
)

# 1) Meilleur film par année (niveau film)
best_movie_per_year_df = (movie_level
    .filter(F.col("year").isNotNull())
    .withColumn("rn", F.row_number().over(w_year))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .orderBy("year")
)

# 2) Meilleur film par genre (niveau genre)
best_movie_per_genre_df = (silver2
    .withColumn("rn", F.row_number().over(w_genre))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .orderBy("genre")
)

# 3) Meilleur film "Action" par année
best_action_per_year_df = (silver2
    .filter((F.col("year").isNotNull()) & (F.col("genre") == "Action"))
    .withColumn("rn", F.row_number().over(w_year))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .orderBy("year")
)

# 4) Meilleur film "Romance" par année
best_romance_per_year_df = (silver2
    .filter((F.col("year").isNotNull()) & (F.col("genre") == "Romance"))
    .withColumn("rn", F.row_number().over(w_year))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .orderBy("year")
)

print("=== Best movie per year (DF) ===")
best_movie_per_year_df.show(30, truncate=False)

print("=== Best movie per genre (DF) ===")
best_movie_per_genre_df.show(30, truncate=False)

print("=== Best Action per year (DF) ===")
best_action_per_year_df.show(30, truncate=False)

print("=== Best Romance per year (DF) ===")
best_romance_per_year_df.show(30, truncate=False)

# On sauvegarde les résultats DataFrame
write_result(best_movie_per_year_df, "best_movie_per_year_df")
write_result(best_movie_per_genre_df, "best_movie_per_genre_df")
write_result(best_action_per_year_df, "best_action_per_year_df")
write_result(best_romance_per_year_df, "best_romance_per_year_df")


# ------------------------------------------------------------
# 5.B) Les mêmes requêtes en Spark SQL
# (On utilise ROW_NUMBER() pour prendre le top 1 par groupe)
# ------------------------------------------------------------

best_movie_per_year_sql = spark.sql("""
    SELECT year, movieId, movie_name, num_ratings, avg_rating
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY year
               ORDER BY avg_rating DESC, num_ratings DESC, movieId ASC
             ) AS rn
      FROM movie_level
      WHERE year IS NOT NULL
    ) t
    WHERE rn = 1
    ORDER BY year
""")

best_movie_per_genre_sql = spark.sql("""
    SELECT genre, year, movieId, movie_name, num_ratings, avg_rating
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY genre
               ORDER BY avg_rating DESC, num_ratings DESC, movieId ASC
             ) AS rn
      FROM silver_movies
    ) t
    WHERE rn = 1
    ORDER BY genre
""")

best_action_per_year_sql = spark.sql("""
    SELECT year, movieId, movie_name, genre, num_ratings, avg_rating
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY year
               ORDER BY avg_rating DESC, num_ratings DESC, movieId ASC
             ) AS rn
      FROM silver_movies
      WHERE year IS NOT NULL AND genre = 'Action'
    ) t
    WHERE rn = 1
    ORDER BY year
""")

best_romance_per_year_sql = spark.sql("""
    SELECT year, movieId, movie_name, genre, num_ratings, avg_rating
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY year
               ORDER BY avg_rating DESC, num_ratings DESC, movieId ASC
             ) AS rn
      FROM silver_movies
      WHERE year IS NOT NULL AND genre = 'Romance'
    ) t
    WHERE rn = 1
    ORDER BY year
""")

print("=== Best movie per year (SQL) ===")
best_movie_per_year_sql.show(30, truncate=False)

print("=== Best movie per genre (SQL) ===")
best_movie_per_genre_sql.show(30, truncate=False)

print("=== Best Action per year (SQL) ===")
best_action_per_year_sql.show(30, truncate=False)

print("=== Best Romance per year (SQL) ===")
best_romance_per_year_sql.show(30, truncate=False)

# On sauvegarde les résultats SQL
write_result(best_movie_per_year_sql, "best_movie_per_year_sql")
write_result(best_movie_per_genre_sql, "best_movie_per_genre_sql")
write_result(best_action_per_year_sql, "best_action_per_year_sql")
write_result(best_romance_per_year_sql, "best_romance_per_year_sql")

print(f"=== Résultats écrits dans : {RESULTS_OUT} ===")

spark.stop()
