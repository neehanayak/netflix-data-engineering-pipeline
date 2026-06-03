# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer — MovieLens Raw Ingest
# MAGIC
# MAGIC **Purpose:** Read raw MovieLens CSV files and land them as Delta tables with zero transformations.
# MAGIC
# MAGIC Bronze is an immutable record of exactly what arrived. No cleaning, no renaming —
# MAGIC if something goes wrong downstream we can always reprocess from here.
# MAGIC
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Read `ratings.csv` (25 M rows) |
# MAGIC | 2 | Read `movies.csv` (62 K rows) |
# MAGIC | 3 | Save both as Delta tables |
# MAGIC | 4 | Print row counts to confirm |

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config
# MAGIC
# MAGIC Change `SOURCE_PATH` to point at wherever the CSVs live.
# MAGIC On ADLS Gen2 this would be:
# MAGIC `abfss://landing@adlsnetflixpipeline.dfs.core.windows.net/movielens/`

# COMMAND ----------

# Path to the folder that contains all MovieLens CSV files.
# - Local Databricks cluster: use /dbfs/... or dbfs:/...
# - ADLS Gen2 (production):   use abfss://landing@<storage>.dfs.core.windows.net/movielens/
SOURCE_PATH = "dbfs:/FileStore/movielens/"

# Delta tables will be created in this database (schema).
# Run  `CREATE DATABASE IF NOT EXISTS bronze`  once to set it up.
TARGET_DATABASE = "bronze"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Read ratings.csv

# COMMAND ----------

# ratings.csv has 25 million rows — the main fact table for the pipeline.
# Columns: userId (int), movieId (int), rating (float), timestamp (unix int)
#
# inferSchema=True lets Spark sample the file and pick the right types
# automatically so we don't have to write a manual StructType.
# This adds a small upfront cost but keeps the code simple for Bronze.

ratings_df = (
    spark.read
    .format("csv")
    .option("header", "true")        # first row is column names
    .option("inferSchema", "true")   # auto-detect int / float / string
    .load(f"{SOURCE_PATH}ratings.csv")
)

print("ratings schema:")
ratings_df.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Read movies.csv

# COMMAND ----------

# movies.csv is the dimension table — maps movieId → title and genres.
# Columns: movieId (int), title (string), genres (pipe-separated string)
#
# genres arrives as a raw string e.g. "Adventure|Animation|Children"
# We leave it as-is here in Bronze. Splitting into an array happens in Silver.

movies_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}movies.csv")
)

print("movies schema:")
movies_df.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Save as Delta tables

# COMMAND ----------

# Write ratings to the bronze Delta table.
#
# mode("overwrite") replaces the table on each run so the notebook is
# idempotent — safe to re-run without creating duplicate data.
#
# In production you might switch to mode("append") + a deduplication
# step in Silver, but overwrite keeps things simple for now.

print("Writing bronze_ratings ...")

(
    ratings_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")   # allows schema changes between runs
    .saveAsTable(f"{TARGET_DATABASE}.bronze_ratings")
)

print("bronze_ratings saved.")

# COMMAND ----------

# Write movies to the bronze Delta table.

print("Writing bronze_movies ...")

(
    movies_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET_DATABASE}.bronze_movies")
)

print("bronze_movies saved.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Confirm row counts

# COMMAND ----------

# Read back from the Delta tables (not the original DataFrames) so we are
# confirming what actually landed on disk, not just what was in memory.

ratings_count = spark.table(f"{TARGET_DATABASE}.bronze_ratings").count()
movies_count  = spark.table(f"{TARGET_DATABASE}.bronze_movies").count()

print("=" * 45)
print("  Bronze ingest complete")
print("=" * 45)
print(f"  bronze_ratings : {ratings_count:>12,} rows")
print(f"  bronze_movies  : {movies_count:>12,} rows")
print("=" * 45)

# Expected output:
#   bronze_ratings :   25,000,095 rows
#   bronze_movies  :       62,423 rows
