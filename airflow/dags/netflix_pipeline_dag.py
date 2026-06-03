"""
Netflix Data Engineering Pipeline — Airflow DAG
================================================
This DAG simulates a real-world Medallion architecture pipeline.
In production this would connect to Azure Data Factory and Databricks.
For this portfolio project each task prints what it *would* do.

Pipeline flow:
    check_source_file → ingest_to_bronze → transform_to_silver → load_to_gold
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


# ── Default arguments ─────────────────────────────────────────────────────────
# These are inherited by every task in the DAG unless a task overrides them.

default_args = {
    "owner": "data-engineering",
    "retries": 1,                         # retry once before marking as failed
    "retry_delay": timedelta(minutes=5),  # wait 5 minutes between retries
    "email_on_failure": False,            # set to True + add email in production
}


# ── Task functions ────────────────────────────────────────────────────────────
# Each function maps to one task. They are defined here and referenced below
# inside the DAG block using python_callable=...


def check_source_file(**_):
    """
    TASK 1 — check_source_file
    --------------------------
    In the real pipeline:
        Azure Data Factory would check that the Netflix titles CSV has
        arrived in the ADLS Gen2 'landing' container before we do anything.
        If the file is missing the pipeline stops here and alerts the team.

    Why this step exists:
        Failing fast on a missing source saves compute — there is no point
        spinning up Databricks clusters if there is nothing to process.
    """
    print("Checking if Netflix CSV is available from GitHub")

    # Simulate a file-availability check
    source_url = "https://raw.githubusercontent.com/datasets/netflix-shows/main/data/netflix-shows.csv"
    print(f"Source URL  : {source_url}")
    print(f"Status      : File found — ready to ingest")

    return True   # return value is stored in XCom so downstream tasks can read it


def ingest_to_bronze(**_):
    """
    TASK 2 — ingest_to_bronze
    -------------------------
    In the real pipeline:
        ADF copies the raw CSV from the landing zone into the Bronze Delta
        table in ADLS Gen2 with zero transformations — exactly as received.
        An Autoloader notebook on Databricks then picks up new files
        automatically using cloud file notifications.

    Why Bronze exists (the 'Raw' layer):
        Bronze is an immutable historical record. If a bug is introduced in
        Silver or Gold we can always re-process from Bronze without going
        back to the source. Think of it as a cheap insurance policy.
    """
    ingestion_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print("Ingesting raw data into Bronze layer in ADLS")
    print(f"Ingestion timestamp : {ingestion_time}")
    print(f"Target container    : adls://adlsnetflixpipeline/bronze/netflix_titles/")
    print(f"Format              : Delta Lake (append-only, no schema changes)")
    print(f"Status              : Raw data landed successfully")


def transform_to_silver(**_):
    """
    TASK 3 — transform_to_silver
    ----------------------------
    In the real pipeline:
        A PySpark notebook on Databricks reads from Bronze, applies
        cleaning rules, and writes to the Silver Delta table:
          - Drop rows where title or type is null
          - Parse 'date_added' string → proper DateType
          - Split 'listed_in' comma string → array of genres
          - Deduplicate on show_id
          - Enforce schema with Delta constraints

    Why Silver exists (the 'Cleaned' layer):
        Silver is the single source of truth for analysts and data
        scientists. It is clean, typed, and deduplicated — but still
        granular. All downstream Gold aggregations read from here.
    """
    # Placeholder row count — in production this would come from the
    # Databricks job output via XCom or the ADF pipeline run metrics.
    placeholder_row_count = 8_807

    print("Cleaning and transforming data into Silver layer")
    print(f"Rows processed      : {placeholder_row_count:,}")
    print(f"Null titles dropped : 3")
    print(f"Duplicates removed  : 12")
    print(f"Target table        : silver.netflix_titles")
    print(f"Status              : Silver table refreshed successfully")


def load_to_gold(**_):
    """
    TASK 4 — load_to_gold
    ---------------------
    In the real pipeline:
        A Databricks Delta Live Tables (DLT) pipeline reads from Silver
        and builds several Gold aggregation tables consumed by dashboards:
          - gold.titles_by_country   → total titles per country
          - gold.titles_by_year      → release trend over time
          - gold.genre_distribution  → top genres by content type
          - gold.movies_vs_shows     → Movie vs TV Show split

    Why Gold exists (the 'Business' layer):
        Gold tables are pre-aggregated and optimised for fast reads by
        Power BI, Databricks SQL, and ML feature pipelines. Analysts
        never query raw Bronze/Silver directly.
    """
    completed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print("Loading business-ready data into Gold layer")
    print(f"Tables refreshed    : titles_by_country, titles_by_year, genre_distribution, movies_vs_shows")
    print(f"Completed at        : {completed_at}")
    print("Pipeline complete!")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="netflix_pipeline",
    description="Medallion architecture: Bronze → Silver → Gold for Netflix titles data",
    schedule_interval="0 2 * * *",   # cron: every day at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,                   # don't backfill historical runs
    default_args=default_args,
    tags=["netflix", "medallion", "portfolio"],
) as dag:

    # ── Task 1 ────────────────────────────────────────────────────────────────
    # Verify the source data file exists before starting any processing.
    # In production: ADF sensor checking the ADLS landing container.
    check_source_file = PythonOperator(
        task_id="check_source_file",
        python_callable=check_source_file,
    )

    # ── Task 2 ────────────────────────────────────────────────────────────────
    # Copy raw data as-is into the Bronze (raw) Delta table.
    # In production: ADF Copy Activity + Databricks Autoloader notebook.
    ingest_to_bronze = PythonOperator(
        task_id="ingest_to_bronze",
        python_callable=ingest_to_bronze,
    )

    # ── Task 3 ────────────────────────────────────────────────────────────────
    # Clean, deduplicate, and type-cast data into the Silver (curated) table.
    # In production: Databricks PySpark notebook job.
    transform_to_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
    )

    # ── Task 4 ────────────────────────────────────────────────────────────────
    # Build business aggregation tables in the Gold layer for dashboards/ML.
    # In production: Databricks Delta Live Tables (DLT) pipeline.
    load_to_gold = PythonOperator(
        task_id="load_to_gold",
        python_callable=load_to_gold,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    # The >> operator sets execution order: each task waits for the one before it.
    check_source_file >> ingest_to_bronze >> transform_to_silver >> load_to_gold
