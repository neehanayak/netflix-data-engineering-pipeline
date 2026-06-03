# Netflix Data Engineering Pipeline

A production-grade data engineering pipeline built on Azure, inspired by Netflix's data platform architecture. This project demonstrates an end-to-end Medallion architecture for ingesting, transforming, and serving streaming analytics data.

---

## Medallion Architecture

```
Raw Sources  →  Bronze (Raw)  →  Silver (Cleaned)  →  Gold (Aggregated)
```

| Layer | Description | Storage Format | Typical Use |
|-------|-------------|----------------|-------------|
| **Bronze** | Raw ingested data, exactly as received. No transformations, immutable. | Delta Lake (append-only) | Auditing, reprocessing |
| **Silver** | Cleaned, deduplicated, and schema-enforced records. | Delta Lake | Data science, feature engineering |
| **Gold** | Business-level aggregates and KPIs, optimized for reporting. | Delta Lake | Dashboards, BI tools, ML models |

---

## Tech Stack

| Component | Tool | Purpose |
|-----------|------|---------|
| **Orchestration** | Azure Data Factory (ADF) | Pipeline scheduling and data movement |
| **Processing** | Azure Databricks (PySpark) | Distributed data transformation across all layers |
| **Storage** | Azure Data Lake Storage Gen2 (ADLS) | Scalable, cost-effective blob storage |
| **Table Format** | Delta Lake | ACID transactions, time travel, schema evolution |
| **Infrastructure** | Terraform | Infrastructure as Code for all Azure resources |
| **Governance** | Unity Catalog | Data discovery, lineage, and access control |
| **Testing** | pytest + Great Expectations | Unit tests and data quality validation |
| **Version Control** | Git | Source control |

---

## Project Structure

```
Netflix Pipeline/
├── adf/                    # Azure Data Factory pipeline definitions (JSON/ARM)
├── databricks/
│   ├── bronze/             # Ingestion notebooks — raw data landing
│   ├── silver/             # Transformation notebooks — cleansed layer
│   ├── gold/               # Aggregation notebooks — business layer
│   └── governance/         # Unity Catalog setup, access policies, lineage configs
├── infra/                  # Terraform modules for Azure infrastructure
├── data/                   # Sample/seed datasets for local development
├── tests/                  # pytest unit and integration tests
└── README.md
```

---

## Pipeline Overview

1. **Ingest** — ADF triggers pull data from source systems (REST APIs, event hubs, blob uploads) into the Bronze Delta table on ADLS Gen2.
2. **Transform (Bronze → Silver)** — Databricks notebooks apply schema validation, null handling, deduplication, and type casting.
3. **Aggregate (Silver → Gold)** — Business aggregations (e.g., top content by watch time, churn signals, regional engagement) are computed and written to Gold tables.
4. **Govern** — Unity Catalog enforces column-level security, tracks lineage, and provides a searchable data catalog across all layers.
5. **Serve** — Gold tables are consumed by BI dashboards (Power BI / Databricks SQL) and ML feature pipelines.

---

## Getting Started

### Prerequisites
- Azure subscription
- Terraform >= 1.5
- Python >= 3.10
- Databricks CLI

### Provision Infrastructure
```bash
cd infra/
terraform init
terraform plan
terraform apply
```

### Run Tests
```bash
pip install -r requirements.txt
pytest tests/
```

# Netflix Data Engineering Pipeline

An end-to-end data engineering pipeline that ingests the MovieLens 25M dataset, processes it through a Medallion architecture on Azure, and produces business-ready aggregations — orchestrated by Apache Airflow and built entirely with industry-standard tools.

---

## Architecture

```
  GitHub (CSV files)
        │
        │  HTTP ingest
        ▼
  Azure Data Factory
        │
        │  copy activity
        ▼
  ADLS Gen2  ──  landing/  (raw files arrive here)
        │
        │  Databricks reads
        ▼
  ┌─────────────────────────────────────────────┐
  │              Databricks + Delta Lake         │
  │                                             │
  │   Bronze          Silver           Gold     │
  │  (raw copy)  →  (cleaned)  →  (aggregated)  │
  └─────────────────────────────────────────────┘
        │
        │  served to
        ▼
  Databricks SQL / BI Dashboards

  ─────────────────────────────────────────────
  Apache Airflow orchestrates the entire flow
  ─────────────────────────────────────────────
```

---

## Tech Stack

| Tool | Role | Why I chose it |
|------|------|----------------|
| **Apache Airflow** | Orchestration | Industry-standard workflow scheduler. DAGs as code made it easy to define dependencies, add retries, and see exactly what ran and when. |
| **Azure Data Factory** | Ingestion | Managed ingestion service with built-in connectors. Handles the HTTP → ADLS copy without needing a running compute cluster. |
| **ADLS Gen2** | Storage | Azure's data lake storage with hierarchical namespace — needed for the folder-level permissions that Databricks relies on per Medallion layer. |
| **Databricks + PySpark** | Processing | Distributed processing for 25M rows. Databricks runs managed Spark clusters so I could focus on transformation logic rather than cluster setup. |
| **Delta Lake** | Table format | ACID transactions, schema enforcement, and time travel on top of Parquet. The `overwriteSchema` option saved me multiple times during development. |
| **Terraform** | Infrastructure as Code | Reproducible Azure infrastructure in version-controlled `.tf` files. One `terraform apply` provisions the resource group, storage account, containers, ADF, and RBAC roles. |

---

## Dataset

**MovieLens 25M** — published by the GroupLens research lab at the University of Minnesota.

| File | Rows | Description |
|------|------|-------------|
| `ratings.csv` | 25,000,095 | User → movie ratings (0.5–5.0 stars) |
| `movies.csv` | 62,423 | Movie titles and pipe-separated genres |
| `tags.csv` | 1,093,360 | Free-text user-applied tags |
| `links.csv` | 62,423 | IMDb and TMDB IDs for each movie |
| `genome-scores.csv` | 15,584,448 | ML-computed tag relevance scores per movie |
| `genome-tags.csv` | 1,128 | Tag ID → human-readable label |

The pipeline focuses on `ratings.csv` and `movies.csv` as the core fact and dimension tables.

---

## Medallion Architecture

### Bronze — Raw Layer
Data lands exactly as received. No transformations, no type changes. Stored as Delta tables in `workspace.bronze`.

The Bronze layer is an immutable historical record — if a bug is introduced in Silver or Gold, you can always reprocess from Bronze without going back to the source.

### Silver — Cleaned Layer
PySpark transformations applied in `nb_silver_transform.ipynb`:

- `timestamp` (unix integer) → `rated_at` (TimestampType) via `from_unixtime`
- `genres` pipe-string → `ArrayType` e.g. `"Action|Comedy"` → `["Action", "Comedy"]`
- `release_year` extracted from title using a Python UDF (used a UDF because Spark's Catalyst optimizer evaluates `.cast("int")` across all rows before a `when()` condition can filter empty strings — causing `CAST_INVALID_INPUT` with a plain cast)
- `title` cleaned: `"Toy Story (1995)"` → `"Toy Story"`
- `ingestion_date` added as a pipeline audit column

### Gold — Business Layer
Four aggregation tables built in `nb_gold_aggregate.ipynb`:

| Table | Description | Rows |
|-------|-------------|------|
| `gold_top_movies` | Best-rated movies with > 100 ratings | **10,291** |
| `gold_genre_popularity` | Avg rating and total ratings per genre | **20 genres** |
| `gold_user_activity` | Rating count and avg per active user (> 10 ratings) | **162,541 users** |
| `gold_ratings_by_year` | Movies and avg rating per release year | **135 years** |

---

## How to Run Locally

### Prerequisites

- Docker Desktop
- Python 3.10+
- Azure CLI (for infrastructure)
- Terraform >= 1.3.0
- A Databricks workspace (free trial works)

### 1. Clone the repo

```bash
git clone <repo-url>
cd "Netflix Pipeline"
```

### 2. Set up environment variables

```bash
cp .env.example .env
# Fill in your Azure credentials in .env
```

### 3. Provision Azure infrastructure

```bash
cd infra
terraform init
terraform plan
terraform apply
```

This creates the resource group, ADLS Gen2 storage account, four containers (landing/bronze/silver/gold), and Azure Data Factory with its managed identity RBAC role.

### 4. Start Airflow locally

```bash
docker compose up airflow-init   # first time only — creates DB and admin user
docker compose up -d             # start scheduler + webserver
```

Open `http://localhost:8080` and log in with `admin / admin`.

### 5. Run the Databricks notebooks in order

Import the notebooks from `databricks/` into your Databricks workspace:

```
1. databricks/bronze/nb_bronze_ingest.ipynb
2. databricks/silver/nb_silver_transform.ipynb
3. databricks/gold/nb_gold_aggregate.ipynb
```

Run them top to bottom. Each notebook prints row counts and sample rows at the end so you can confirm each layer before moving to the next.

---

## Project Structure

```
Netflix Pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── netflix_pipeline_dag.py   # Airflow DAG — 4 tasks, daily at 02:00 UTC
│   └── requirements.txt
│
├── databricks/
│   ├── bronze/
│   │   └── nb_bronze_ingest.ipynb    # Reads workspace.default → saves to bronze
│   ├── silver/
│   │   └── nb_silver_transform.ipynb # Type fixes, genre split, year extraction
│   ├── gold/
│   │   └── nb_gold_aggregate.ipynb   # 4 business aggregation tables
│   └── governance/                   # Unity Catalog config (placeholder)
│
├── infra/
│   ├── main.tf                       # Resource group, ADLS, ADF, RBAC
│   ├── providers.tf                  # azurerm ~> 3.85
│   ├── variables.tf                  # Input variables with defaults
│   ├── outputs.tf                    # ADLS endpoint, ADF name + principal ID
│   └── terraform.tfvars              # Actual values — gitignored
│
├── adf/                              # ADF pipeline JSON definitions
├── data/                             # MovieLens CSV files (local dev)
├── tests/                            # Placeholder for pytest tests
│
├── docker-compose.yml                # Local Airflow stack (Postgres + LocalExecutor)
├── .env.example                      # Environment variable template
└── README.md
```

---

## What I Learned

**Delta Lake behaviour in practice** — `overwriteSchema = true` is needed whenever you change a column type between runs during development. Without it Delta rejects the write even in overwrite mode.

**Spark's Catalyst optimizer** — I kept getting `CAST_INVALID_INPUT` when trying to cast an empty string to `IntegerType` even with a `when()` guard. The issue is that Catalyst evaluates the cast expression across all rows at the physical plan level before the condition filters them. The fix was a Python UDF, which is a black box to the optimizer and runs per-row in Python where I can return `None` before any cast happens.

**Airflow LocalExecutor vs CeleryExecutor** — For a single-machine setup, LocalExecutor runs tasks as subprocesses and is much simpler to operate. CeleryExecutor adds Redis and worker containers — worth it for a production cluster, overkill for local development.

**Terraform state** — `terraform.tfstate` is the source of truth for what's deployed. Learned this the hard way when I manually deleted a resource in the portal and Terraform's plan became out of sync. The fix is to either import the resource back (`terraform import`) or let Terraform recreate it.

**Unity Catalog three-part naming** — Databricks Unity Catalog uses `catalog.schema.table` naming (e.g. `workspace.bronze.bronze_ratings`). Once you understand this, `spark.table()` is far cleaner than file paths and removes the need for DBFS mounts entirely.

**`skip_provider_registration = true`** — Without this, the Terraform azurerm provider tries to register every Azure resource provider in your subscription, which requires elevated permissions. Setting it to `true` means you register only what you need, manually — better security and faster applies.

---

## Author

Built by a recent data engineering graduate as a hands-on learning project.
The goal was to understand how a real pipeline fits together end-to-end — not just run tutorials, but make design decisions, hit real errors, and debug them.
