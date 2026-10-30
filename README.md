# Databricks End-to-End Data Engineering Pipeline

An end-to-end lakehouse on **Azure Databricks** for a fictional restaurant chain ("Spice Route", 5 outlets across the UAE). It ingests **real-time orders** from Azure Event Hubs and **operational data** from Azure SQL Database (with CDC), refines it through a **Medallion architecture** (Bronze → Silver → Gold) using **Spark Declarative Pipelines**, enriches customer reviews with **LLM-based sentiment analysis**, and serves the results to Databricks SQL dashboards. Everything is governed by **Unity Catalog**.

## Tech Stack

| Area | Tools |
|---|---|
| Streaming ingestion | Azure Event Hubs (via Kafka protocol), Spark Structured Streaming |
| Batch / CDC ingestion | Azure SQL Database, Lakeflow Connect (Change Tracking) |
| Transformation | Spark Declarative Pipelines (PySpark + SQL), Delta Lake |
| AI enrichment | Databricks `ai_query()` with Mosaic AI model serving (`databricks-gpt-oss-20b`) |
| Governance | Unity Catalog |
| Serving | Databricks SQL dashboards |
| Data generation | Python, Faker, pandas |

## How It Works

### 1. Source data (synthetic)
Python scripts in [`00_synthetic_data/`](projects/databricks-e2e-project/00_synthetic_data/) generate realistic data:
- **Master data** - restaurants, ~500 customers, ~145 menu items
- **Historical orders** - ~8,000 completed orders, loaded into Azure SQL Database
- **Customer reviews** - templated review text matched to ratings
- **Live orders** - a producer that streams a new JSON order to Event Hubs every few seconds

### 2. Ingestion
- **Streaming:** [`pipeline_ingest_eventhub.py`](projects/databricks-e2e-project/01_pipelines/pipeline_ingest_eventhub.py) reads Event Hubs through its Kafka-compatible endpoint (SASL_SSL), parses JSON payloads against an explicit schema, and lands them in `01_bronze.orders`.
- **CDC:** Lakeflow Connect runs a continuous ingestion gateway against Azure SQL Database with **Change Tracking** enabled, so inserts/updates in the source (e.g. a customer's city changes) flow into Bronze automatically.

### 3. Silver - cleaned, validated facts
| Table | What it does |
|---|---|
| `fact_orders` | Casts timestamps, derives `order_date`, `order_hour`, `day_of_week`, `is_weekend`, `item_count` |
| `fact_order_items` | Explodes the nested `items` JSON array into one row per item |
| `fact_reviews` | Calls an LLM per review with `ai_query()` to extract **sentiment** and flag issues (**delivery, food quality, pricing, portion size**) with reasons |

Data quality is enforced with **pipeline expectations** (`@dp.expect_all_or_drop` / `CONSTRAINT ... EXPECT`): null keys, non-positive amounts, and invalid status/payment/sentiment values are dropped and tracked in the pipeline event log.

### 4. Gold - business-ready materialized views
| View | Purpose |
|---|---|
| `d_sales_summary` | Daily revenue, orders, AOV, unique customers, and order-type split (partitioned by `order_date`) |
| `d_customer_360` | Per-customer lifetime spend, AOV, favourite restaurant & item (window functions), review stats, loyalty tier (Bronze → Platinum), VIP flag |
| `d_restaurant_reviews` | Per-restaurant rating distribution and sentiment counts |

### 5. Dashboards
Two Databricks SQL dashboards are built on the Gold layer (see [`dashboard_metrics.md`](projects/databricks-e2e-project/dashboard_metrics.md)):
- **Restaurant Performance** - revenue, AOV, best sellers, peak-hour heatmap, revenue by order type and category
- **Review Insights** - sentiment trend, rating distribution, issue categorisation, recent review feed

## Key Technical Highlights
- **Unified batch + streaming** in one declarative pipeline - Silver tables are streaming tables, Gold tables are materialized views that refresh incrementally.
- **Change Data Capture** from an OLTP database without writing custom CDC logic.
- **LLM inference inside SQL** - unstructured review text becomes structured, queryable columns.
- **Declarative data quality** with expectations instead of hand-written validation code.
- **Unity Catalog** three-level namespace (`catalog.01_bronze / 02_silver / 03_gold`) for governance and lineage.

## Repository Structure
```
├── projects/databricks-e2e-project/
│   ├── 00_synthetic_data/        # Data generators, Azure SQL setup scripts, Event Hub producer
│   ├── 01_pipelines/
│   │   ├── pipeline_ingest_eventhub.py      # Event Hubs → Bronze
│   │   └── pipeline_bronze_to_gold/         # Silver & Gold transformations
│   ├── diagrams/                 # Architecture diagrams
│   ├── dashboard_metrics.md      # Dashboard KPI definitions
│   └── commands_used.md          # Databricks CLI & SQL commands used
└── delta_lake/                   # Notebooks covering Delta Lake fundamentals
```

The [`delta_lake/`](delta_lake/) notebooks are hands-on companions covering the Delta Lake features used in the project: DML, time travel, schema validation & evolution, deletion vectors, shallow/deep clones, `OPTIMIZE`/`VACUUM`, Z-Ordering and Liquid Clustering.

## Getting Started

**Prerequisites:** an Azure subscription with Azure Databricks (Unity Catalog enabled), Azure SQL Database, and Azure Event Hubs.

1. **Generate data**
   ```bash
   cd projects/databricks-e2e-project/00_synthetic_data
   pip install -r requirements.txt python-dotenv
   python 03_run.py
   ```
2. **Set up Azure SQL** - run [`sql/azuresqldatabase_setup.sql`](projects/databricks-e2e-project/00_synthetic_data/sql/azuresqldatabase_setup.sql), load the generated CSVs, then run `utility_script.sql` to enable Change Tracking.
3. **Configure Lakeflow Connect** - create a SQL Server connection and ingestion pipeline into the `01_bronze` schema.
4. **Create the pipelines** in Databricks from `01_pipelines/`, setting `eh.namespace`, `eh.name` and `eh.connectionString` in the pipeline configuration (ideally from a secret scope).
5. **Stream live orders**
   ```bash
   # from the repo root: add your Event Hub connection string and name
   cp .env.example .env
   python projects/databricks-e2e-project/00_synthetic_data/04_eventhub_orders.py
   ```
6. **Build the dashboards** on the `03_gold` tables.
