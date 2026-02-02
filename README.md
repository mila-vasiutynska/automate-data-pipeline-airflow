# Sparkify Data Pipeline (Airflow + Redshift)

An Apache Airflow DAG that loads the **Sparkify** song-play analytics data from **S3** into **Amazon Redshift**.  
The pipeline stages raw JSON files into Redshift staging tables, builds a star schema (fact + dimensions), and runs data quality checks.

---

## What this project does

This project orchestrates an ETL pipeline using Airflow:

1. **Drops tables** (if they exist)
2. **Creates tables** (staging + analytics schema)
3. **Stages data from S3 → Redshift**
   - Event logs → `staging_events`
   - Song metadata → `staging_songs`
4. **Loads analytics tables**
   - Fact: `songplays`
   - Dimensions: `users`, `songs`, `artists`, `time`
5. **Runs data quality checks** to validate the load

The DAG is scheduled to run **hourly** (`@hourly`) and does **not** backfill (`catchup=False`).

---

## DAG overview

**DAG name:** `sparkify_data_pipeline`

### Tasks

- `Begin_execution` (dummy start)
- `Drop_tables` (runs `drop_tables.sql`)
- `Create_tables` (runs `create_tables.sql`)
- `Stage_events` (S3 → Redshift `staging_events`)
- `Stage_songs` (S3 → Redshift `staging_songs`)
- `Load_songplays_fact_table` (fact load)
- `Load_user_dim_table` (dimension load)
- `Load_song_dim_table` (dimension load)
- `Load_artist_dim_table` (dimension load)
- `Load_time_dim_table` (dimension load)
- `Run_data_quality_checks`
- `Stop_execution` (dummy end)

### Dependencies (high level)

- Start → Drop → Create
- Create → Stage Events + Stage Songs
- Stage Songs → Load Songs + Load Artists
- Stage Events → Load Users
- Stage Events + Stage Songs → Load Songplays Fact
- Songplays Fact → Load Time
- All loads → Data Quality → Stop

---

## Data sources

- **S3 bucket & prefix are Airflow Variables**
  - `s3_bucket`
  - `s3_prefix`

### Expected S3 layout

- Events: `s3://<s3_bucket>/<s3_prefix>/log-data/`
- Songs:  `s3://<s3_bucket>/<s3_prefix>/song-data/`
- JSON paths file for events:
  - `s3://<s3_bucket>/<s3_prefix>/log_json_path.json`

---

## Redshift schema

### Staging tables
- `staging_events`
- `staging_songs`

### Analytics tables (star schema)
- Fact: `songplays`
- Dimensions: `users`, `songs`, `artists`, `time`

SQL insert statements are referenced from:
- `common.final_project_sql_statements.SqlQueries`

---

## Custom Airflow operators

This DAG uses custom operators located under `final_project_operators/`:

- `StageToRedshiftOperator`
  - Copies JSON from S3 into Redshift (COPY command)
- `LoadFactOperator`
  - Loads the fact table (`songplays`)
- `LoadDimensionOperator`
  - Loads dimension tables with optional `truncate_before_insert`
- `DataQualityOperator`
  - Runs configurable checks defined as SQL + expected results

---

## Data quality checks

The DAG runs these checks (examples):

- `songplays` must contain rows  
  `SELECT COUNT(*) FROM songplays` should be `> 0`
- no null `userid` in `users`  
  `SELECT COUNT(*) FROM users WHERE userid IS NULL` should be `0`
- `songs` and `artists` must contain rows  
  `SELECT COUNT(*) FROM songs` should be `> 0`  
  `SELECT COUNT(*) FROM artists` should be `> 0`

> Note: each test supports `comparison` (default `==`), e.g. `">"`.

---

## Setup

### 1) Airflow Connections

Create these connections in the Airflow UI (Admin → Connections):

- `aws_credentials`
  - AWS access key + secret (or configured via environment/roles)
- `redshift`
  - Redshift/Postgres connection info (host, schema/db, user, password, port)

### 2) Airflow Variables

Set these variables in Airflow (Admin → Variables):

- `s3_bucket` → your S3 bucket name
- `s3_prefix` → folder/prefix inside the bucket (e.g. `data-pipelines`)

### 3) SQL files

Make sure these SQL scripts exist and are accessible to the DAG:

- `create_tables.sql`
- `drop_tables.sql`

(They are executed via `PostgresOperator` using `postgres_conn_id="redshift"`.)

---

## How to run

1. Start Airflow (scheduler + webserver)
2. Enable the DAG: `sparkify_data_pipeline`
3. Trigger manually or let it run hourly

You can monitor progress in the Airflow UI and view task logs for debugging.

---

## Notes / design choices

- Retries: **3** retries, **5 minutes** apart.
- `catchup=False` avoids backfilling from `start_date`.
- Dimensions are loaded with `truncate_before_insert=True` (full refresh behavior).
- The `time` dimension load is dependent on `songplays` (matching the Udacity project logic).

---

## Project structure (suggested)

```text
.
├── dags/
│   └── sparkify_data_pipeline.py
├── final_project_operators/
│   ├── stage_redshift.py
│   ├── load_fact.py
│   ├── load_dimension.py
│   └── data_quality.py
├── udacity/
│   └── common/
│       └── final_project_sql_statements.py
├── create_tables.sql
└── drop_tables.sql

```

## Troubleshooting

Staging tasks fail (COPY errors):
 - confirm aws_credentials connection
 - confirm S3 paths + region
 - confirm Redshift IAM permissions to read S3

Data quality fails:
 - check upstream task logs (staging + loads)
 - verify your create_tables.sql matches the expected schema

Airflow Variables missing:
 - set s3_bucket and s3_prefix in the Airflow UI
