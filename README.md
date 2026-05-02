# DataSentry - End-to-End ELT Data Quality Pipeline

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=delta&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)

---

## Overview

DataSentry is an end-to-end ELT data quality pipeline for financial data, built on Databricks Free Edition. It processes 307,500+ records across a Bronze/Silver/Gold medallion architecture, applying 41 rule-based validation checks across 5 data quality dimensions to achieve a 98.9% pipeline pass rate.

The pipeline is fully orchestrated via Databricks Lakeflow Jobs with File Arrival triggering, and visualised through a two-page Power BI dashboard covering pipeline health and exception reporting.

---

## Architecture

```
Raw CSV Files (Unity Catalog Volume)
            |
            v
    [ Bronze Layer ]
    Notebook 02 - Raw ingestion, no transformation
    Idempotent ingestion + row-level reconciliation
    Audit log: bronze_ingestion_log
            |
            v
    [ Silver Layer ]
    Notebook 03 - 41 DQ rules across 5 dimensions
    Pass/fail flag per record + failed_rules tagging
    Severity classification: HIGH / MEDIUM / LOW
            |
            v
    [ Gold Layer ]
    Notebook 04 - DQ scorecard (rule / dimension / table level)
    Notebook 05 - Unified exception log
            |
            v
    [ Power BI Dashboard ]
    Page 1 - Pipeline Health Overview
    Page 2 - Exception Deep Dive
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Databricks Free Edition | Notebooks, ELT, Delta tables, Unity Catalog, Volumes |
| PySpark | Data processing and DQ rule engine |
| Delta Lake | Bronze, Silver, Gold table storage |
| Unity Catalog | Data governance, Volumes, table management |
| Lakeflow Jobs | Pipeline orchestration with File Arrival triggering |
| Power BI Desktop | Dashboard and exception report |
| Python / Faker | Synthetic financial data generation |
| GitHub | Version control |

---

## Data Quality Dimensions

41 rules applied across 5 dimensions:

| Dimension | Rules | Description |
|---|---|---|
| Completeness | 15 | Required fields must not be null |
| Validity | 13 | Values must match correct format or range |
| Consistency | 5 | Values must be logically correct across fields |
| Uniqueness | 3 | No duplicate primary keys |
| Referential Integrity | 3 | FK relationships must be valid across tables |

---

## Pipeline Results

| Table | Records | Pass Rate | Status |
|---|---|---|---|
| customers | 50,500 | 99.67% | GREEN |
| accounts | 55,000 | 99.62% | GREEN |
| transactions | 202,000 | 98.60% | AMBER |
| **Overall** | **307,500** | **98.9%** | **AMBER** |

### Key Findings

- `failed_reversed_amount_not_positive` - 18,024 FAILED and REVERSED transactions retain positive amounts, indicating the upstream system does not roll back transaction amounts correctly. This is a systemic business logic issue surfaced by the pipeline.
- `account_id_exists_in_accounts` - 11,920 transactions reference account IDs that do not exist in the accounts table. Orphaned foreign keys cascading from upstream.
- `customer_id_exists_in_customers` - 2,720 accounts reference customer IDs that do not exist in the customers table.

---

## Notebooks

| Notebook | Purpose |
|---|---|
| `00_reset` | Maintenance utility - resets pipeline by dropping Delta tables selectively. Does not affect the Volume or raw CSV files. Run selectively per layer. |
| `01_data_generation` | Data simulator - generates 307,500 synthetic financial records using Faker with intentional dirt (nulls, duplicates, invalid formats, orphaned FKs). In production this is replaced by real data arriving from an upstream source system. |
| `02_bronze_ingestion` | Idempotent ingestion with reconciliation - reads CSVs from Volume, checks ingestion log to skip already-processed files, compares source vs ingested row counts, appends to Bronze Delta tables. |
| `03_silver_validation` | Applies 41 DQ rules across 5 dimensions. Tags each record PASS or FAIL with a failed_rules column listing exact rule violations. |
| `04_gold_scoring` | Computes DQ scorecard at rule, dimension, and table level. Assigns GREEN/AMBER/RED health status per rule based on pass rate thresholds. |
| `05_exception_reporting` | Extracts all failed records from Silver tables and writes a unified exception log with HIGH/MEDIUM/LOW severity classification. |

> **Note:** Notebook 01 is not included in the Lakeflow Job. It is a development utility for generating synthetic data. In a production environment, real data files would arrive directly into the Unity Catalog Volume from an upstream source system.

---

## Idempotent Ingestion

Bronze ingestion is fully idempotent. Every ingested file is recorded in `bronze_ingestion_log` with its file path, row counts, reconciliation status, and timestamp. On each pipeline run, the notebook checks this log and skips files that have already been processed - preventing duplicate data regardless of how many times the pipeline runs.

---

## Reconciliation

After each file ingestion, the Bronze notebook compares source CSV row counts against ingested Delta table row counts. A MATCH confirms clean ingestion. A MISMATCH raises a WARNING in the ingestion log. This provides full source-to-Bronze traceability on every run.

---

## Orchestration

The pipeline is orchestrated via a Databricks Lakeflow Job with 4 tasks running in sequence:

```
bronze_ingestion → silver_validation → gold_scoring → exception_reporting
```

Each task uses `Run if dependencies: All succeeded` - if any task fails, downstream tasks are automatically skipped.

### File Arrival Trigger

The pipeline is configured with a File Arrival trigger monitoring the Unity Catalog Volume at:
```
/Volumes/workspace/banking_datasentry/datasentry_files/
```

When a new CSV file lands in this path, the pipeline fires automatically - no manual intervention required.

### Dynamic File Discovery

Bronze ingestion uses dynamic file discovery. Rather than hardcoding file names, the notebook scans the Volume at runtime and picks up any CSV file matching the expected pattern. Dropping a new file into the Volume is sufficient to have it ingested automatically on the next run.

---

## Recreating the Pipeline

The full job configuration is defined in `job_config/datasentry_job_config.json`. To recreate the pipeline in your own Databricks workspace:

**Via Databricks CLI:**
```bash
databricks jobs create --json @datasentry_job_config.json
```

**Via REST API:**
```bash
curl -X POST https://<your-databricks-instance>/api/2.1/jobs/create \
  -H "Authorization: Bearer <your-token>" \
  -d @datasentry_job_config.json
```

> Update the notebook paths in the JSON file to match your own Databricks workspace username before importing.

---

## Repository Structure

```
datasentry/
├── notebooks/
│   ├── 00_reset.sql
│   ├── 01_data_generation.ipynb
│   ├── 02_bronze_ingestion.ipynb
│   ├── 03_silver_validation.ipynb
│   ├── 04_gold_scoring.ipynb
│   └── 05_exception_reporting.ipynb
├── sample_data/
│   ├── customers_sample.csv        (1,000 rows)
│   ├── accounts_sample.csv         (1,000 rows)
│   └── transactions_sample.csv     (1,000 rows)
├── job_config/
│   └── datasentry_job_config.json
├── dashboard/
│   └── DataSentry_DQ_Dashboard.pbix
└── README.md
```

> Sample data (1,000 rows per table) is included for reference. The full dataset (50,500 customers, 55,000 accounts, 202,000 transactions) is generated by Notebook 01 and stored in the Databricks Unity Catalog Volume.

---

## Gold Tables (Power BI Data Sources)

| Table | Description |
|---|---|
| `gold_rule_scores` | Pass rate per rule per table - 41 rows |
| `gold_dimension_scores` | Pass rate aggregated by dimension per table |
| `gold_table_scores` | Overall pass rate per table - 3 rows |
| `gold_exception_log` | Full exception detail with severity classification |
| `gold_top10_failing_rules` | Pre-aggregated top 10 failing rules for Power BI |

---

## Power BI Dashboard

**Page 1 - Pipeline Health Overview**
- Overall pass rate KPI (98.9%)
- Table health status with GREEN/AMBER/RED conditional formatting
- Pass rate by dimension (clustered bar chart)
- Rule health status distribution (donut chart)

**Page 2 - Exception Deep Dive**
- Total exceptions and severity breakdown KPI cards (46,590 total - 21,427 HIGH, 25,163 MEDIUM)
- Top 10 failing rules (bar chart)
- Exceptions by table and severity (matrix)
- Full exception log with rule tags, dimensions, and severity (drillable table)

---

## Health Status Thresholds

| Status | Pass Rate |
|---|---|
| GREEN | >= 99% |
| AMBER | >= 95% |
| RED | < 95% |

---

## Author

Kamran Habib
Data Analyst / Data Engineer
[LinkedIn](https://www.linkedin.com/in/kamranhabib) | [GitHub](https://github.com/kamranhabib1212)
