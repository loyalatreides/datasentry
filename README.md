# DataSentry: End-to-End ELT Data Quality Pipeline

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=delta&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)

---

## Overview

DataSentry is an end-to-end ELT data quality pipeline for financial data, built on Databricks Free Edition. It runs in two phases:

**Phase 1: Detection.** Raw files move through a Bronze/Silver/Gold medallion architecture. Silver applies 43 rule checks (38 distinct rules) across 5 data quality dimensions and tags every record PASS or FAIL with the exact rules it broke. Gold turns this into a scorecard and a severity-ranked exception log.

**Phase 2: Remediation.** Every failed record is routed to one of three outcomes: auto-fixed, quarantined for a source system fix, or escalated for human review. Fixed records are written to separate remediated tables, so Silver stays an untouched record of what arrived.

Current state (two batches, 355,700 records, 5,500,600 rule checks per run):

| Metric | Value |
|---|---|
| Check-level pass rate (Gold) | 98.39% |
| Record-level pass rate before remediation | 79.27% |
| Record-level pass rate after remediation | 81.74% (+2.47 pp) |
| Failed records routed | 73,735 (100% of failures) |

The pipeline is orchestrated with Databricks Lakeflow Jobs and a File Arrival trigger, and reported through a three-page Power BI dashboard.

---

## Architecture

```
Raw CSV Files (Unity Catalog Volume)
            |
            v
    [ Bronze Layer ]
    Notebook 02: raw ingestion, no transformation
    Idempotent ingestion + reconciliation against landed rows
    Audit log: bronze_ingestion_log
            |
            v
    [ Silver Layer ]
    Notebook 03: 43 rule checks across 5 dimensions
    PASS/FAIL per record + failed_rules tagging
            |
            v
    [ Remediation ]
    Notebook 06: AUTO_FIX / QUARANTINE / ESCALATE routing
    Writes silver_remediated_* and remediation logs
            |
            v
    [ Gold Layer ]
    Notebook 04: DQ scorecard (rule / dimension / table level)
    Notebook 05: unified exception log with severity
            |
            v
    [ Power BI Dashboard ]
    Page 1: Pipeline Health Overview
    Page 2: Exception Deep Dive
    Page 3: Remediation Before vs After
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
| Power BI Desktop | Dashboard, exception report and remediation view |
| Python / Faker | Synthetic financial data generation |
| GitHub | Version control |

---

## Data Quality Rules

43 rule checks across 3 tables (customers 12, accounts 14, transactions 17). Some rules, such as `currency_valid`, run on more than one table, giving 38 distinct rule names.

| Dimension | Checks | Description |
|---|---|---|
| Completeness | 15 | Required fields must not be null |
| Validity | 16 | Values must match the correct format, type or range |
| Consistency | 7 | Values must be logically correct across fields |
| Uniqueness | 3 | No duplicate primary keys |
| Referential Integrity | 2 | Foreign keys must exist in the parent table |

### Severity

| Severity | Rule |
|---|---|
| HIGH | Record fails a Referential Integrity or Uniqueness rule |
| MEDIUM | Record fails a Validity or Consistency rule |
| LOW | Record fails Completeness rules only |

### Health Status Thresholds

| Status | Pass Rate |
|---|---|
| GREEN | >= 99% |
| AMBER | >= 95% |
| RED | < 95% |

---

## Phase 2: Remediation

Each failed rule is mapped to an action. A record that fails several rules takes the strictest one, in the order **ESCALATE > QUARANTINE > AUTO_FIX**.

| Action | When | Examples |
|---|---|---|
| AUTO_FIX | The correct value can be derived safely | Duplicate primary keys (keep the earliest ingested copy), country names mapped to ISO codes (IRELAND to IE, UK to GB) |
| QUARANTINE | The value is wrong and must be corrected at source | Invalid currency, non-numeric or zero amounts, future dates, invalid account or transaction types |
| ESCALATE | A person needs to decide | Missing names or emails, invalid email format, orphaned foreign keys, FAILED or REVERSED transactions with positive amounts |

Design choices:
- **Silver is immutable.** Fixed records go to `silver_remediated_customers`, `silver_remediated_accounts` and `silver_remediated_transactions`.
- **Fixes are re-checked.** After country standardisation, any value that is still not a valid two-letter code (for example "Irish" or "England") is moved to escalation instead of being written as fixed.
- **Honest denominator.** The "after" pass rate excludes dropped duplicates, so removing rows cannot inflate the score.
- **Two metrics, two meanings.** The check-level pass rate (Gold) counts individual rule checks. The record-level pass rate (Remediation) counts a record as passing only if every rule passes, which is why it is lower.

### Remediation Results

| Table | Records | Failed | Auto-fixed | Duplicates dropped | Quarantined | Escalated | Before | After |
|---|---|---|---|---|---|---|---|---|
| customers | 66,900 | 8,093 | 2,867 | 1,809 | 0 | 3,417 | 87.90% | 94.75% |
| accounts | 70,600 | 12,214 | 416 | 416 | 3,497 | 7,885 | 82.70% | 83.78% |
| transactions | 218,200 | 53,428 | 2,027 | 2,032 | 6,192 | 43,177 | 75.51% | 77.16% |
| **Overall** | **355,700** | **73,735** | **5,310** | **4,257** | **9,689** | **54,479** | **79.27%** | **81.74%** |

Most failures are escalated rather than fixed. That is deliberate: the pipeline only changes data where the correct value is certain, and routes everything else to the people who own it.

---

## Pipeline Results (Detection View)

| Table | Records | Check-level Pass Rate | Status |
|---|---|---|---|
| customers | 66,900 | 98.93% | AMBER |
| accounts | 70,600 | 98.39% | AMBER |
| transactions | 218,200 | 98.28% | AMBER |
| **Overall** | **355,700** | **98.39%** | **AMBER** |

88,293 failed checks out of 5,500,600. Rule health: 29 GREEN, 10 AMBER, 4 RED.

### Key Findings

- `account_id_exists_in_accounts`: 23,713 transactions point to accounts that are missing or failed validation. One bad account cascades to every transaction linked to it, which makes this the largest source of exceptions.
- `failed_reversed_amount_not_positive`: 22,277 FAILED or REVERSED transactions keep their original positive amount. Whether this is an error depends on the source system's convention (many systems keep the original amount and record the reversal separately), so these are escalated for business review rather than corrected automatically.
- `customer_id_exists_in_customers`: 7,885 accounts reference customers that are missing or failed validation.
- `customer_id_unique`: 3,783 duplicate customer records (94.35% pass rate, RED).

---

## Dirty Batch Test

To prove the pipeline handles new bad data correctly, a second batch was generated with known, counted errors (one error type per row) and dropped into the Volume. The File Arrival trigger started the job automatically.

| Injected | Count | Result |
|---|---|---|
| New files (11,000 customers, 10,600 accounts, 10,800 transactions) | 3 files | Reconciliation MATCH on all 3 |
| Non-numeric account balances ("N/A", "1,250.00") | 482 | `balance_valid` caught exactly 482 |
| Non-numeric transaction amounts | 499 | `amount_valid` caught exactly 499 |
| Country names with no ISO mapping ("Irish", "England") | 311 | Re-check escalated exactly 311 |
| Zero-amount transactions | 280 | `amount_greater_than_zero` rose by 269 (the rest are FAILED or REVERSED, which are exempt) |
| Future-dated transactions | 278 | `transaction_date_not_future` caught 278 |
| Mapped country variants ("IRELAND", "USA") | 1,187 | Auto-fixed |
| Duplicate rows | 2,400 | Caught by uniqueness rules, deduplicated where safe |

### Before vs After the Dirty Batch

| Metric | Baseline (batch 1) | After batch 2 |
|---|---|---|
| Records | 323,300 | 355,700 |
| Check-level pass rate | 98.64% | 98.39% |
| Failed checks | 68,388 | 88,293 |
| Record-level pass rate (before → after remediation) | 82.40% → 83.85% | 79.27% → 81.74% |
| Remediation uplift | +1.45 pp | +2.47 pp |

The uplift is larger after batch 2 because most of the injected customer errors were duplicates and country spellings, which are auto-fixable. Customers moved from GREEN to AMBER in Gold.

---

## Hardening Changes

Issues found while reviewing and testing the pipeline, and how they were fixed:

| Area | Problem | Fix |
|---|---|---|
| Reconciliation | Compared the CSV count with the DataFrame count, which always matched | Now counts rows that actually landed in the Delta table, filtered by `source_file` and `ingested_at` |
| Type casting | A malformed timestamp crashed the whole job (`CANNOT_PARSE_TIMESTAMP`) | `try_cast`, `try_to_date` and `try_to_timestamp` turn bad values into NULL so rules catch them |
| Silent failures | Non-numeric balances and amounts became NULL and passed | Added `balance_valid` and `amount_valid` Validity rules |
| Rule conflict | `amount_greater_than_zero` and `failed_reversed_amount_not_positive` could not both pass for FAILED or REVERSED rows | `amount_greater_than_zero` now exempts FAILED and REVERSED |
| Schema changes | Adding rule columns broke Silver writes (`DELTA_METADATA_MISMATCH`) | Silver and remediation writes use `overwriteSchema` |
| Auto-fix trust | Unmapped country names were written as fixed | Post-fix re-check escalates anything still invalid |
| Metric inflation | Dropping duplicates raised the pass rate on its own | Dropped duplicates are excluded from the denominator |

### Known Limitations

- `transaction_date_not_future` is relative to the run date, so a record dated "in the future" will pass once that date has passed.
- `country_code_valid` checks the two-letter format, so placeholder codes such as XX would pass.
- Dropped duplicates are counted but not yet written to their own log.
- The Gold exception log shows every Silver failure, including records that remediation later auto-fixes.

---

## Notebooks

| Notebook | Purpose |
|---|---|
| `00_reset` | Maintenance utility. Drops Delta tables per layer (Bronze, Silver, Gold, Phase 2). Does not touch the Volume or raw CSV files. |
| `01_data_generation` | Data simulator. Generates synthetic financial records using Faker with intentional dirt (nulls, duplicates, invalid formats, orphaned FKs). In production this is replaced by real data arriving from an upstream system. |
| `02_bronze_ingestion` | Idempotent ingestion with reconciliation. Discovers CSVs by prefix, skips files already in the ingestion log, appends to Bronze and reconciles source rows against landed rows. |
| `03_silver_validation` | Casts types safely and applies 43 rule checks. Tags each record PASS or FAIL with a `failed_rules` column. |
| `04_gold_scoring` | Scores full Silver at rule, dimension and table level and assigns GREEN/AMBER/RED health status. |
| `05_exception_reporting` | Writes a unified exception log of every failed record with HIGH/MEDIUM/LOW severity. |
| `06_remediation` | Routes failed records to AUTO_FIX, QUARANTINE or ESCALATE, applies safe fixes, re-checks them and writes before/after statistics. |

> Notebook 01 is not part of the Lakeflow Job. It is a development utility for generating synthetic data.

---

## Idempotent Ingestion

Every ingested file is recorded in `bronze_ingestion_log` with its file path, row counts, reconciliation status and timestamp. Each run checks this log and skips files already processed, so running the pipeline again never duplicates data.

## Reconciliation

After each file is ingested, the source CSV row count is compared with the number of rows that actually landed in the Bronze Delta table for that file and run. A MATCH confirms clean ingestion. A MISMATCH is recorded as a WARNING in the ingestion log.

---

## Orchestration

A Databricks Lakeflow Job runs 5 tasks in sequence:

```
bronze_ingestion → silver_validation → remediation → gold_task → exception_reporting
```

Each task uses `Run if dependencies: All succeeded`, so if a task fails, the tasks after it are skipped.

### File Arrival Trigger

The job watches the Unity Catalog Volume:
```
/Volumes/workspace/banking_datasentry/datasentry_files/
```
When a new CSV lands there, the pipeline starts automatically.

### Dynamic File Discovery

File names are not hardcoded. Bronze scans the Volume at runtime and picks up any CSV whose name starts with `customers`, `accounts` or `transactions`, so a file such as `customers_batch2.csv` is ingested with no code change.

---

## Recreating the Pipeline

The job configuration is in `datasentry_job_config.json`. To recreate it in your own workspace:

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

> Update the notebook paths in the JSON file to match your own workspace username before importing.

---

## Repository Structure

```
datasentry/
├── notebooks/
│   ├── 00_reset.ipynb
│   ├── 01_data_generation.ipynb
│   ├── 02_bronze_ingestion.ipynb
│   ├── 03_silver_validation.ipynb
│   ├── 04_gold_scoring.ipynb
│   ├── 05_exception_reporting.ipynb
│   └── 06_remediation.ipynb
├── sample_data/
│   ├── customers_sample.csv        (1,000 rows)
│   ├── accounts_sample.csv         (1,000 rows)
│   └── transactions_sample.csv     (1,000 rows)
├── docs/
├── DataSentry_DQ_Dashboard.pbix
├── DataSentry_DQ_Dashboard.pdf
├── datasentry_job_config.json
└── README.md
```

> Sample data (1,000 rows per table) is included for reference. The full dataset is generated by Notebook 01 and stored in the Unity Catalog Volume.

---

## Output Tables

**Gold (Power BI sources)**

| Table | Description |
|---|---|
| `gold_rule_scores` | Pass rate per rule per table (43 rows) |
| `gold_dimension_scores` | Pass rate by dimension per table |
| `gold_table_scores` | Overall pass rate per table (3 rows) |
| `gold_exception_log` | Every failed record with failed rules, dimensions and severity |
| `gold_top10_failing_rules` | Top 10 failing rules by fail count |
| `gold_remediation_summary` | Before/after pass rates and routing counts per table |

**Remediation**

| Table | Description |
|---|---|
| `silver_remediated_customers` / `_accounts` / `_transactions` | Passing plus auto-fixed records |
| `gold_remediation_log` | Records that were auto-fixed |
| `gold_quarantine_log` | Records held for a source system fix |
| `gold_escalation_log` | Records needing human review, with priority |

---

## Power BI Dashboard

**Page 1: Pipeline Health Overview**
- Check-level pass rate, total checks and total fails
- Table health status with GREEN/AMBER/RED formatting
- Pass rate by dimension
- Rule health status distribution

**Page 2: Exception Deep Dive**
- Total exceptions with HIGH, MEDIUM and LOW cards (73,735 total: 40,890 HIGH, 32,443 MEDIUM, 402 LOW)
- Top 10 failing rules
- Exceptions by table and severity
- Full exception log with rule tags, dimensions and severity

**Page 3: Remediation Before vs After**
- Pass rate before, after, improvement and failures handled
- Before vs after pass rate by table
- Where the failures went: auto-fixed, duplicates dropped, quarantined, escalated
- Detailed remediation table

---

## Author

Kamran Habib
Data Analyst / Data Engineer
[LinkedIn](https://www.linkedin.com/in/kamranhabib10) | [GitHub](https://github.com/loyalatreides)
