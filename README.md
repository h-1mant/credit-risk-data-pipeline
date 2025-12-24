# Credit Risk Data Pipeline (GCP)

## Objective
Build a data pipeline to ingest daily transaction data, enrich it with static cardholder information, identify potential credit risk signals, and store analytics-ready results in BigQuery.

---

## Architecture
![Architecture Diagram](img/path/to/architecture.png)

**Flow Overview**
- Static cardholder data stored in BigQuery
- Daily transaction JSON files ingested from GCS
- Airflow orchestrates execution
- PySpark processing runs on Dataproc Serverless
- Final enriched data written to BigQuery

---

## Tech Stack
- Python
- PySpark
- Google Cloud Storage (GCS)
- Google BigQuery
- Dataproc Serverless
- Cloud Composer (Airflow)
- PyTest
- GitHub
- GitHub Actions (CI/CD)

---

## Data Setup

### 1. Cardholder Static Data
Upload cardholder data to GCS:


Create BigQuery dataset and table:
- Dataset: `credit-analysis`
- Table: `cardholders` (static table loaded from CSV)

### 2. Transaction Data
- Daily transaction JSON files arrive in a GCS input folder
- Files are archived after successful processing

---

## Pipeline Orchestration (Airflow)

The pipeline is orchestrated using **Cloud Composer (Airflow)**.

### DAG Tasks
1. **GCSFileSensor**
   - Detects availability of daily transaction files
2. **Dataproc Serverless Batch Job**
   - Triggers PySpark credit risk processing
3. **Archive Step**
   - Moves processed files to an archive folder in GCS

---

## PySpark Processing Logic

### Validations
- Schema enforcement
- Null and invalid value checks

### Transformations
- Enrichment using cardholder attributes
- Feature derivation for credit risk analysis
- Bucketing and categorical transformations

### Output
- Written to BigQuery
- Mode: `append`
- Write method: `Direct`

---

## CI/CD & Testing

### Unit Testing
- PySpark transformation logic tested using **PyTest**

### GitHub Actions Workflow

#### On Push to `dev`
- Checkout repository
- Setup Python environment
- Run unit tests

#### On Push to `main`
- Checkout repository
- Authenticate to GCP using GitHub Secrets
- Setup gcloud SDK
- Upload PySpark application to GCS
- Deploy Airflow DAG to Cloud Composer


