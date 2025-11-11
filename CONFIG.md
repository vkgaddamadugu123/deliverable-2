# Configuration Reference - COVID-19 Pipeline with Streamlit Dashboard

## Project Configuration

### Core Settings
```python
# Project Information
PROJECT_ID = "skilful-union-474420-c7"
REGION = "us-central1"
BUCKET_NAME = "my-first-project-covid-etl-bucket"
DATAPROC_CLUSTER = "covid-dp-cluster"

# Phase 2 Configuration
GKE_CLUSTER = "covid-dashboard-cluster"
DOCKER_REPO = "covid-docker-repo"
DASHBOARD_IMAGE = "covid-dashboard:v1"
GSA_NAME = "covid-dashboard-sa"

# API Configuration
CDC_API_URL = "https://data.cdc.gov/resource/n8mc-b4w4.json"
PAGE_SIZE = 50000
MAX_RECORDS = 1_000_000
```

### File Locations
```bash
# GCS Structure
gs://my-first-project-covid-etl-bucket/
├── bronze/                          # Raw COVID data files
│   └── raw_covid_cases_YYYYMMDD.json
├── signals/                         # Success signal files
│   └── bronze_to_bq_success_YYYYMMDD.flag
└── code/                           # Spark job code
    └── covid_transform.py

# Docker Registry Structure
us-central1-docker.pkg.dev/skilful-union-474420-c7/covid-docker-repo/
└── covid-dashboard:v1               # Streamlit dashboard image
```

### BigQuery Tables
```sql
-- Bronze Layer (Raw Data)
`skilful-union-474420-c7.medallion_bronze.raw_covid_cases`

-- Silver Layer (Cleaned Data)
`skilful-union-474420-c7.medallion_silver.covid_cases_clean`

-- Gold Layer (Aggregated Data)
`skilful-union-474420-c7.medallion_gold.covid_state_monthly`
```

## Resource Specifications

### Dataproc Cluster
```yaml
cluster_name: covid-dp-cluster
region: us-central1
workers: 2
machine_type: n1-standard-4
disk_size: 100GB
connectors: BigQuery Spark connector
auto_delete: 3 hours
```

### Cloud Function
```yaml
name: gcs-to-bigquery
runtime: python39
memory: 512MB
timeout: 540s
trigger: GCS file upload (bronze/)
```

### Cloud Composer
```yaml
environment: covid-pipeline-env
location: us-central1
node_count: 3
machine_type: n1-standard-1
python_version: 3
```

### GKE Cluster
```yaml
cluster_name: covid-dashboard-cluster
type: Autopilot
region: us-central1
workload_identity: enabled
```

### Streamlit Dashboard
```yaml
framework: Streamlit
python_version: 3.12
container_port: 8080
service_type: LoadBalancer
replicas: 1
```

## Required APIs
```bash
# Enable these APIs
bigquery.googleapis.com
storage.googleapis.com
cloudfunctions.googleapis.com
dataproc.googleapis.com
composer.googleapis.com
artifactregistry.googleapis.com
container.googleapis.com
```

## IAM Permissions

### Service Account Roles
```yaml
# Cloud Function Service Account
roles:
  - roles/storage.objectViewer
  - roles/bigquery.dataEditor
  - roles/bigquery.jobUser

# Dataproc Service Account  
roles:
  - roles/bigquery.dataEditor
  - roles/bigquery.jobUser
  - roles/storage.objectViewer

# Composer Service Account
roles:
  - roles/storage.admin
  - roles/dataproc.editor
  - roles/bigquery.admin

# Dashboard Service Account
roles:
  - roles/bigquery.dataViewer
  - roles/bigquery.jobUser
```

## Data Schema

### COVID Data Fields
```python
REQUIRED_COLUMNS = [
    "case_month",           # Month of case occurrence
    "cdc_case_earliest_dt", # Earliest case date
    "res_state",           # State of residence
    "age_group",           # Age category
    "sex",                 # Gender
    "race",                # Race category
    "ethnicity",           # Ethnicity category
    "death_yn",            # Death indicator
    "hosp_yn",             # Hospitalization indicator
    "icu_yn",              # ICU indicator
    "medcond_yn"           # Medical condition indicator
]
```

## Quick Commands Reference

### Start Pipeline
```bash
# Trigger Airflow DAG manually
gcloud composer environments run covid-pipeline-env \
    --location=us-central1 \
    dags trigger covid_medallion_pipeline
```

### Phase 2: Dashboard Deployment
```bash
# Build and push Docker image
cd streamlit_dashboard
gcloud builds submit --tag us-central1-docker.pkg.dev/skilful-union-474420-c7/covid-docker-repo/covid-dashboard:v1

# Deploy to GKE
kubectl apply -f k8s/covid-dashboard-deployment.yaml
kubectl apply -f k8s/covid-dashboard-service.yaml

# Get dashboard URL
kubectl get services covid-dashboard-service
```

### Check Status
```bash
# Check BigQuery tables
bq query "SELECT COUNT(*) FROM \`PROJECT.medallion_bronze.raw_covid_cases\`"

# Check GCS files
gsutil ls gs://bucket-name/bronze/
gsutil ls gs://bucket-name/signals/

# Check Dataproc cluster
gcloud dataproc clusters describe covid-dp-cluster --region=us-central1

# Check GKE deployment
kubectl get deployments covid-dashboard
kubectl get services covid-dashboard-service
```

### Troubleshooting
```bash
# View Cloud Function logs
gcloud logging read "resource.type=cloud_function"

# View Dataproc job logs  
gcloud dataproc jobs list --region=us-central1

# View Airflow logs
gcloud composer environments run covid-pipeline-env \
    --location=us-central1 \
    dags state covid_medallion_pipeline

# View dashboard logs
kubectl logs deployment/covid-dashboard

# View GKE cluster status
gcloud container clusters describe covid-dashboard-cluster --region=us-central1
```

This configuration provides all the essential settings needed to deploy and run the COVID-19 data pipeline successfully.
