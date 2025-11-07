import datetime
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from google.cloud import storage

# -------- Constants --------
PROJECT_ID = "skilful-union-474420-c7"
REGION = "us-central1"
BUCKET_NAME = "my-first-project-covid-etl-bucket"
DATAPROC_CLUSTER = "covid-dp-cluster"

API_URL = "https://data.cdc.gov/resource/n8mc-b4w4.json"
PAGE_SIZE = 50000          # Socrata max ~50k
MAX_RECORDS = 1_000_000    # cap at 1M

REQUIRED_COLUMNS = [
    "case_month",
    "cdc_case_earliest_dt",
    "res_state",
    "age_group",
    "sex",
    "race",
    "ethnicity",
    "death_yn",
    "hosp_yn",
    "icu_yn",
    "medcond_yn",
]


def fetch_covid_to_gcs(ds_nodash, **kwargs):
    """Fetch up to 1M records from CDC API and write JSONL to GCS."""
    object_name = f"bronze/raw_covid_cases_{ds_nodash}.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)

    total = 0
    offset = 0

    with blob.open("w") as f:
        while total < MAX_RECORDS:
            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
            }
            resp = requests.get(API_URL, params=params, timeout=60)
            resp.raise_for_status()
            page = resp.json()

            if not page:
                break

            for row in page:
                filtered = {col: row.get(col) for col in REQUIRED_COLUMNS}
                f.write(json.dumps(filtered) + "\n")
                total += 1
                if total >= MAX_RECORDS:
                    break

            offset += PAGE_SIZE

    print(f"Wrote {total} records to gs://{BUCKET_NAME}/{object_name}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="covid_medallion_pipeline",
    default_args=default_args,
    description="CDC COVID -> GCS -> BQ bronze -> Dataproc -> BQ silver/gold",
    start_date=datetime.datetime(2025, 1, 1),
    schedule_interval="@daily",     #can keep none as well, but kept daily.
    catchup=False,
    max_active_runs=1,
    tags=["covid", "medallion", "gcp"],
) as dag:

    fetch_to_gcs = PythonOperator(
        task_id="fetch_covid_to_gcs",
        python_callable=fetch_covid_to_gcs,   
    )

    wait_for_bq_load = GCSObjectExistenceSensor(
        task_id="wait_for_bq_load",
        bucket=BUCKET_NAME,
        object="signals/bronze_to_bq_success_{{ ds_nodash }}.flag",
        timeout=60 * 60,         # wait up to 1 hour
        poke_interval=60,        # check every minute
        mode="poke",
    )

    dataproc_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET_NAME}/code/covid_transform.py",
            
        },
    }

    run_dataproc = DataprocSubmitJobOperator(
        task_id="run_dataproc_transform",
        job=dataproc_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    fetch_to_gcs >> wait_for_bq_load >> run_dataproc