import os
from google.cloud import bigquery, storage
import functions_framework

# Hard-code your project ID (simplest & safest for this demo)
PROJECT_ID = "skilful-union-474420-c7"

BQ_DATASET = os.environ.get("BQ_DATASET", "medallion_bronze")
BQ_TABLE = os.environ.get("BQ_TABLE", "raw_covid_cases")
SIGNAL_BUCKET = os.environ.get("SIGNAL_BUCKET", "my-first-project-covid-etl-bucket")

bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)

SCHEMA = [
    bigquery.SchemaField("case_month", "STRING"),
    bigquery.SchemaField("cdc_case_earliest_dt", "STRING"),
    bigquery.SchemaField("res_state", "STRING"),
    bigquery.SchemaField("age_group", "STRING"),
    bigquery.SchemaField("sex", "STRING"),
    bigquery.SchemaField("race", "STRING"),
    bigquery.SchemaField("ethnicity", "STRING"),
    bigquery.SchemaField("death_yn", "STRING"),
    bigquery.SchemaField("hosp_yn", "STRING"),
    bigquery.SchemaField("icu_yn", "STRING"),
    bigquery.SchemaField("medcond_yn", "STRING"),
]


@functions_framework.cloud_event
def gcs_to_bigquery(cloud_event):
    """Gen2 CloudEvent function triggered by GCS finalize."""
    data = cloud_event.data
    bucket_name = data["bucket"]
    object_name = data["name"]

    print(f"Received event for gs://{bucket_name}/{object_name}")

    # Only process bronze files
    if not object_name.startswith("bronze/"):
        print(f"Skipping non-bronze object: {object_name}")
        return

    uri = f"gs://{bucket_name}/{object_name}"
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    print(f"Loading URI {uri} into table {table_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # ---- Load into BigQuery ----
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    print("BigQuery load completed.")

    # Derive ds_nodash from file name: bronze/raw_covid_cases_YYYYMMDD.json
    try:
        ds_part = object_name.split("_")[-1].split(".")[0]
    except Exception:
        ds_part = "unknown"

    # ---- Write success flag ----
    signal_bucket = storage_client.bucket(SIGNAL_BUCKET)
    signal_name = f"signals/bronze_to_bq_success_{ds_part}.flag"
    blob = signal_bucket.blob(signal_name)
    blob.upload_from_string("OK")
    print(f"Wrote success flag: gs://{SIGNAL_BUCKET}/{signal_name}")
    #entry point for the function = gcs_to_bigquery.