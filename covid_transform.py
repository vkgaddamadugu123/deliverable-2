from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    count,
    sum as _sum,
    trim,
    upper,
)

# ----- Constants -----
PROJECT_ID = "skilful-union-474420-c7"

BRONZE_TABLE = f"{PROJECT_ID}.medallion_bronze.raw_covid_cases"
SILVER_TABLE = f"{PROJECT_ID}.medallion_silver.covid_cases_clean"
GOLD_TABLE   = f"{PROJECT_ID}.medallion_gold.covid_state_monthly"

# GCS bucket for BigQuery connector temp files
TEMP_GCS_BUCKET = "my-first-project-covid-etl-bucket"


def main():
    spark = (
        SparkSession.builder
        .appName("covid_bronze_to_silver_gold")
        .getOrCreate()
    )

    # Tell BigQuery connector which GCS bucket to use for temporary data
    spark.conf.set("temporaryGcsBucket", TEMP_GCS_BUCKET)
    # NEW (optional but good): fewer partitions for 1M rows
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    # ----- BRONZE -> base DF -----
    df_bronze = (
        spark.read.format("bigquery")
        .option("table", BRONZE_TABLE)
        .load()
    )

    # Basic cleaning / standardisation
    df_clean = (
        df_bronze
        .select(
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
        )
        # keep only rows with a case_month and state
        .filter(col("case_month").isNotNull())
        .filter(col("res_state").isNotNull())
        # simple demo filter: only 2021+ data (optional, but keeps volume bounded)
        .filter(col("case_month") >= "2021-01")
        # normalise some string columns
        .withColumn("res_state", upper(trim(col("res_state"))))
        .withColumn("sex",       upper(trim(col("sex"))))
        .withColumn("race",      upper(trim(col("race"))))
        .withColumn("ethnicity", upper(trim(col("ethnicity"))))
        # drop completely unknown state
        .filter(col("res_state") != "UNKNOWN")
    )

    # ---- SILVER: flags + cleaned columns ----
    df_silver = (
        df_clean
        .withColumn("death_flag",   when(col("death_yn") == "Yes", 1).otherwise(0))
        .withColumn("hosp_flag",    when(col("hosp_yn")  == "Yes", 1).otherwise(0))
        .withColumn("icu_flag",     when(col("icu_yn")   == "Yes", 1).otherwise(0))
        .withColumn("medcond_flag", when(col("medcond_yn") == "Yes", 1).otherwise(0))
    )

    (
        df_silver.write.format("bigquery")
        .option("table", SILVER_TABLE)
        # Overwrite the entire silver table each run (idempotent)
        .mode("overwrite")
        .save()
    )

    # ---- GOLD: aggregated metrics ----
    df_gold = (
        df_silver
        .groupBy("case_month", "res_state")
        .agg(
            count("*").alias("total_cases"),
            _sum("death_flag").alias("total_deaths"),
        )
    )

    (
        df_gold.write.format("bigquery")
        .option("table", GOLD_TABLE)
        .mode("overwrite")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()