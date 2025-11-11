import os
import pandas as pd
import streamlit as st
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "skilful-union-474420-c7")
TABLE_ID = f"{PROJECT_ID}.medallion_gold.covid_state_monthly"

@st.cache_data(ttl=600)
def load_data():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""SELECT case_month, res_state, total_cases, total_deaths
                FROM `{TABLE_ID}` ORDER BY case_month, res_state;"""
    df = client.query(query).to_dataframe()
    return df

def main():
    st.title("COVID-19 Monthly Cases & Deaths by State (Gold Layer)")
    df = load_data()
    df["case_month"] = pd.to_datetime(df["case_month"], errors="coerce")
    df_display = df.reset_index(drop=True)
    st.dataframe(df_display.head(50))
    st.line_chart(df.groupby("case_month")[["total_cases","total_deaths"]].sum())

if __name__ == "__main__":
    main()
