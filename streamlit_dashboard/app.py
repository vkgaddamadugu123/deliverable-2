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
    
    # Load and prepare data
    df = load_data()
    df["case_month"] = pd.to_datetime(df["case_month"], errors="coerce")
    df = df.dropna(subset=["case_month"])
    
    # Data overview
    st.subheader("📊 Data Overview")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Records", f"{len(df):,}")
    with col2:
        st.metric("Total Cases", f"{df['total_cases'].sum():,}")
    with col3:
        st.metric("Total Deaths", f"{df['total_deaths'].sum():,}")
    with col4:
        mortality_rate = (df['total_deaths'].sum() / df['total_cases'].sum() * 100) if df['total_cases'].sum() > 0 else 0
        st.metric("Mortality Rate", f"{mortality_rate:.2f}%")
    
    # Raw data table
    st.subheader("📋 Raw Data Sample")
    df_display = df.reset_index(drop=True).sort_values(["case_month", "total_cases"], ascending=[False, False])
    st.dataframe(df_display.head(50))
    
    # Time series analysis
    st.subheader("📈 National Trends Over Time")
    monthly_totals = df.groupby("case_month")[["total_cases", "total_deaths"]].sum().reset_index()
    
    # Create separate charts for better visualization
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Total Cases by Month**")
        st.line_chart(monthly_totals.set_index("case_month")["total_cases"])
    
    with col2:
        st.write("**Total Deaths by Month**")
        st.line_chart(monthly_totals.set_index("case_month")["total_deaths"])
    
    # Combined chart with dual y-axis approach
    st.subheader("📊 Cases vs Deaths Comparison")
    st.write("**Combined View (Cases and Deaths)**")
    st.line_chart(monthly_totals.set_index("case_month")[["total_cases", "total_deaths"]])
    
    # Deaths analysis
    st.subheader(" Death Analysis")
    deaths_by_month = monthly_totals[monthly_totals["total_deaths"] > 0]
    if len(deaths_by_month) > 0:
        st.write(f"**Months with recorded deaths:** {len(deaths_by_month)}")
        st.write(f"**Peak deaths month:** {deaths_by_month.loc[deaths_by_month['total_deaths'].idxmax(), 'case_month'].strftime('%Y-%m')}")
        st.write(f"**Average deaths per month:** {deaths_by_month['total_deaths'].mean():.0f}")
        
        # Show top states by deaths
        st.write("**Top 10 States by Total Deaths:**")
        top_deaths_states = df.groupby("res_state")["total_deaths"].sum().sort_values(ascending=False).head(10)
        st.bar_chart(top_deaths_states)
    else:
        st.warning("No death data found in the current dataset")
    
    # State-level analysis
    st.subheader("🗺️ State-Level Analysis")
    selected_states = st.multiselect(
        "Select states to compare:",
        options=sorted(df["res_state"].unique()),
        default=["CA", "NY", "TX", "FL"] if all(state in df["res_state"].unique() for state in ["CA", "NY", "TX", "FL"]) else sorted(df["res_state"].unique())[:4]
    )
    
    if selected_states:
        state_data = df[df["res_state"].isin(selected_states)]
        state_monthly = state_data.groupby(["case_month", "res_state"])[["total_cases", "total_deaths"]].sum().reset_index()
        
        # Pivot for better visualization
        cases_pivot = state_monthly.pivot(index="case_month", columns="res_state", values="total_cases")
        deaths_pivot = state_monthly.pivot(index="case_month", columns="res_state", values="total_deaths")
        
        st.write("**Cases by Selected States:**")
        st.line_chart(cases_pivot)
        
        st.write("**Deaths by Selected States:**")
        st.line_chart(deaths_pivot)

if __name__ == "__main__":
    main()
