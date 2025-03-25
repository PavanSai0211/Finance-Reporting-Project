import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from automation_report import generate_report, send_email
from dotenv import load_dotenv
import os

# Loading environment variables from .env file
load_dotenv()

# Fetching variables
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
email_password = os.getenv("EMAIL_PASSWORD")


# Initialize BigQuery Client
client = bigquery.Client()


# Function to fetch data from BigQuery
@st.cache_data
def fetch_data(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

# Load final.csv for visualizations
@st.cache_data
def load_data():
    df = pd.read_csv("final.csv")
    df["Date"] = pd.to_datetime(df["Date"])
    return df

# Sidebar Navigation
st.sidebar.title("Navigation")
menu = st.sidebar.radio("Go to", ["Overview", "Schema", "Visualizations", "KPIs", "Aggregates", "Data Marts", "Report Automation"])

# Overview Section
if menu == "Overview":
    st.title("Financial Data Analysis Dashboard")
    st.write("""
    This project develops a financial reporting automation framework that consolidates data from multiple sources,
    applies transformations using Python, and loads the data into BigQuery. It enables standardized financial reporting,
    automates period-end analysis, and provides customizable reports through Streamlit.
    
    **Terminologies:**
    - **BigQuery:** A cloud-based data warehouse for running SQL queries on large datasets.
    - **ETL (Extract, Transform, Load):** The process of extracting data, transforming it into a usable format, and loading it into a storage system.
    - **Data Marts:** Subsets of data warehouses focused on specific business functions like financial performance.
    - **KPIs (Key Performance Indicators):** Metrics used to evaluate the success of financial and business performance.
    - **EBITDA (Earnings Before Interest, Taxes, Depreciation, and Amortization):** A measure of a company's operating performance.
    - **Revenue:** The total income generated from sales or services before expenses are deducted.
    - **OHLC (Open-High-Low-Close):** A method of representing stock price movements.
    """)

# Schema Section
elif menu == "Schema":
    st.title("Project Schema")
    st.write("""
    **Schema Structure:**
    - **Fact Tables:** Contain transactional and financial data (e.g., revenue, EBITDA, stock prices).
    - **Dimension Tables:** Contain descriptive attributes (e.g., dates, company details, financial categories).
    - **Aggregates:** Pre-computed summaries for faster analysis.
    - **Data Marts:** Business-specific structured data for performance analysis.
    """)
    st.image("star_schema.png", caption="Financial Reporting Star Schema", use_container_width=True)

# Visualizations Section
elif menu == "Visualizations":
    st.title("Financial Data Visualizations")
    df = load_data()
    
    # st.write("Dataset Preview:")
    # st.write(df.head())

    # Revenue vs EBITDA Trend
    if "total_revenue" in df.columns and "total_ebitda" in df.columns:
        st.subheader("Revenue & EBITDA Over Time")
        fig1 = px.line(df, x="Date", y=["total_revenue", "total_ebitda"], title="Revenue vs EBITDA", markers=True)
        st.plotly_chart(fig1, use_container_width=True)

    # OHLC Candlestick Chart
    st.subheader("Stock Price OHLC Chart")
    fig2 = go.Figure(data=[go.Candlestick(
        x=df["Date"],
        open=df["Open"],
        high=df["High"],
        low=df["Low"],
        close=df["Close"],
        name="OHLC Data"
    )])
    fig2.update_layout(title="OHLC Stock Price Movement", xaxis_rangeslider_visible=True)
    st.plotly_chart(fig2, use_container_width=True)

    # Closing Price Trend
    st.subheader("Closing Price Over Time")
    fig3 = px.line(df, x="Date", y="Close", title="Stock Closing Price Trend", markers=True)
    st.plotly_chart(fig3, use_container_width=True)

elif menu == "KPIs":
    st.title("Key Performance Indicators (KPIs)")
    
    selected_table = st.selectbox("Select a KPI:", 
                                  ["kpi_avg_daily_volume", "kpi_avg_closing_price", "kpi_yearly_report"])

    query = f"SELECT * FROM `{client.project}.{dataset_id}.{selected_table}`"
    df_kpi = fetch_data(query)

    if df_kpi.empty:
        st.warning("No data available for the selected KPI.")
    else:
        st.write("### **Fetched KPI Data:**")
        st.dataframe(df_kpi)

        # st.write("### **Column Names & Data Types:**")
        # st.write(df_kpi.dtypes)

        figs = []

        if "avg_daily_volume" in df_kpi.columns:
            st.subheader("Average Daily Trading Volume Over Years")
            fig1 = px.bar(df_kpi, x="year", y="avg_daily_volume", title="Average Daily Trading Volume", 
                          color="avg_daily_volume")
            figs.append(fig1)

        if "avg_closing_price" in df_kpi.columns:
            st.subheader("Average Closing Price Over Years")
            fig2 = px.line(df_kpi, x="year", y="avg_closing_price", title="Average Closing Price", markers=True)
            figs.append(fig2)

        revenue_col = None
        ebitda_col = None

        for col in df_kpi.columns:
            if col.lower() == "total_revenue":
                revenue_col = col
            if col.lower() == "total_ebitda":
                ebitda_col = col

        if revenue_col and ebitda_col:
            st.subheader("Yearly Revenue & EBITDA")
            df_kpi["year"] = df_kpi["year"].astype(int)
            fig3 = px.bar(df_kpi, x="year", y=[revenue_col, ebitda_col], 
                          title="Yearly Revenue & EBITDA", barmode="group")
            figs.append(fig3)

        for fig in figs:
            st.plotly_chart(fig, use_container_width=True)

elif menu == "Aggregates":
    st.title("Aggregated Financial Data")
    
    selected_kpi = st.selectbox("Select an aggregate table:", 
                                ["Yearly Dividend Yield", "Average Daily Volume", "Annual Closing Price"])

    table_mapping = {
        "Yearly Dividend Yield": "agg_yearly_dividend_yield",
        "Average Daily Volume": "kpi_avg_daily_volume",
        "Annual Closing Price": "kpi_avg_closing_price"
    }

    query = f"SELECT * FROM `{client.project}.{dataset_id}.{table_mapping[selected_kpi]}`"
    df = fetch_data(query)

    if df.empty:
        st.warning("No data available for the selected aggregate.")
    else:
        st.write(df)

        st.subheader("Aggregates Visualization")
        
        # Handle Graphs Based on Selected KPI
        if selected_kpi == "Yearly Dividend Yield" and "avg_dividend_yield" in df.columns:
            fig = px.line(df, x="year", y="avg_dividend_yield", 
                          title="Yearly Dividend Yield", markers=True)
            st.plotly_chart(fig, use_container_width=True)

        elif selected_kpi == "Average Daily Volume" and "avg_daily_volume" in df.columns:
            fig = px.bar(df, x="year", y="avg_daily_volume", 
                         title="Average Daily Trading Volume", color="avg_daily_volume")
            st.plotly_chart(fig, use_container_width=True)

        elif selected_kpi == "Annual Closing Price" and "avg_closing_price" in df.columns:
            fig = px.line(df, x="year", y="avg_closing_price", 
                          title="Annual Average Closing Price", markers=True)
            st.plotly_chart(fig, use_container_width=True)


# Data Marts Section
elif menu == "Data Marts":
    st.title("Financial Data Marts")
    selected_table = st.selectbox("Select a Data Mart:", ["market_performance_mart", "risk_governance_mart"])

    query = f"SELECT * FROM `{client.project}.{dataset_id}.{selected_table}` LIMIT 10"
    df = fetch_data(query)
    st.write(df)

#automation reporting code
elif menu == "Report Automation":
    st.title("Automated Financial Reports")
    report_type = st.selectbox("Select Report Type", ["Monthly","Yearly"])

    if st.button("Generate & Send Report"):
        report_file = generate_report(report_type.lower())
        send_email(report_file, report_type.lower())
        st.success(f"{report_type} Financial Report sent successfully!")
