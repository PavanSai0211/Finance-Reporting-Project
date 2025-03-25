from google.cloud import bigquery
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# Fetch variables
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
email_password = os.getenv("EMAIL_PASSWORD")


client = bigquery.Client()
def create_aggregation_tables():
    """Creates aggregation tables in BigQuery."""
    
    queries = {
        "agg_monthly_volume": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.agg_monthly_volume` AS
            SELECT 
                DATE_TRUNC(DATE(Date), MONTH) AS month,
                AVG(Volume) AS avg_trading_volume
            FROM `{project_id}.{dataset_id}.fact_financials`
            GROUP BY month
            ORDER BY month;
        """,
        "agg_monthly_stock_performance": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.agg_monthly_stock_performance` AS
            SELECT 
                DATE_TRUNC(DATE(Date), MONTH) AS month,
                AVG(Open) AS avg_open,
                AVG(Close) AS avg_close,
                MAX(High) AS max_high,
                MIN(Low) AS min_low
            FROM `{project_id}.{dataset_id}.fact_financials`
            GROUP BY month
            ORDER BY month;
        """,
        "agg_yearly_dividend_yield": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.agg_yearly_dividend_yield` AS
            SELECT 
                EXTRACT(YEAR FROM DATE(Date)) AS year,
                AVG(dividends_x) AS avg_dividend_yield
            FROM `{project_id}.{dataset_id}.fact_financials`
            GROUP BY year
            ORDER BY year;
        """,
        "agg_annual_price_volatility": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.agg_annual_price_volatility` AS
            SELECT 
                EXTRACT(YEAR FROM DATE(Date)) AS year,
                STDDEV(Close) AS price_volatility
            FROM `{project_id}.{dataset_id}.fact_financials`
            GROUP BY year
            ORDER BY year;
        """
    }

    for table_name, query in queries.items():
        print(f"Executing query for {table_name}...")
        job = client.query(query)
        job.result()
        print(f"Table {table_name} created successfully.")

    print("All aggregation tables created successfully!")

if __name__ == "__main__":
    create_aggregation_tables()
