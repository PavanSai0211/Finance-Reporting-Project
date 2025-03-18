from google.cloud import bigquery
from config import project_id,dataset_id
# Set up BigQuery client
client = bigquery.Client()

# Define dataset and project ID
# project_id = "financial-project-453807"
# dataset_id = "finanacial_project"  # Corrected dataset name

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
        job.result()  # Wait for the query to finish
        print(f"Table {table_name} created successfully.")

    print("All aggregation tables created successfully!")

if __name__ == "__main__":
    create_aggregation_tables()
