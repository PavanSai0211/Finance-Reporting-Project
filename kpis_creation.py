from google.cloud import bigquery
from google.oauth2 import service_account
from config import project_id,dataset_id

def execute_kpi_queries():
    client = bigquery.Client()

    # Initialize BigQuery client
    #client = bigquery.Client(credentials=credentials, project="financial-project-453807")

    queries = [
        """
        CREATE OR REPLACE TABLE `financial-project-453807.finanacial_project.kpi_avg_daily_volume` AS
        SELECT 
            EXTRACT(YEAR FROM DATE(Date)) AS year,
            AVG(Volume) AS avg_daily_volume
        FROM `financial-project-453807.finanacial_project.fact_financials`
        GROUP BY year
        ORDER BY year;
        """,
        """
        CREATE OR REPLACE TABLE `financial-project-453807.finanacial_project.kpi_avg_closing_price` AS
        SELECT 
          EXTRACT(YEAR FROM DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', Date))) AS year,
          AVG(Close) AS avg_closing_price
        FROM `financial-project-453807.finanacial_project.fact_financials`
        GROUP BY year
        ORDER BY year;
        """,
        """
        CREATE OR REPLACE TABLE `financial-project-453807.finanacial_project.kpi_yearly_report` AS
        SELECT 
            EXTRACT(YEAR FROM TIMESTAMP(Date)) AS year,
            symbol,
            SUM(totalRevenue) AS total_revenue,
            SUM(ebitda) AS total_ebitda
        FROM `financial-project-453807.finanacial_project.fact_financials`
        GROUP BY 1, 2
        ORDER BY 1, 2;
        """
    ]

    # Execute queries
    for query in queries:
        query_job = client.query(query)
        query_job.result()
        print("Query executed successfully.")

if __name__ == "__main__":
    execute_kpi_queries()
