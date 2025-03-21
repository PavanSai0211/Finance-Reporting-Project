import pandas as pd
from google.cloud import bigquery

def create_kpi_tables():
    client = bigquery.Client()
    
    kpi_queries = {
        "kpi_total_revenue": """
            CREATE OR REPLACE TABLE `financial-project-453807.financial_project.kpi_total_revenue` AS
            SELECT SUM(totalRevenue) AS total_revenue
            FROM `financial-project-453807.financial_project.fact_financials`;
        """,
        
        "kpi_avg_ebitda": """
            CREATE OR REPLACE TABLE `financial-project-453807.financial_project.kpi_avg_ebitda` AS
            SELECT AVG(ebitda) AS avg_ebitda
            FROM `financial-project-453807.financial_project.fact_financials`;
        """,
        
        "kpi_market_cap_growth": """
            CREATE OR REPLACE TABLE `financial-project-453807.financial_project.kpi_market_cap_growth` AS
            SELECT symbol, ((MAX(marketCap) - MIN(marketCap)) / MIN(marketCap)) * 100 AS market_cap_growth
            FROM `financial-project-453807.financial_project.fact_financials`
            GROUP BY symbol;
        """,
        
        "kpi_debt_to_equity": """
            CREATE OR REPLACE TABLE `financial-project-453807.financial_project.kpi_debt_to_equity` AS
            SELECT symbol, AVG(debtToEquity) AS avg_debt_to_equity
            FROM `financial-project-453807.financial_project.fact_financials`
            GROUP BY symbol;
        """
    }
    
    for table_name, query in kpi_queries.items():
        client.query(query)
        print(f"Table {table_name} created successfully.")

if __name__ == "__main__":
    create_kpi_tables()
