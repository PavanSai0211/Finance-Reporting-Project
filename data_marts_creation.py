from google.cloud import bigquery
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Fetch variables
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
email_password = os.getenv("EMAIL_PASSWORD")

def create_data_marts():
    client = bigquery.Client()
    
    queries = {
        "Profitability_DataMart": """
            CREATE OR REPLACE TABLE `{dataset_id}.Profitability_DataMart` AS
            SELECT symbol, totalRevenue, grossProfits, ebitda, returnOnAssets, returnOnEquity, earningsGrowth, revenueGrowth
            FROM `{dataset_id}.fact_financials`;
        """.format(dataset_id=dataset_id),
        
        "Market_Performance_DataMart": """
            CREATE OR REPLACE TABLE `{dataset_id}.Market_Performance_DataMart` AS
            SELECT symbol, Date, Open, High, Low, Close, Volume, fiftyTwoWeekLow, fiftyTwoWeekHigh, fiftyDayAverage, twoHundredDayAverage
            FROM `{dataset_id}.fact_financials`
            JOIN `{dataset_id}.dim_stock_performance` USING (symbol);
        """.format(dataset_id=dataset_id),
        
        "Risk_Governance_DataMart": """
            CREATE OR REPLACE TABLE `{dataset_id}.Risk_Governance_DataMart` AS
            SELECT symbol, debtToEquity, currentRatio, quickRatio, auditRisk, boardRisk, compensationRisk, shareHolderRightsRisk, overallRisk
            FROM `{dataset_id}.fact_financials`
            JOIN `{dataset_id}.dim_company` USING (symbol);
        """.format(dataset_id=dataset_id),
        
        "Dividend_Analysis_DataMart": """
            CREATE OR REPLACE TABLE `{dataset_id}.Dividend_Analysis_DataMart` AS
            SELECT symbol, dividendRate, dividendYield, exDividendDate, payoutRatio, fiveYearAvgDividendYield, lastDividendValue, lastDividendDate
            FROM `{dataset_id}.dim_dividends`;
        """.format(dataset_id=dataset_id)
    }
    
    for name, query in queries.items():
        print(f"Creating {name}...")
        query_job = client.query(query)
        query_job.result()
        print(f"{name} created successfully!")

if __name__ == "__main__":
    create_data_marts()
