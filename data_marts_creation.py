from google.cloud import bigquery
from google.oauth2 import service_account
from config import project_id,dataset_id
def execute_data_mart_queries():
    client = bigquery.Client()
    # Define dataset ID
    #dataset_id = "finanacial_project"  # Corrected dataset ID

    queries = [
        f"""
        CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.market_performance_mart` AS
        SELECT 
            f.Date,
            f.symbol,
            f.marketCap,
            f.enterpriseValue,
            f.Open,
            f.High,
            f.Low,
            f.Close,
            f.Volume,
            sp.fiftyDayAverage,
            sp.twoHundredDayAverage,
            sp.fiftyTwoWeekLow,
            sp.fiftyTwoWeekHigh
        FROM `{client.project}.{dataset_id}.fact_financials` f
        LEFT JOIN `{client.project}.{dataset_id}.dim_stock_performance` sp 
            ON f.symbol = sp.symbol;
        """,
        f"""
        CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.dividend_payout_mart` AS
        SELECT 
            d.symbol,
            d.dividendRate,
            d.dividendYield,
            d.payoutRatio,
            d.fiveYearAvgDividendYield,
            d.lastDividendValue,
            d.lastDividendDate
        FROM `{client.project}.{dataset_id}.dim_dividends` d;
        """,
        f"""
        CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.risk_governance_mart` AS
        SELECT 
            c.symbol,
            c.auditRisk,
            c.boardRisk,
            c.compensationRisk,
            c.shareHolderRightsRisk,
            c.overallRisk,
            c.governanceEpochDate,
            c.compensationAsOfEpochDate
        FROM `{client.project}.{dataset_id}.dim_company` c;
        """
    ]

    # Execute queries
    for query in queries:
        query_job = client.query(query)
        query_job.result()
        print("Query executed successfully.")

if __name__ == "__main__":
    execute_data_mart_queries()
