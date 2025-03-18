
from google.cloud import bigquery
from config import project_id, dataset_id
import pandas_gbq
from pandas_gbq import to_gbq
from fact_dim_creation import fact_financials, dim_company,dim_dividends,dim_location,dim_market,dim_stock_performance
# Set up BigQuery client
client = bigquery.Client(project=project_id)



# Dictionary of dimension tables
tables = {
    'fact_financials': fact_financials,
    'dim_company': dim_company,
    'dim_market': dim_market,
    'dim_location': dim_location,
    'dim_dividends': dim_dividends,
    'dim_stock_performance': dim_stock_performance
}
for table_name, df in tables.items():
    to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, if_exists="replace")

# Upload all tables to BigQuery
for tab_name, df_name in tables.items():
    pandas_gbq.to_gbq(df_name, f'{dataset_id}.{tab_name}', project_id=project_id, if_exists='replace')
