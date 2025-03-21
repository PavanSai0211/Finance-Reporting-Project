from data_extraction import download_stock_data
from data_cleaning import clean_data
from data_merge import merge_datasets
from fact_dim_creation import create_fact_and_dimension_tables
from agg import create_aggregation_tables
from kpis_creation import execute_kpi_queries
from data_marts_creation import execute_data_mart_queries

import pandas as pd

def main():
    # Step 1: Download data from Yahoo Finance
    download_stock_data()

    # Step 2: Clean the data
    clean_data()

    # Step 3: Merge the datasets
    df1 = merge_datasets()
    #print(df1.head())

    # Step 4: Create Fact & Dimension Tables
    create_fact_and_dimension_tables()

    # Step 5: Create Aggregation Tables
    create_aggregation_tables()

    # Step 6: Create KPI Tables
    execute_kpi_queries()

    # Step 7: Create Data Marts
    execute_data_mart_queries()
    print("Main file")

if __name__ == "__main__":
    main()
