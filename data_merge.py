import pandas as pd
import os

def merge_datasets(data_dir="datasets"):
    # Define file paths
    historical_path = os.path.join(data_dir, "cleaned_historical_data.csv")
    company_path = os.path.join(data_dir, "cleaned_company_info.csv")
    dividends_path = os.path.join(data_dir, "cleaned_dividends.csv")
    cashflow_path = os.path.join(data_dir, "cleaned_cashflow.csv")
    balance_sheet_path = os.path.join(data_dir, "cleaned_balance_sheet.csv")
    income_statement_path = os.path.join(data_dir, "cleaned_income_statement.csv")

    # Load datasets with date parsing
    historical_df = pd.read_csv(historical_path, parse_dates=["Date"])
    company_df = pd.read_csv(company_path)
    dividends_df = pd.read_csv(dividends_path, parse_dates=["Date"])
    cashflow_df = pd.read_csv(cashflow_path).rename(columns={"Unnamed: 0": "Date"})
    balance_sheet_df = pd.read_csv(balance_sheet_path).rename(columns={"Unnamed: 0": "Date"})
    income_statement_df = pd.read_csv(income_statement_path).rename(columns={"Unnamed: 0": "Date"})

    # Convert 'Date' columns to datetime
    for df in [historical_df, dividends_df, cashflow_df, balance_sheet_df, income_statement_df]:
        df["Date"] = pd.to_datetime(df["Date"], utc=True)

    # Merge datasets on "Date"
    merged_df = (
        historical_df
        .merge(dividends_df, on="Date", how="left")
        .merge(cashflow_df, on="Date", how="left")
        .merge(balance_sheet_df, on="Date", how="left")
        .merge(income_statement_df, on="Date", how="left")
    )

    # Add company details efficiently using pd.concat()
    company_details = pd.DataFrame([company_df.iloc[0]] * len(merged_df)).reset_index(drop=True)
    merged_df = pd.concat([merged_df.reset_index(drop=True), company_details], axis=1)

    # Save merged data
    output_path = os.path.join(data_dir, "merged_data.csv")
    merged_df.to_csv(output_path, index=False)
    
    return merged_df

if __name__ == "__main__":
    merged_data = merge_datasets()
    print(merged_data.head())
