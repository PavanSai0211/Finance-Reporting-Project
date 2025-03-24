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
    historical_df = pd.read_csv(historical_path, parse_dates=["Date"]).drop_duplicates()
    company_df = pd.read_csv(company_path)
    dividends_df = pd.read_csv(dividends_path, parse_dates=["Date"]).drop_duplicates()
    cashflow_df = pd.read_csv(cashflow_path).rename(columns={"Unnamed: 0": "Date"}).drop_duplicates()
    balance_sheet_df = pd.read_csv(balance_sheet_path).rename(columns={"Unnamed: 0": "Date"}).drop_duplicates()
    income_statement_df = pd.read_csv(income_statement_path).rename(columns={"Unnamed: 0": "Date"}).drop_duplicates()

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

    # Drop duplicate rows
    merged_df = merged_df.drop_duplicates()
    merged_df = merged_df.dropna(axis=1, how='all')
    merged_df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    merged_df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    merged_df["total_ebitda"] = pd.to_numeric(df["total_ebitda"], errors="coerce")
    merged_df.fillna(0, inplace=True)
    merged_df["revenue"] = pd.to_numeric(merged_df["revenue"], errors="coerce")





    # Drop rows where ‘Date’ is missing (important for time-series)
    merged_df = merged_df.dropna(subset=["Date"])

    # Fill missing numeric values with 0
    numeric_cols = merged_df.select_dtypes(include=["number"]).columns
    merged_df[numeric_cols] = merged_df[numeric_cols].fillna(0)

    # Fill missing categorical values with 'Unknown'
    categorical_cols = merged_df.select_dtypes(include=["object"]).columns
    merged_df[categorical_cols] = merged_df[categorical_cols].fillna("Unknown")

    # Add company details, ensuring no duplication
    if not company_df.empty:
        company_details = company_df.iloc[0].to_dict()
        for col, val in company_details.items():
            merged_df[col] = val  # Assign the value directly instead of concatenating

    # Save cleaned merged data
    #output_path = os.path.join(data_dir, "merged_data.csv")
    merged_df.to_csv("cleaned_merged_data.csv", index=False)

    print("Merging complete. Shape of merged data:", merged_df.shape)
    print("Missing values after cleaning:", merged_df.isnull().sum().sum())
    print(list(merged_df.columns))
    return merged_df

if __name__ == "__main__":
    merged_data = merge_datasets()
    print(merged_data.head())
