import pandas as pd

def clean_data():
    """
    Function to clean and preprocess financial datasets.
    
    Returns:
    - Dictionary containing cleaned dataframes.
    """

    # Load datasets
    income_df = pd.read_csv("datasets/AAPL_income_statement.csv")
    balance_sheet_df = pd.read_csv("datasets/AAPL_balance_sheet.csv")
    cashflow_df = pd.read_csv("datasets/AAPL_cashflow.csv")
    company_info_df = pd.read_csv("datasets/AAPL_company_info.csv")
    dividends_df = pd.read_csv("datasets/AAPL_dividends.csv")
    historical_data_df = pd.read_csv("datasets/AAPL_historical_data.csv")

    # Handle missing values in all datasets
    for df in [income_df, balance_sheet_df, cashflow_df, company_info_df, dividends_df, historical_data_df]:
        df.fillna(method='ffill', inplace=True)  # Forward fill missing values
        df.fillna(method='bfill', inplace=True)  # Backward fill as a fallback

    # Convert 'Date' column to datetime and set as index for historical data
    historical_data_df["Date"] = pd.to_datetime(historical_data_df["Date"], errors='coerce')
    historical_data_df.set_index("Date", inplace=True)

    # Compute moving averages and volatility for historical data
    historical_data_df["50-Day MA"] = historical_data_df["Close"].rolling(window=50, min_periods=1).mean()
    historical_data_df["200-Day MA"] = historical_data_df["Close"].rolling(window=200, min_periods=1).mean()
    historical_data_df["Daily_Return"] = historical_data_df["Close"].pct_change()
    historical_data_df["Rolling_Volatility"] = historical_data_df["Daily_Return"].rolling(window=30, min_periods=1).std()

    # Ensure index is timezone-naive
    #historical_data_df.index = historical_data_df.index.tz_localize(None)

    # Fill missing values in calculated columns
    fill_columns = ["50-Day MA", "200-Day MA", "Daily_Return", "Rolling_Volatility"]
    for col in fill_columns:
        historical_data_df[col].fillna(method='ffill', inplace=True)  # Forward fill first
        historical_data_df[col].fillna(method='bfill', inplace=True)  # Backward fill if needed

    # Remove unnecessary columns if they exist
    columns_to_drop = ["50_MA", "200_MA", "Year-Month", "Difference"]
    historical_data_df.drop(columns=[col for col in columns_to_drop if col in historical_data_df.columns], inplace=True)

    # Rename columns where necessary
    income_df.rename(columns={'Unnamed: 0': 'Date'}, inplace=True)
    cashflow_df.rename(columns={'Unnamed: 0': 'Date'}, inplace=True)
    balance_sheet_df.rename(columns={'Unnamed: 0': 'Date'}, inplace=True)

    # Check for duplicate dates and handle them if necessary
    balance_sheet_df.drop_duplicates(subset=["Date"], inplace=True)
    cashflow_df.drop_duplicates(subset=["Date"], inplace=True)

    # Save cleaned datasets
    income_df.to_csv("cleaned_income_statement.csv", index=False)
    balance_sheet_df.to_csv("cleaned_balance_sheet.csv", index=False)
    cashflow_df.to_csv("cleaned_cashflow.csv", index=False)
    company_info_df.to_csv("cleaned_company_info.csv", index=False)
    dividends_df.to_csv("cleaned_dividends.csv", index=False)
    historical_data_df.to_csv("cleaned_historical_data.csv")

    # Return cleaned dataframes for further use

    print(income_df.isnull().sum())
    print(balance_sheet_df.isnull().sum())
    print(cashflow_df.isnull().sum())
    print(company_info_df.isnull().sum())
    print(dividends_df.isnull().sum())
    print(historical_data_df.isnull().sum())

    return {
        "Income Statement": income_df,
        "Balance Sheet": balance_sheet_df,
        "Cash Flow": cashflow_df,
        "Company Info": company_info_df,
        "Dividends": dividends_df,
        "Historical Data": historical_data_df
    }
