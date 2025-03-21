import pandas as pd

def create_fact_and_dimension_tables():
    df = pd.read_csv('merged_data.csv')

    # Drop null columns
    df = df.dropna(axis=1, how='all')

    # Drop unnecessary columns
    df.drop(['Dividends_y', 'executiveTeam', 'bid', 'ask', 'bidSize', 'askSize'], axis=1, inplace=True)

    # Fact Table: Fact_Financials
    fact_financials = df[[ 
        "Date", "Open", "High", "Low", "Close", "Volume", "Dividends_x", "Stock Splits",
        "previousClose", "dayLow", "dayHigh", "marketCap", "enterpriseValue", "totalRevenue",
        "grossProfits", "ebitda", "returnOnAssets", "returnOnEquity", "debtToEquity", "currentRatio", 
        "quickRatio", "earningsGrowth", "revenueGrowth", "priceToBook", "priceEpsCurrentYear", 
        "trailingPE", "forwardPE", "symbol"
    ]]

    # Dimension Table: Company Information
    dim_company = df[[ 
        "symbol", "shortName", "longName", "industry", "industryKey", "industryDisp", "sector", 
        "sectorKey", "sectorDisp", "longBusinessSummary", "fullTimeEmployees", "companyOfficers", 
        "auditRisk", "boardRisk", "compensationRisk", "shareHolderRightsRisk", "overallRisk", 
        "governanceEpochDate", "compensationAsOfEpochDate", "irWebsite", "website"
    ]].drop_duplicates()

    # Dimension Table: Market Information
    dim_market = df[[ 
        "exchange", "market", "fullExchangeName", "exchangeTimezoneName", "exchangeTimezoneShortName", 
        "currency", "financialCurrency", "tradeable", "cryptoTradeable", "priceHint", "quoteType"
    ]].drop_duplicates()

    # Dimension Table: Location Information
    dim_location = df[[ 
        "symbol", "address1", "city", "state", "zip", "country", "phone"
    ]].drop_duplicates()

    # Dimension Table: Dividends Information
    dim_dividends = df[[ 
        "symbol", "dividendRate", "dividendYield", "exDividendDate", "payoutRatio", 
        "fiveYearAvgDividendYield", "lastDividendValue", "lastDividendDate"
    ]].drop_duplicates()

    # Dimension Table: Stock Performance
    dim_stock_performance = df[[ 
        "symbol", "fiftyTwoWeekLow", "fiftyTwoWeekHigh", "fiftyDayAverage", "twoHundredDayAverage", 
        "trailingAnnualDividendRate", "trailingAnnualDividendYield", "epsTrailingTwelveMonths", 
        "epsForward", "epsCurrentYear", "fiftyDayAverageChange", "fiftyDayAverageChangePercent", 
        "twoHundredDayAverageChange", "twoHundredDayAverageChangePercent"
    ]].drop_duplicates()
    print("Fact___Dim done")