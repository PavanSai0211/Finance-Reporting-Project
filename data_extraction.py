import pandas as pd
import yfinance as yf
def download_stock_data(ticker="AAPL"):
    # stock = yf.Ticker(ticker)

    # # Download historical data
    # hist_data = stock.history(period="40y", interval="1d")
    # hist_data.to_csv(f"datasets/{ticker}_historical_data.csv")

    # # Download financial data
    # company_info = pd.DataFrame([stock.info])
    # company_info.to_csv(f"datasets/{ticker}_company_info.csv", index=False)

    # stock.financials.T.to_csv(f"datasets/{ticker}_income_statement.csv")
    # stock.balance_sheet.T.to_csv(f"datasets/{ticker}_balance_sheet.csv")
    # stock.cashflow.T.to_csv(f"datasets/{ticker}_cashflow.csv")

    # stock.dividends.to_csv(f"datasets/{ticker}_dividends.csv")
    # stock.splits.to_csv(f"datasets/{ticker}_splits.csv")

    # # Download options data if available
    # if stock.options:
    #     latest_expiry = stock.options[-1]
    #     option_chain = stock.option_chain(latest_expiry)
    #     option_chain.calls.to_csv(f"datasets/{ticker}_options_calls.csv", index=False)
    #     option_chain.puts.to_csv(f"datasets/{ticker}_options_puts.csv", index=False)

    print("Stock data download completed.")
