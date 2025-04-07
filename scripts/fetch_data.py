import requests
import pandas as pd
import os
from scripts.config import FRED_API_KEY
import yfinance as yf

# List of macroeconomic indicators (FRED series)
MACRO_FACTORS = {
    "DGS2": "2Y_Treasury_Yield",
    "DGS5": "5Y_Treasury_Yield",
    "DGS10": "10Y_Treasury_Yield",
    "DGS30": "30Y_Treasury_Yield",
    "FEDFUNDS": "Fed_Funds_Rate",
    "RPONTSYD": "Overnight_Repo_Rate",
    "CPIAUCSL": "CPI",
    "CPILFESL": "Core_CPI",
    "PCEPI": "PCE_Inflation",
    "T5YIFR": "5Y_Inflation_Expectation",
    "M2SL": "M2_Money_Supply",
    "TEDRATE": "TED_Spread",
    "BAMLH0A0HYM2": "High_Yield_Spread",
    "A191RL1Q225SBEA": "GDP_Growth",
    "UNRATE": "Unemployment_Rate",
    "INDPRO": "Industrial_Production",
    "RSAFS": "Retail_Sales",
    "DEXUSEU": "USD_EUR_Exchange_Rate"# ,
    # "EMGUS": "EM_Bond_Spread"
}

# Fetch FRED series dynamically
def fetch_fred_series(series_id):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json"
    response = requests.get(url)
    
    try:
        data = response.json()
    except Exception as e:
        print(f"Error decoding JSON for {series_id}: {e}")
        print(f"Response content: {response.text}")
        return pd.DataFrame()  # Return empty DataFrame on error

    if "observations" not in data:
        print(f"⚠️ Warning: No 'observations' found for {series_id}. Response: {data}")
        return pd.DataFrame()  # Return empty DataFrame if no data

    df = pd.DataFrame(data["observations"])
    df = df.drop(columns=["realtime_start", "realtime_end"], errors="ignore")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp()
    df = df.groupby("date").last().reset_index()
    
    return df.rename(columns={"value": series_id})

# Fetch macroeconomic data
def fetch_macro_data():
    all_data = []
    
    for series_id, column_name in MACRO_FACTORS.items():
        df = fetch_fred_series(series_id)
        
        if df.empty:
            print(f"⚠️ Skipping {series_id} ({column_name}) due to missing data.")
        else:
            df = df.rename(columns={series_id: column_name})
            all_data.append(df)
    
    if not all_data:
        raise ValueError("❌ No macroeconomic data was retrieved. Check your FRED series IDs.")
    
    macro_df = all_data[0]
    for df in all_data[1:]:
        macro_df = macro_df.merge(df, on='date', how='outer')
    
    return macro_df

# Fetch bond data from Yahoo Finance
def fetch_yahoo_bond_data(tickers):
    bond_data = {}
    
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        bond = yf.Ticker(ticker)
        bond_hist = bond.history(period="max")  # You can change the period here

        # Ensure the data has a 'Close' column, which is typically the adjusted closing price
        if 'Close' in bond_hist.columns:
            bond_hist = bond_hist[['Close']].rename(columns={'Close': ticker})
            
            # Resample to monthly frequency to avoid duplicate daily dates
            bond_hist.index = pd.to_datetime(bond_hist.index)  # Ensure DateTime format
            bond_hist = bond_hist.resample('M').last()  # Keep last value of each month
            
            bond_data[ticker] = bond_hist[ticker]
        else:
            print(f"⚠️ Warning: No 'Close' data found for {ticker}.")
    
    # Convert the dictionary to a DataFrame
    df_bond_data = pd.DataFrame(bond_data)

    # Reset the index to make 'Date' a column and ensure it is in datetime format
    df_bond_data.reset_index(inplace=True)
    df_bond_data.rename(columns={'index': 'Date'}, inplace=True)
    df_bond_data['Date'] = df_bond_data['Date'].dt.to_period('M').dt.to_timestamp()

    return df_bond_data


# List of bond ETFs (you can add more bond tickers here)
BOND_TICKERS = ['^TNX', 'BND', 'TLT', 'BIL', 'SHY', 'IEF', 'LQD']

# Fetch bond data (Yahoo Finance)
def fetch_bond_data():
    bond_df = fetch_yahoo_bond_data(BOND_TICKERS)
    return bond_df


# Compute credit spreads for corporate bonds
def compute_credit_spreads(df):
    required_columns = ["BAA_Corp_Bond_Yield", "AAA_Corp_Bond_Yield", "10Y_Treasury_Yield"]
    
    # Check if all required columns are present
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"⚠️ Missing columns: {missing_cols}. Skipping credit spread calculations.")
        return df  # Return the original DataFrame without adding spreads

    df["BAA_Spread"] = df["BAA_Corp_Bond_Yield"] - df["10Y_Treasury_Yield"]
    df["AAA_Spread"] = df["AAA_Corp_Bond_Yield"] - df["10Y_Treasury_Yield"]

    return df

# Fetch all data and return two separate DataFrames
def fetch_all_data():
    # Fetch macro and bond data
    macro_data = fetch_macro_data()
    bond_data = fetch_bond_data()

    # Print the first few rows of both DataFrames to inspect them
    print("Macro Data:")
    print(macro_data.head())
    print("\nBond Data:")
    print(bond_data.head())
    bond_data.to_csv("data/bond_data.csv", index=False)

    # Return the two DataFrames separately
    return macro_data, bond_data


if __name__ == "__main__":
    macro_df, bond_df = fetch_all_data()
    # Now you have both macro_df and bond_df for further processing
    