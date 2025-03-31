import requests
import pandas as pd
import os
from scripts.config import FRED_API_KEY, TREASURY_API_URL

# Fetch data from FRED (Economic series) and aggregate to monthly frequency
def fetch_fred_series(series_id):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['observations'])
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    # Convert 'date' to datetime and then to monthly frequency
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.to_period('M').dt.to_timestamp()  # Convert to month start
    
    # Group by 'date' to ensure monthly aggregation (take most recent value for each month)
    df = df.groupby('date').last().reset_index()

    # Rename columns to match series name
    df = df[['date', 'value']].rename(columns={'value': series_id})

    print(f"Fetched {series_id} data", df.head())
    return df

# Fetch US Treasury yield curve data from FRED and aggregate to monthly frequency
def fetch_treasury_yield():
    treasury_series = [
        "DGS10",  # 10Y Treasury Yield
        "DGS2",   # 2Y Treasury Yield
        "DGS3MO", # 3-Month Treasury Yield
    ]
    
    all_treasury_data = []
    for series in treasury_series:
        df = fetch_fred_series(series)
        all_treasury_data.append(df)
    
    # Merge Treasury data into one DataFrame
    treasury_df = all_treasury_data[0]
    for df in all_treasury_data[1:]:
        treasury_df = treasury_df.merge(df, on='date', how='outer')

    treasury_df = treasury_df.rename(columns={"DGS10": "10Y_Treasury_Yield", "DGS2": "2Y_Treasury_Yield", "DGS3MO": "3M_Treasury_Yield"})
    print("Fetched Treasury Yield Data", treasury_df.head())
    return treasury_df

# Fetch Corporate bond yield data from FRED and aggregate to monthly frequency
def fetch_corporate_bond_yields():
    corporate_bond_series = [
        "BAA10Y",  # Corporate bond yield (BAA 10-Year)
        "AAA10Y",  # Corporate bond yield (AAA 10-Year)
    ]
    
    all_corp_bond_data = []
    for series in corporate_bond_series:
        df = fetch_fred_series(series)
        all_corp_bond_data.append(df)
    
    # Merge Corporate Bond data into one DataFrame
    corporate_bonds_df = all_corp_bond_data[0]
    for df in all_corp_bond_data[1:]:
        corporate_bonds_df = corporate_bonds_df.merge(df, on='date', how='outer')

    corporate_bonds_df = corporate_bonds_df.rename(columns={"BAA10Y": "BAA_Corp_Bond_Yield", "AAA10Y": "AAA_Corp_Bond_Yield"})
    print("Fetched Corporate Bond Yield Data", corporate_bonds_df.head())
    return corporate_bonds_df

# Fetch other macroeconomic data from FRED and aggregate to monthly frequency
def fetch_macro_data():
    fred_series = {
        'CPIAUCSL': 'CPI',  # Inflation
        'DGS10': '10Y_Treasury_Yield',  # 10Y bond yield
        'DGS2': '2Y_Treasury_Yield',  # 2Y bond yield
        'DEXUSEU': 'USD_EUR_Exchange_Rate',  # FX Rate
        'M2SL': 'M2_Money_Supply',  # M2 Money Supply
        'UNRATE': 'Unemployment_Rate'  # Unemployment rate
    }

    all_data = []
    for series_id, name in fred_series.items():
        df = fetch_fred_series(series_id)
        df = df.rename(columns={series_id: name})
        all_data.append(df)

    # Merge all macroeconomic data into one DataFrame
    macro_df = all_data[0]
    for df in all_data[1:]:
        macro_df = macro_df.merge(df, on='date', how='outer')
    
    print("Fetched Macro Data", macro_df.head())
    return macro_df

# Fetch all data (macroeconomic + bond yields) and save as CSV
def fetch_all_data():
    print("Fetching Treasury and macroeconomic data...")
    
    # Fetching macroeconomic data
    macro_data = fetch_macro_data()
    
    # Fetching Treasury yield data
    treasury_data = fetch_treasury_yield()
    
    # Fetching Corporate bond yield data
    corporate_bond_data = fetch_corporate_bond_yields()

    # Merging all datasets into one final DataFrame
    final_df = macro_data
    final_df = final_df.merge(treasury_data, on='date', how='outer')
    final_df = final_df.merge(corporate_bond_data, on='date', how='outer')

    # Save the final data to a CSV
    final_df.to_csv("data/macro_and_bond_data.csv", index=False)

    print("Data fetching complete. Final dataset saved to 'data/macro_and_bond_data.csv'.")

if __name__ == "__main__":
    fetch_all_data()
