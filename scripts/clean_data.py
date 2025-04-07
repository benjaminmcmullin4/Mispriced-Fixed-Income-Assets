 # Cleans & preprocesses raw data

import pandas as pd

def clean_data():
    # Load your dataset (ensure it has been properly fetched before this)
    df = pd.read_csv("data/macro_and_bond_data.csv")
    
    # Convert all columns to numeric (errors='coerce' will turn invalid data into NaNs)
    df = df.apply(pd.to_numeric, errors='coerce')
    
    # Interpolate missing values
    df = df.interpolate().fillna(method='bfill')
    
    # Calculate returns for each column (skip non-numeric columns automatically)
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:  # Ensure we only apply pct_change to numeric columns
            df[f"{col}_returns"] = df[col].pct_change()
    
    # Drop rows with missing values after processing
    df = df.dropna()

    # Save the cleaned data
    df.to_csv("data/cleaned_macro_and_bond_data.csv", index=False)
    
    print("Data cleaning complete. Saved to 'data/cleaned_macro_and_bond_data.csv'.")
    return df

if __name__ == "__main__":
    clean_data()
