 # Cleans & preprocesses raw data

import pandas as pd

def clean_data():
    df = pd.read_csv("data/macro_and_bond_data.csv", parse_dates=['date'])

    # Fill missing values (interpolation or forward-fill)
    df = df.interpolate().fillna(method='bfill')

    # Compute returns (log differences)
    for col in df.columns[1:]:
        df[f"{col}_returns"] = df[col].pct_change()

    # Drop first row due to NaN returns
    df = df.dropna()

    df.to_csv("data/cleaned_macro_data.csv", index=False)

if __name__ == "__main__":
    clean_data()
