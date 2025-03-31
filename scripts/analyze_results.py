# Identifies mispriced bonds

import pandas as pd

def identify_mispricing():
    df = pd.read_csv("data/regression_results.csv")

    # Define a threshold for mispricing (e.g., top 5% largest residuals)
    threshold = df["residuals"].abs().quantile(0.95)
    mispriced_bonds = df[df["residuals"].abs() > threshold]

    mispriced_bonds.to_csv("data/mispriced_bonds.csv", index=False)

    print("Top mispriced bonds saved.")

if __name__ == "__main__":
    identify_mispricing()
