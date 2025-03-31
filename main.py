# Orchestrates the full pipeline

import pandas as pd
from scripts.fetch_data import fetch_all_data
from scripts.clean_data import clean_data
from scripts.run_regressions import run_regression
from scripts.analyze_results import identify_mispricing

# 1. Fetch all required data (Treasury and macroeconomic data)
print("Fetching Treasury and macroeconomic data...")
fetch_all_data()  # This will fetch and save data to "data/macro_data.csv"

# 2. Clean the data
print("Cleaning data...")
clean_df = clean_data()
# Save the cleaned data to a csv file after making it a df
clean_df = pd.DataFrame(clean_df)
clean_df.to_csv("data/cleaned_macro_data.csv", index=False)

# 3. Run regression analysis
print("Running regressions...")
regression_results = run_regression()

# 4. Identify mispriced fixed-income assets
print("Identifying mispriced bonds...")
mispriced_bonds = identify_mispricing(regression_results)
mispriced_bonds.to_csv("data/mispriced_bonds.csv", index=False)

print("✅ Process complete! Check 'data/mispriced_bonds.csv' for results.")
