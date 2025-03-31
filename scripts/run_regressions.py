# Runs factor regressions on bond returns

import pandas as pd
import statsmodels.api as sm

def run_regression():
    df = pd.read_csv("data/cleaned_macro_data.csv")

    # Define Y (bond excess returns) and X (macro factors)
    y = df["10Y_Treasury_Yield_returns"]
    x = df[["CPI_returns", "USD_EUR_Exchange_Rate_returns"]]
    
    # Add constant term
    x = sm.add_constant(x)

    # Run regression
    model = sm.OLS(y, x).fit()
    
    # Save results
    df["predicted_returns"] = model.predict(x)
    df["residuals"] = y - df["predicted_returns"]
    
    df.to_csv("data/regression_results.csv", index=False)

if __name__ == "__main__":
    run_regression()
