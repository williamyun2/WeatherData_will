import pandas as pd

df = pd.read_parquet("station_clean.parquet")

with open("station_report.txt", "w") as f:
    f.write("--- Shape ---\n")
    f.write(str(df.shape) + "\n\n")

    f.write("--- Columns ---\n")
    f.write(str(df.columns.tolist()) + "\n\n")

    f.write("--- Data types ---\n")
    f.write(str(df.dtypes) + "\n\n")

    f.write("--- Missing values per column ---\n")
    f.write(str(df.isna().sum()) + "\n\n")

    f.write("--- Describe (numeric stats) ---\n")
    f.write(str(df.describe()) + "\n\n")

    f.write("--- Head (first 5 rows) ---\n")
    f.write(str(df.head()) + "\n")
