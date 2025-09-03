import pandas as pd

# Load parquet
df = pd.read_parquet("station_hawaii.parquet", engine="pyarrow")  # or engine="fastparquet"

# Save as CSV
df.to_csv("station_hawaii.csv", index=False)
