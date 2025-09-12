import pandas as pd
import os

# Input file
input_file = "git_station.parquet"

# Load parquet
df = pd.read_parquet(input_file, engine="pyarrow")

# Create output filename by changing extension
output_file = os.path.splitext(input_file)[0] + ".csv"

# Save as CSV
df.to_csv(output_file, index=False)

print(f"Converted {input_file} to {output_file}")