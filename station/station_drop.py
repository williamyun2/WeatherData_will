import pandas as pd

# Load original parquet
df = pd.read_parquet("station.parquet")

# Keep only the needed columns
needed_cols = ["Latitude", "Longitude", "ElevationMeters", "Region", "Country2"]
df_clean = df[needed_cols].copy()

# Save to a new parquet file
df_clean.to_parquet("station_clean.parquet", index=False)

# Optionally, also save as CSV for easier inspection
df_clean.to_csv("station_clean.csv", index=False)

print("Saved cleaned dataset with columns:", df_clean.columns.tolist())
print("Shape:", df_clean.shape)
