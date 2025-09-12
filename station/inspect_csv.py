import pandas as pd

# Read the CSV file
df = pd.read_csv(r"C:\class\code\service_testing\cds\station\hawaii_station.csv")

# Get the 2nd column (index 1)
second_column = df.iloc[:, 0]

# Get min and max values
min_value = second_column.min()
max_value = second_column.max()

print(f"2nd column name: '{second_column.name}'")
print(f"Minimum value: {min_value}")
print(f"Maximum value: {max_value}")

# Optional: Show some additional info
print(f"Column data type: {second_column.dtype}")
print(f"Number of values: {len(second_column)}")
print(f"Number of non-null values: {second_column.count()}")

# Show first few values to verify
print(f"\nFirst 5 values in 2nd column:")
print(second_column.head())