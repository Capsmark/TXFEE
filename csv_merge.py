import pandas as pd

# List of file paths for the 4 CSV files
file_paths = ["./csv/fees.csv", "./csv/addresses-count.csv", "./csv/price-ohlcv.csv", "./csv/transactions-count.csv"]

# List to store the dataframes of each CSV file
dataframes = []

# Read each CSV file and store its dataframe in the list
for file_path in file_paths:
    df = pd.read_csv(file_path)
    dataframes.append(df)

# Merge dataframes based on the "date" column
merged_df = pd.merge(dataframes[0], dataframes[1], on="date", how="inner")
merged_df = pd.merge(merged_df, dataframes[2], on="date", how="inner")
merged_df = pd.merge(merged_df, dataframes[3], on="date", how="inner")

# Save the merged dataframe to a new CSV file
merged_df.to_csv("merged_data_Aug_02.csv", index=False)
