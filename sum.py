import pandas as pd
from glob import glob

# List of all CSV files
csv_files = glob("./*.csv")

# Read the first file to initialize the dataframe
master_df = pd.read_csv(csv_files[0])

# Loop through the remaining CSV files and add their values to the master dataframe
for file in csv_files[1:]:
    temp_df = pd.read_csv(file)
    for column in master_df.columns:
        if column != 'datetime':
            master_df[column] += temp_df[column]

# Save the summed data to a new CSV file
master_df.to_csv("summed_data.csv", index=False)
