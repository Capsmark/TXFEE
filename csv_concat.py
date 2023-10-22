import os

# Directory path where the CSV files are located
directory_path = '/mnt/storage/csv/'

# Output file path
output_file_path = "/mnt/storage/out/BTCUSD_2019-10-1_to_2023-08-06.csv"

# Remove the output file if it already exists
if os.path.exists(output_file_path):
    os.remove(output_file_path)

# Iterate over each file in the directory
for filename in os.listdir(directory_path):
    if filename.endswith('.csv'):
        # Create a full file path by joining the directory path and filename
        file_path = os.path.join(directory_path, filename)

        if not os.path.isfile(output_file_path):
            # If the output file doesn't exist, copy the first file with header
            with open(file_path, 'r') as f_in, open(output_file_path, 'w') as f_out:
                f_out.write(f_in.read())
        else:
            # If the output file exists, append without the header
            with open(file_path, 'r') as f_in, open(output_file_path, 'a') as f_out:
                next(f_in)  # Skip the header
                f_out.write(f_in.read())
