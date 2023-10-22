import os

# Directory path where the CSV files are located
directory_path = '/mnt/storage/csv/'

# Output file path
output_file_path = "/mnt/storage/out/New_BTCUSD_2019-10-1_to_2023-08-06.csv"

# Remove the output file if it already exists
if os.path.exists(output_file_path):
    os.remove(output_file_path)

# Iterate over each file in the directory
for filename in os.listdir(directory_path):
    if filename.endswith('.csv'):
        # Create a full file path by joining the directory path and filename
        file_path = os.path.join(directory_path, filename)

        with open(file_path, 'r') as f_in:
            lines = f_in.readlines()

            # The header is the first line, and the rest of the lines are reversed
            header, data = lines[0], lines[1:]
            reversed_data = data[::-1]

            with open(output_file_path, 'a') as f_out:
                # If it's the first file, write the header
                if os.stat(output_file_path).st_size == 0:
                    f_out.write(header)

                # Write the reversed data to the output file
                f_out.writelines(reversed_data)
