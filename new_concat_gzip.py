import os
import gzip

# Directory path where the CSV files are located
directory_path = '/mnt/storage/csv/'

# Output directory path
output_directory = "/mnt/storage/gzip/"
output_base_name = "New_BTCUSD_2019-10-1_to_2023-08-15_"

# Max file size: 5GB
MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB


def get_next_output_path(output_directory, output_base_name):
    """Generate the next available output path."""
    index = 1
    while True:
        potential_path = os.path.join(
            output_directory, f"{output_base_name}{index}.csv")
        if not os.path.exists(potential_path):
            return potential_path
        index += 1


def compress_file(file_path):
    """Compress a file using gzip."""
    with open(file_path, 'rb') as f_in:
        with gzip.open(file_path + '.gz', 'wb') as f_out:
            f_out.writelines(f_in)
    os.remove(file_path)


def main():
    current_output_path = get_next_output_path(
        output_directory, output_base_name)

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

                while reversed_data:
                    with open(current_output_path, 'a') as f_out:
                        # If it's the first file, write the header
                        if os.stat(current_output_path).st_size == 0:
                            f_out.write(header)

                        # Check how much space we have left in the current output file
                        space_left = MAX_FILE_SIZE - \
                            os.stat(current_output_path).st_size

                        # Determine how many lines we can write without exceeding the limit
                        bytes_to_write = 0
                        lines_to_write = []
                        for line in reversed_data:
                            bytes_to_write += len(line)
                            if bytes_to_write > space_left:
                                break
                            lines_to_write.append(line)

                        # Write the lines to the output file
                        f_out.writelines(lines_to_write)

                        # Remove the written lines from reversed_data
                        reversed_data = reversed_data[len(lines_to_write):]

                        # If we've used up the current output file, compress it and start a new one
                        if not reversed_data or os.stat(current_output_path).st_size >= MAX_FILE_SIZE:
                            compress_file(current_output_path)
                            current_output_path = get_next_output_path(
                                output_directory, output_base_name)


if __name__ == "__main__":
    main()
