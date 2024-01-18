import csv
import requests
import pandas as pd
from datetime import datetime, timedelta

old_format = '%Y-%m-%d %H:%M:%S'
api_format = '%Y%m%dT%H%M%S'

access_token = 'EUfMfAM6TILIl5UUvsONlrG2DPYfrQsdWVnNv2gw'
headers = {'Authorization': 'Bearer ' + access_token}


def write_to_file(data, csv_file):
    headers = data[0].keys() if data else []

    with open(csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()

        for row in data:
            writer.writerow(row)


def fetch_api(from_time, to_time, limit, base_url):
    all_data = []
    url = (f"{base_url}?window=day&limit=100000&"
           f"from={from_time}&to={to_time}")

    while from_time <= limit:
        response = requests.get(url, headers=headers).json()
        result = response.get('result', {})
        data = result.get('data', [])

        for d in data:
            all_data.append(d)

        from_time = to_time
        to_time = (datetime.strptime(to_time, api_format) +
                   timedelta(days=1)).strftime(api_format)
        url = (f"{base_url}?window=day&limit=100000&"
               f"from={from_time}&to={to_time}")

    return all_data


def merge_csv_files():
    # List of file paths for the 4 CSV files
    file_paths = ["./new/fees.csv", "./new/addresses-count.csv",
                  "./new/price-ohlcv.csv", "./new/transactions-count.csv"]

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
    merged_df.to_csv("./merged.csv", index=False)


def main():
    # Define the starting and ending time for the data range
    from_time = '20200101T000000'
    to_time = (datetime.strptime(from_time, api_format) +
               timedelta(days=100000)).strftime(api_format)
    limit = '20231101T000000'

    name = "transactions-count"
    base_url = f"https://api.cryptoquant.com/v1/eth/network-data/{name}"
    data = fetch_api(from_time, to_time, limit, base_url)
    write_to_file(data, f"./new/{name}.csv")

    name = "fees"
    base_url = f"https://api.cryptoquant.com/v1/eth/network-data/{name}"
    data = fetch_api(from_time, to_time, limit, base_url)
    write_to_file(data, f"./new/{name}.csv")

    name = "addresses-count"
    base_url = f"https://api.cryptoquant.com/v1/eth/network-data/{name}"
    data = fetch_api(from_time, to_time, limit, base_url)
    write_to_file(data, f"./new/{name}.csv")

    name = "price-ohlcv"
    base_url = f"https://api.cryptoquant.com/v1/eth/market-data/{name}"
    data = fetch_api(from_time, to_time, limit, base_url)
    write_to_file(data, f"./new/{name}.csv")

    merge_csv_files()


if __name__ == "__main__":
    main()
