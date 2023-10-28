import csv
import requests
from datetime import datetime, timedelta

old_format = '%Y-%m-%d %H:%M:%S'
api_format = '%Y%m%dT%H%M%S'


def main():
    access_token = 'EUfMfAM6TILIl5UUvsONlrG2DPYfrQsdWVnNv2gw'
    headers = {'Authorization': 'Bearer ' + access_token}

    # Define the starting and ending time for the data range
    from_time = '20200101T000000'
    to_time = (datetime.strptime(from_time, api_format) +
               timedelta(days=100000)).strftime(api_format)
    limit = '20231023T000000'

    all_data = []

    while from_time <= limit:
        print(f"\nfrom: {from_time} _ to: {to_time}")
        url = f"https://api.cryptoquant.com/v1/eth/network-data/transactions-count?window=day&limit=100000&from={from_time}&to={to_time}"

        response = requests.get(url, headers=headers).json()
        result = response.get('result', {})
        data = result.get('data', [])

        for d in data:
            all_data.append(d)

        from_time = to_time
        to_time = (datetime.strptime(to_time, api_format) +
                   timedelta(days=1)).strftime(api_format)

    # Define the CSV file name and headers
    csv_file = 'new/transactions-count.csv'
    headers = all_data[0].keys() if all_data else []

    # Write the data to a CSV file
    with open(csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for row in all_data:
            writer.writerow(row)


if __name__ == "__main__":
    main()
