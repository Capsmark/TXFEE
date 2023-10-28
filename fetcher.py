import csv
import requests
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


def fetch_api(from_time, to_time, limit, url):
    all_data = []

    while from_time <= limit:
        response = requests.get(url, headers=headers).json()
        result = response.get('result', {})
        data = result.get('data', [])

        for d in data:
            all_data.append(d)

        from_time = to_time
        to_time = (datetime.strptime(to_time, api_format) +
                   timedelta(days=1)).strftime(api_format)
        url = f"https://api.cryptoquant.com/v1/eth/market-data/price-ohlcv?window=day&&limit=100000&from={from_time}&to={to_time}"

    return all_data


def main():
    # Define the starting and ending time for the data range
    from_time = '20200101T000000'
    to_time = (datetime.strptime(from_time, api_format) +
               timedelta(days=100000)).strftime(api_format)
    limit = '20231023T000000'

    # url = f"https://api.cryptoquant.com/v1/eth/network-data/transactions-count?window=day&limit=100000&from={from_time}&to={to_time}"
    # data = fetch_api(from_time, to_time, limit, url)
    # write_to_file(data, 'new/transactions-count.csv')

    # url = f"https://api.cryptoquant.com/v1/eth/network-data/fees?window=day&limit=100000&from={from_time}&to={to_time}"
    # data = fetch_api(from_time, to_time, limit, url)
    # write_to_file(data, 'new/fees.csv')

    # url = f"https://api.cryptoquant.com/v1/eth/network-data/addresses-count?window=day&limit=100000&from={from_time}&to={to_time}"
    # data = fetch_api(from_time, to_time, limit, url)
    # write_to_file(data, 'new/addresses-count.csv')

    url = f"https://api.cryptoquant.com/v1/eth/market-data/price-ohlcv?window=day&&limit=100000&from={from_time}&to={to_time}"
    data = fetch_api(from_time, to_time, limit, url)
    write_to_file(data, 'new/price-ohlcv.csv')


if __name__ == "__main__":
    main()
