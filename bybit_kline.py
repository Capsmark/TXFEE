import requests
import csv
import time
from datetime import datetime, timedelta, timezone


def date_to_milliseconds(date_str):
    # Convert date string to datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')

    # Get the timestamp in milliseconds
    timestamp_ms = int(date_obj.timestamp())
    return timestamp_ms


def save_to_csv(data, file_name):
    fieldnames = ["category", "symbol", "startTime", "openPrice",
                  "highPrice", "lowPrice", "closePrice", "volume", "turnover"]
    with open(file_name, mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def get_kline_data(category, symbol, interval, start, end, limit=1000):
    base_url = "https://api-testnet.bybit.com/v5/market/kline"
    formatted_data = []

    start_datetime = datetime.fromtimestamp(start, tz=timezone.utc)
    limit_datetime = datetime.fromtimestamp(end, tz=timezone.utc)

    while start_datetime < limit_datetime:
        # Calculate the end_datetime by adding 16 hours to the start_datetime
        end_datetime = start_datetime + timedelta(hours=16)

        # If the calculated end_datetime exceeds the limit_datetime, set end_datetime as limit_datetime
        if end_datetime > limit_datetime:
            end_datetime = limit_datetime

        # Convert start and end timestamps to milliseconds
        start_ms = int(start_datetime.timestamp() * 1000) - 1
        end_ms = int(end_datetime.timestamp() * 1000) + 2

        print('\n----------------------------')
        print(start_datetime, end_datetime)
        print(start_ms, end_ms)
        print('----------------------------\n')

        # Prepare the query parameters
        params = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "start": start_ms,
            "end": end_ms,
            "limit": limit
        }

        try:
            # Make the GET request
            response = requests.get(base_url, params=params)

            # Check if the request was successful
            response.raise_for_status()

            # Parse the JSON response
            data = response.json()

            # print(data)
            # Extract and format the required fields
            candles = data.get("result", {}).get("list", [])
            for candle in candles:
                # Remove the last three digits from the string before converting to int
                timestamp = int(candle[0][:-3])
                start_time = datetime.fromtimestamp(
                    timestamp, tz=timezone.utc)
                formatted_candle = {
                    "category": category,
                    "symbol": symbol,
                    "startTime": start_time.isoformat(),
                    "openPrice": candle[1],
                    "highPrice": candle[2],
                    "lowPrice": candle[3],
                    "closePrice": candle[4],
                    "volume": candle[5],
                    "turnover": candle[6]
                }
                print(formatted_candle)
                formatted_data.append(formatted_candle)

            # Print the time range and the number of data points fetched
            # num_actual_data = len(formatted_data)

            # If the number of data points fetched is less than the limit, we have reached the end of available data
            # if num_actual_data < limit:
            #     break

        except requests.exceptions.RequestException as e:
            print("Error making the request:", e)
            break

        # Update start_datetime for the next request
        start_datetime = end_datetime

        # Introduce a 1-second delay between each iteration
        time.sleep(3)
    formatted_data.sort(key=lambda x: x["startTime"])
    return formatted_data


# Rest of the code remains the same

# Example usage:
category = "linear"
symbol = "BTCUSDT"
interval = 1  # Interval is in minutes (60 minutes = 1 hour)
start_timestamp = date_to_milliseconds("2020-03-01")
end_timestamp = date_to_milliseconds("2023-08-04")
limit = 1000

data = get_kline_data(category, symbol, interval,
                      start_timestamp, end=end_timestamp, limit=limit)

# Save the data to a CSV file
file_name = "kline_data_minute.csv"
save_to_csv(data, file_name)

print("Data saved to CSV:", file_name)
