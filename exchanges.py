import requests
import time
import pandas as pd
from io import StringIO


access_token = 'EUfMfAM6TILIl5UUvsONlrG2DPYfrQsdWVnNv2gw'
headers = {'Authorization': 'Bearer ' + access_token}
url = "https://api.cryptoquant.com/v1/btc/status/entity-list?type=exchange"

response = requests.get(url, headers=headers)
data = response.json()['result']

symbols = [item['symbol'] for item in data['data']]

# Initialize an empty DataFrame to store the cumulative sum
sum_df = pd.DataFrame()

is_invalid = False
for symbol in symbols:
    all_csv_data = []
    first_year = True

    for year in range(2023, 2012, -1):
        print(f"{symbol} _ {year}: ....")
        url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/exchange-inflow-age-distribution?exchange={symbol}&window=hour&from={year - 1}0101T000000&to={year}1231T000000&limit=10000&format=csv"

        response = requests.get(url, headers=headers)
        if response.status_code == 400:
            with open("./age/invalid_symbols.txt", 'a') as invalid_symbols_file:
                invalid_symbols_file.write(f"{symbol}\n")

            is_invalid = True
            break

        if response.status_code == 200:
            is_invalid = False
            csv_content = response.text
            if csv_content.strip():
                # Convert the CSV content to a DataFrame
                new_data_df = pd.read_csv(StringIO(csv_content), parse_dates=[
                                          'datetime'], index_col='datetime')

                # Sum the new data with the cumulative data, considering missing data as 0
                sum_df = sum_df.add(new_data_df, fill_value=0)

                if not first_year:
                    csv_content = '\n'.join(csv_content.split('\n')[1:])
                else:
                    first_year = False

                all_csv_data.append(csv_content)

                print(
                    f"Data for {symbol} for the year {year} retrieved successfully")
        else:
            print(response.json()['status'])
            print(f"Failed to retrieve data for {symbol} for the year {year}")

        time.sleep(0.5)

    if is_invalid:
        print(f"The {symbol} was invalid, skipping to the next")
        is_invalid = False
        continue

    # Save all the CSV data for the symbol to a single file
    with open(f"./age/{symbol}.csv", 'w', newline='', encoding='utf-8') as f:
        for csv_content in all_csv_data:
            f.write(csv_content)
            f.write('\n')  # Add a newline to separate each year's CSV content

    print(f"All data for {symbol} merged and saved to {symbol}.csv")

# Save the cumulative sum data to 'sum.csv'
sum_df.to_csv('./age/sum/sum.csv')

print("Sum data saved to 'sum.csv'")
