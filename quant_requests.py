import requests

access_token = 'EUfMfAM6TILIl5UUvsONlrG2DPYfrQsdWVnNv2gw'
headers = {'Authorization': 'Bearer ' + access_token}

from_time = '20170807T000000'
to_time = '20170808T000000'
url = f"https://api.cryptoquant.com/v1/eth/network-data/transactions-count?window=hour&from={from_time}&to={to_time}"

# response = requests.get(url, headers=headers).json()
# result = response.get('result', {})
# data = result.get('data', [])

# print(data)

try:
    response = requests.get(url, headers=headers)
    # Raise an exception if the response status is not 2xx (successful)
    response.raise_for_status()

    if response.text:  # Check if the response is not empty
        data = response.json()  # Try to parse the response as JSON
        # Process the data here...

        print(data)
    else:
        print("Response is empty.")
except requests.exceptions.RequestException as e:
    # Handle any request-related exceptions (e.g., network issues, invalid URL, etc.)
    print("Request Error:", e)
except requests.exceptions.JSONDecodeError as e:
    # Handle JSON decoding errors
    print("JSON Decode Error:", e)
except Exception as e:
    # Handle any other unexpected exceptions
    print("Error:", e)
