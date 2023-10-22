import requests
from datetime import datetime, timedelta
import boto3

old_format = '%Y-%m-%d %H:%M:%S'
api_format = '%Y%m%dT%H%M%S'

# eth_transaction-count-total
db_name = 'eth_data'
# region_name = 'eu-north-1'
region_name = 'us-east-1'
aws_access_key_id = 'AKIAW2WDGE537RK2FO54'
aws_secret_access_key = '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'

access_token = 'EUfMfAM6TILIl5UUvsONlrG2DPYfrQsdWVnNv2gw'


def lambda_handler(event, context):
    # def lambda_handler():
    headers = {'Authorization': 'Bearer ' + access_token}

    # Define the starting and ending time for the data range
    from_time = '20190807T000000'
    to_time = '20190808T000000'
    limit = '20200807T000000'

    dynamodb = boto3.resource('dynamodb',
                              region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
    table = dynamodb.Table(db_name)

    with table.batch_writer() as batch:
        while from_time <= limit:
            print(f"from: {from_time} _ to: {to_time}")
            url = f"https://api.cryptoquant.com/v1/eth/network-data/fees?window=hour&from={from_time}&to={to_time}"

            response = requests.get(url, headers=headers).json()
            result = response.get('result', {})
            data = result.get('data', [])

            for d in data:
                print("\n")
                print(d["transactions_count_total"])
                date_object = datetime.strptime(
                    d["datetime"], old_format).isoformat()
                print(date_object)
                print('---------')

                batch.put_item(Item={
                    'datetime': date_object,
                    'fees_block_mean': d["fees_block_mean"],
                    'fees_block_mean_usd': d["fees_block_mean_usd"],
                    'fees_total': d["fees_total"],
                    'fees_total_usd': d["fees_total_usd"],
                    'fees_reward_percent': d["fees_reward_percent"]
                })

                print("\n+")

            from_time = to_time
            to_time = (datetime.strptime(to_time, api_format) +
                       timedelta(days=1)).strftime(api_format)

    # Return or further process the extracted data as required
    return {
        "statusCode": 200,
        "body": "Data extraction successful."
    }


# lambda_handler()
