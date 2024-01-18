import requests
from datetime import datetime, timedelta
import boto3

old_format = '%Y-%m-%d %H:%M:%S'
api_format = '%Y%m%dT%H%M%S'


def delete_all():
    dynamodb = boto3.resource('dynamodb',
                              region_name='eu-north-1',
                              aws_access_key_id='AKIAW2WDGE537AGI7R4X',
                              aws_secret_access_key='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')
    table = dynamodb.Table('eth_transaction-count-total')

    response = table.scan()

    # Delete each item one by one
    with table.batch_writer() as batch:
        for item in response['Items']:
            print(item)
            batch.delete_item(Key={
                'datetime': item['datetime']
            })

    print("All items have been deleted from the table")


# def lambda_handler(event, context):
def lambda_handler():
    access_token = 'EUfMfAM6TILIl5UUvsONlrG2DPYfrQsdWVnNv2gw'
    headers = {'Authorization': 'Bearer ' + access_token}

    # Define the starting and ending time for the data range
    from_time = '20160807T000000'
    to_time = '20160808T000000'
    limit = '20170725T000000'

    dynamodb = boto3.resource('dynamodb',
                              region_name='eu-north-1',
                              aws_access_key_id='AKIAW2WDGE537AGI7R4X',
                              aws_secret_access_key='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')
    table = dynamodb.Table('eth_transaction-count-total')

    while from_time <= limit:
        print(f"from: {from_time} _ to: {to_time}")
        url = f"https://api.cryptoquant.com/v1/eth/network-data/transactions-count?window=hour&from={from_time}&to={to_time}"

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

            res = table.put_item(Item={
                'datetime': date_object,
                'count': d["transactions_count_total"],
            })

            print("\n+")
            print(res)

        from_time = to_time
        to_time = (datetime.strptime(to_time, api_format) +
                   timedelta(days=1)).strftime(api_format)

    # Return or further process the extracted data as required
    return {
        "statusCode": 200,
        "body": "Data extraction successful."
    }


lambda_handler()
