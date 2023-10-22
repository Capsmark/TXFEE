import boto3

db_name = 'eth_transaction-count-total'
# db_name = 'eth_data'
region_name = 'eu-north-1'
# region_name = 'us-east-1'
aws_access_key_id = 'AKIAW2WDGE537RK2FO54'
aws_secret_access_key = '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'

access_token = 'EUfMfAM6TILIl5UUvsONlrG2DPYfrQsdWVnNv2gw'


def delete_all():
    dynamodb = boto3.resource('dynamodb',
                              region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
    table = dynamodb.Table(db_name)

    response = table.scan()

    # Delete each item one by one
    with table.batch_writer() as batch:
        for item in response['Items']:
            print(item)
            batch.delete_item(Key={
                'datetime': item['datetime']
            })

    print("All items have been deleted from the table")


delete_all()
