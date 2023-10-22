import boto3

#
db_name = 'eth_transaction-count-total'
# db_name = 'eth_data'
region_name = 'eu-north-1'
# region_name = 'us-east-1'
aws_access_key_id = 'AKIAW2WDGE537RK2FO54'
aws_secret_access_key = 'PyAyYJ8zbnsnMtmoT1sQsv1CLOHkDYJgq91AbqaK'

access_token = 'EUfMfAM6TILIl5UUvsONlrG2DPYfrQsdWVnNv2gw'


def scan_and_sort_table():
    # Your existing code to scan and sort the DynamoDB table
    dynamodb = boto3.resource('dynamodb',
                              region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
    table = dynamodb.Table(db_name)
    response = table.scan()
    sorted_items = sorted(response['Items'], key=lambda x: x['datetime'])
    return sorted_items

# Function to write the sorted_items to a CSV file


def write_to_csv(sorted_items):
    if not sorted_items:
        return ""

    header = sorted_items[0].keys()

    # Writing to a CSV string
    output_csv = ""
    output_csv += ",".join(header) + "\n"
    for item in sorted_items:
        output_csv += ",".join(str(item[key]) for key in header) + "\n"

    return output_csv


# Main execution
sorted_items = scan_and_sort_table()
print(write_to_csv(sorted_items))
