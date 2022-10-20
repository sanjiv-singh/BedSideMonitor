import boto3


default_table_name = 'bsm_data'

dynamodb = boto3.resource('dynamodb')

def create_table(table_name):
    table = dynamodb.create_table(
	TableName=table_name,
	KeySchema=[
        {
            'AttributeName': 'deviceid',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'deviceid',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'timestamp',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    })
    table.wait_until_exists()

if __name__ == '__main__':

    name = input(f'Enter table name ({default_table_name}) : ') or default_table_name
    create_table(name)

