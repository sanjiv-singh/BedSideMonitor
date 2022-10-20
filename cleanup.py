import boto3


default_table_name = 'bsm_data'

dynamodb = boto3.resource('dynamodb')

def delete_tables():
    for table in dynamodb.tables.all():
        table.delete()

if __name__ == '__main__':

    ans = input(f'Delete all tables (y/n)') or "n"
    if ans in ('y', 'Y'):
        delete_tables()


