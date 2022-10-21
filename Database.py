import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import DYNAMODB_CONTEXT

dynamodb = boto3.resource("dynamodb")


class DynamoDB:
    def __init__(self, table_name):
        self.table_name = table_name
        self.table = dynamodb.Table(self.table_name)

    def read_item(self, pk_name, pk_value):
        """
        Return item read by primary key.
        """
        response = self.table.get_item(Key={pk_name: pk_value})
        return response

    def add_item(self, col_dict):
        """
        Add one item (row) to table. col_dict is a dictionary {col_name: value}.
        """
        response = self.table.put_item(Item=col_dict)
        return response

    def update_item(self, pk_name, pk_value, col_dict):
        """
        update one item (row) to table. col_dict is a dictionary {col_name: value}.
        """

        update_expression = 'SET {}'.format(','.join(f'#{k}=:{k}' for k in col_dict))
        expression_attribute_values = {f':{k}': v for k, v in col_dict.items()}
        expression_attribute_names = {f'#{k}': k for k in col_dict}

        response = self.table.update_item(
            Key={'{}'.format(pk_name): pk_value},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names,
            ReturnValues='UPDATED_NEW',
        )

        return response

    def delete_item(self, pk_name, pk_value):
        """
        Delete an item (row) in table from its primary key.
        """
        return self.table.delete_item(Key={pk_name: pk_value})

    def query(self, filter_key=None, filter_value=None):
        """
        Perform a query operation on the table.
        Can specify filter_key (col name) and its value to be filtered.
        """
        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = self.table.query(KeyConditionExpression=filtering_exp)
        else:
            response = self.table.query()

        return response["Items"]

    def execute_query(
        self,
        key_expression=None,
        filter_expression=None,
        projection_expression=None,
    ):
        """
        queries the db table
        'key_expression' must be of type FilterExpression
        'filter_expression' must be of type KeyConditionExpression
        """
        if filter_expression and not projection_expression:
            response = self.table.query(
                KeyConditionExpression=key_expression,
                FilterExpression=filter_expression,
            )
        elif not filter_expression and projection_expression:
            response = self.table.query(
                KeyConditionExpression=key_expression,
                ProjectionExpression=projection_expression,
            )
        elif filter_expression and projection_expression:
            response = self.table.query(
                KeyConditionExpression=key_expression,
                FilterExpression=filter_expression,
                ProjectionExpression=projection_expression,
            )
        else:
            response = self.table.query(KeyConditionExpression=key_expression)

        items = response["Items"]
        while True:
            if response.get("LastEvaluatedKey"):
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                items += response["Items"]
            else:
                break

        return items

    def scan_firstpage(self, filter_key=None, filter_value=None):
        """
        Perform a scan operation on table.
        Can specify filter_key (col name) and its value to be filtered.
        This gets only first page of results in pagination. Returns the response.
        """

        if filter_key and filter_value:
            filtering_exp = Attr(filter_key).is_in(filter_value)
            response = self.table.scan(FilterExpression=filtering_exp)
        else:
            response = self.table.scan()

        return response["Items"]

    def scan_allpages(self, filter_key=None, filter_value=None):
        """
        Perform a scan operation on table.
        Can specify filter_key (col name) and its value to be filtered.
        This gets all pages of results.
        Returns list of items.
        """
        filtering_exp=None
        
        if filter_key and filter_value:
            filtering_exp = Attr(filter_key).is_in(filter_value)
            response = self.table.scan(FilterExpression=filtering_exp)
        else:
            response = self.table.scan()

        items = response["Items"]
        while True:
            if response.get("LastEvaluatedKey"):
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], FilterExpression=filtering_exp)
                items += response["Items"]
            else:
                break

        return items

    def execute_scan(self, filter_expression, projection_expression=None):
        """
        scans the 'table_name'.
        'filter_expression' must be of type FilterExpression
        """
        if projection_expression:
            response = self.table.scan(
                FilterExpression=filter_expression,
                ProjectionExpression=projection_expression,
            )
        else:
            response = self.table.scan(FilterExpression=filter_expression)

        items = response["Items"]
        while True:
            if response.get("LastEvaluatedKey"):
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                items += response["Items"]
            else:
                break

        return items

    def get_table(self):
        return self.table

