import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


class Database:
	# Initialization, mapping each instance to a specific table, and storing the partition and sort key names
	def __init__(self, table_name, partition_key, sort_key):
		self._dynamodb = boto3.resource('dynamodb')
		self._table = self._dynamodb.Table(table_name)
		self._partition_key = partition_key
		self._sort_key = sort_key

	# To get exact single data based on partition and sort attribute values
	def query_exact(self, partition_value, sort_value):
		response = self._table.get_item(
			Key={
				'deviceid': partition_value,
				'timestamp': sort_value
			}
		)
		return response.get('Item')
	
	# To get data based on partition key value
	def query(self, partition_value, filter_key=None, filter_value=None):
		query_params = {}
		query_params['KeyConditionExpression'] = Key('deviceid').eq(partition_value)
		if filter_key:
			query_params['FilterExpression'] = Attr(filter_key).eq(filter_value)
		response = self._table.query(**query_params)
		return response.get('Items')

	# To get data based on partition attribute value and comparison with sort attribute range
	def query_cmp(self, partition_value, cmp_op='lt', cmp_value='0'):
		if cmp_op not in ['eq', 'lt', 'gt', 'lte', 'gte']:
			raise Exception(f'{cmp_op}: Not a supported comparison operator.')
		response = self._table.query(
			KeyConditionExpression=eval(f'Key("deviceid").eq("{partition_value}") & Key("timestamp").{cmp_op}("{cmp_value}")')
		)
		return response.get('Items')

	# To get a continuous range of data based on partition attribute value and sort attribute range
	def query_range(self, partition_value, sort_range, filter_key=None, filter_value=None):
		query_params = {}
		query_params['KeyConditionExpression'] = Key('deviceid').eq(partition_value) & \
	    		Key('timestamp').between(sort_range[0], sort_range[1])
		if filter_key:
			query_params['FilterExpression'] = Attr(filter_key).eq(filter_value)
		response = self._table.query(**query_params)
		return response.get('Items')

	# Inserts (or overwrites) a single item based on already formed item dict
	def put_item(self, item):
		pass


