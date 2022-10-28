from time import time
from Database import Database

class AggregateDataModel:

    def __init__(self):
        self._db = Database('bsm_agg_data', 'deviceid', 'timestamp')
 
    def insert_aggregate_data(self, device_id, agg_data):
        for record in agg_data:
            self._db.put_item(record)


