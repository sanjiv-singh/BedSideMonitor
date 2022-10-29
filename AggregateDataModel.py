from time import time
from Database import Database

class AggregateDataModel:

    def __init__(self):
        self._db = Database('bsm_agg_data', 'deviceid', 'timestamp')
 
    def fetch(self, device_id, start_time, end_time):
        return self._db.query_range(device_id, (start_time, end_time))
    
    def insert(self, device_id, agg_data):
        for record in agg_data:
            self._db.put_item(record)


