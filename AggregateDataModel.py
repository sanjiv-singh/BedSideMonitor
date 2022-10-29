from Database import Database

class AggregateDataModel:

    def __init__(self):

        # Initialise the database
        self._db = Database('bsm_agg_data', 'deviceid', 'timestamp')
 
    # This method fetches data based on parition key (deviceid) and 
    # range of sort key (start and end timestamps)
    def fetch(self, device_id, start_time, end_time):
        return self._db.query_range(device_id, (start_time, end_time))
    
    # Method to insert a new record
    def insert(self, device_id, agg_data):
        for record in agg_data:
            self._db.put_item(record)


