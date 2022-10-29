from Database import Database
from datetime import datetime, timedelta

DATA_TYPES = ['HeartRate', 'SPO2', 'Temperature']

class RawDataModel:

    def __init__(self):

        # Initialize the database
        self._db = Database('bsm_data', 'deviceid', 'timestamp')
        self._data_types = DATA_TYPES

    # This method aggregates data over a time range for each datatype (sensor type)
    # But the aggregation takes place over a minute by calling the aggregate method.
    def aggregate_by_datatype_and_minute(self, device_id, start_timestamp, end_timestamp):
        aggregated_data = []
        count = 0

        # Iterate over each datatype and aggregate data over every minute
        for data_type in self._data_types:

            # Parse timestamp (stirng) values into datetime objects for datetime operations
            start_time = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            time = start_time
            end_time = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            while time <= end_time:
                # Take the starttime as timestamp in the aggregate data table
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S.%f')
                # Add a minute (60 secs) to compute the end time
                time += timedelta(seconds=60)
                timerange = (timestamp, time.strftime('%Y-%m-%d %H:%M:%S.%f'))
                # Call the aggregate method over a minute and append the results to aggregated_data
                aggregated_data.append(self.aggregate(device_id, timerange, data_type, count))
                # Increment the count. The count is being used to change the timestamp by a few microseconds
                # to avoid exactly duplicate timestamps (which is a key)
                count += 1
        return aggregated_data

    # This method is used to aggregate data over a minute for each datatype
    def aggregate(self, device_id, timerange, datatype, count):
        data = self._db.query_range(device_id, timerange, datatype=datatype)

        # initialize min, max and avg to appropriate values
        min, max, sum = (9999, -9999, 0)

        # Iterate over each data record in the aggregate data
        # to find out the min, max and avg
        for record in data:

            # Handle missing deviceid
            if device_id != record.get("deviceid"):
                continue
        
            # Hangle missing or invalid values and convert to float
            try:
                value = float(record.get('value'))
            except TypeError:
                continue

            if min > value:
                min = value
            if max < value:
                max = value
            sum += value
        try:
            avg = 1.0*sum/(1.0*len(data))
        except ZeroDivisionError as e:
            print(f"No data, cannot proceed further: {e}")
        
        timestamp = datetime.strptime(timerange[0], '%Y-%m-%d %H:%M:%S.%f')
        timestamp += timedelta(microseconds=count)  # Incrementing stored timestamp my a microsec to avoid duplicate keys
        return {'deviceid': device_id,
                'datatype': datatype,
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S.%f'),
                'min': min,
                'max': max,
                'avg': avg
        }
