from os import times
from Database import Database
from datetime import datetime, timedelta
import math

DATA_TYPES = ['HeartRate', 'SPO2', 'Temperature']

class RawDataModel:
    def __init__(self):
        self._db = Database('bsm_data', 'deviceid', 'timestamp')
        self._data_types = DATA_TYPES

    def aggregate_by_datatype_and_minute(self, device_id, start_timestamp, end_timestamp):
        aggregated_data = []
        for data_type in self._data_types:
            start_time = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            time = start_time
            end_time = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            while time <= end_time:
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S.%f')
                print(timestamp)
                time += timedelta(seconds=60)
                timerange = (timestamp, time.strftime('%Y-%m-%d %H:%M:%S.%f'))
                aggregated_data.append(self.aggregate(device_id, timerange, data_type))
        return aggregated_data

    def aggregate(self, device_id, timerange, datatype):
        data = self._db.query_range(device_id, timerange, datatype=datatype)
        min, max, sum = (9999, -9999, 0)
        for record in data:
            value = float(record['value'])
            if min > value:
                min = value
            if max < value:
                max = value
            sum += value
        avg = 1.0*sum/(1.0*len(data))
        return {'deviceid': device_id,
                'datatype': datatype,
                'timestamp': timerange[0],
                'min': min,
                'max': max,
                'avg': avg
        }
