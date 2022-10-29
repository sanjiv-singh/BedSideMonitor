from itertools import count
from os import times
from typing import Type
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
        count = 0
        for data_type in self._data_types:
            start_time = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            time = start_time
            end_time = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            while time <= end_time:
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S.%f')
                time += timedelta(seconds=60)
                timerange = (timestamp, time.strftime('%Y-%m-%d %H:%M:%S.%f'))
                aggregated_data.append(self.aggregate(device_id, timerange, data_type, count))
                count += 1
        return aggregated_data

    def aggregate(self, device_id, timerange, datatype, count):
        data = self._db.query_range(device_id, timerange, datatype=datatype)
        min, max, sum = (9999, -9999, 0)
        for record in data:
            if device_id != record.get("deviceid"):
                continue
            try:
                value = float(record.get('value'))
            except TypeError:
                continue
            if min > value:
                min = value
            if max < value:
                max = value
            sum += value
        avg = 1.0*sum/(1.0*len(data))
        timestamp = datetime.strptime(timerange[0], '%Y-%m-%d %H:%M:%S.%f')
        timestamp += timedelta(microseconds=count)  # Incrementing stored timestamp my a microsec to avoid duplicate keys
        return {'deviceid': device_id,
                'datatype': datatype,
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S.%f'),
                'min': min,
                'max': max,
                'avg': avg
        }
