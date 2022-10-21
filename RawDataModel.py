from datetime import datetime
from typing import List
from Database import DynamoDB

class BSMRawData:

    _devicetype: str
    _value: float
    _timestamp: datetime

    def __init__(self, devicetype: str, value: float, timestamp: str):
        self._devicetype = devicetype
        self._value = value
        self._timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
    
    @property
    def devicetype(self):
        return self._devicetype
    
    @devicetype.setter
    def devicetype(self, devicetype):
        self._devicetype = devicetype
    
    @property
    def value(self):
        return self._value
    
    @value.setter
    def value(self, value):
        self._value = value
    
    @property
    def timestamp(self):
        return self._timestamp
    
    @timestamp.setter
    def timestamp(self, timestamp):
        if type(timestamp) == datetime:
            self._timestamp = timestamp
            return
        self._timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
    

class BSMRawDataSet:

    _data_set: List[BSMRawData]

    def __init__(self, start_time, end_time):
        db = DynamoDB('bsm_data')
        db.query(timestamp, start_time)



