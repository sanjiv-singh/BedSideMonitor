import json
import datetime

from Database import Database
from AggregateDataModel import AggregateDataModel


class AlertDataModel:
    
    def __init__(self, config_file):
        pass
        

    def process_rules(self, device_id, start_timestamp, end_timestamp):
        pass
            
    # This method should log the data in the designed database.  
    def _log_alert(self, device_id, rule_id, starting_timestamp, last_breach_type):
        pass


