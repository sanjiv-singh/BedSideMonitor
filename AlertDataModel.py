import json
from Database import Database
from AggregateDataModel import AggregateDataModel


class AlertDataModel:
    
    def __init__(self, config_file):

        # Open config file and read the alert rules
        with open(config_file) as f:
            data = f.read()
        config = json.loads(data)
        self._rules = config.get('rules')
        # Initialize two additional attibutes for indicating
        # the number of continuous breaches and the time of first breach
        for rule in self._rules:
            rule['breach_count'] = 0
            rule['breach_time'] = None
        
        # Initialise the aggregate data model for reading aggregate values
        self._agg_db = AggregateDataModel()

        # Initialise the alert data model for storing alert data
        self._alert_db = Database('bsm_alerts', 'deviceid', 'timestamp')
        
    def process_rules(self, device_id, start_timestamp, end_timestamp):

        # Fetch the aggregate date over the time interval
        data = self._agg_db.fetch(device_id, start_timestamp, end_timestamp)

        # Now loop over the fetched aggregate data to find out breaches
        for record in data:

            # Values in data record
            data_type = record.get('datatype')
            timestamp = record.get('timestamp')
            avg_value = record.get('avg')

            # Values specified in rule for the given datatype
            # Assuming there is a single rule for each datatype (sensor type)
            # We take the first rule pertaining to that datatype
            rule = [rule for rule in self._rules if data_type == rule['datatype']][0]
            avg_min = rule.get('avg_min')
            avg_max = rule.get('avg_max')

            # Compare data and rule values
            breach_type = None
            if avg_value < avg_min:
                breach_type = 'min'
            if avg_value > avg_max:
                breach_type = 'max'
            if breach_type:
                # Save the timestamp and breach_type in case only if it is a first breach.
                # This will give us the first instance of breach as required by  Reqmt 3b 
                if rule['breach_count'] == 0:
                    rule['first_breach_time'] = timestamp
                    rule['breach_type'] = breach_type
                # Increment the breach count in case of breach
                rule['breach_count'] += 1
            else:
                # If none of the rules are breached in this iteration,
                # reset the breach count to zero and remove the
                # breach_type and first_breach_time attributes from the rule dict
                rule["breach_count"] = 0
                _ = rule.pop("first_breach_time", None)
                _ = rule.pop("breach_type", None)
            # Check if an alert condition has been reached,
            # i.e., whether no of continuous breaches equals
            # or exceeds trigger_count
            breached_rule = self._check_alert_condition()

            # If any rule has been breached as per condition
            # print data to output and store in alerts table
            if breached_rule:
                self._log_alert(device_id, breached_rule["rule_id"],
                        breached_rule["first_breach_time"], breached_rule["datatype"], breached_rule["breach_type"])
                
                # After logging / saving, reset the breach count to zero and
                # remove the first_breach_time
                breached_rule["breach_count"] = 0
                breached_rule.pop("first_breach_time", None)
            
    # This method should log the data in the designed database.  
    def _log_alert(self, device_id, rule_id, starting_timestamp, breach_datatype, breach_type):

        msg = f'Alert for device_id {device_id} on rule {rule_id} starting at ' + \
            f'{starting_timestamp} with breach type {breach_type} for data type {breach_datatype}'
        print(msg)

        alert_item =  {
            "deviceid": device_id,
            "rule_id": rule_id,
            "timestamp": starting_timestamp,
            "breach_type": breach_type,
            "datatype": breach_datatype
        }
        self._alert_db.put_item(alert_item)

    def _check_alert_condition(self):
        
        # Iterate over all rules and check whether any rule has been breached
        # continuously for >= the specified trigger count. If yes, return the
        # breached rule. Otherwise return None
        for rule in self._rules:
            if rule["breach_count"] >= rule["trigger_count"]:
                return rule
        return None

