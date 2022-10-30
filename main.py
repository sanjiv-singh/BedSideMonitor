from RawDataModel import RawDataModel
from AggregateDataModel import AggregateDataModel
from AlertDataModel import AlertDataModel
import json


def load_config(file):
    with open(file) as f:
        config = json.loads(f.read())
    return config

if __name__ == '__main__':

    # Load config from file (json format)
    config = load_config("config.json")

    devices = config.get("devices")
    start_timestamp = config.get("start_timestamp")
    end_timestamp = config.get("end_timestamp")

    # Create the raw data model instances for raw, aggregate and alerts data
    raw_data_model = RawDataModel()
    aggregate_data_model = AggregateDataModel()
    alert_data_model = AlertDataModel(config.get("rules_file"))

    # Aggregating items from raw model and then inserting them in the aggregate model
    for device in devices:
        device_id = device.get("device_id")
        print(f'Aggregating data for device {device_id}')
        # Get aggregated data
        agg_data = raw_data_model.aggregate_by_datatype_and_minute(device_id, start_timestamp, end_timestamp)
        # Insert into agg_data table
        aggregate_data_model.insert(device_id, agg_data)

    # Processing the rules on both devices
    for device in devices:
        device_id = device.get("device_id")
        print(f'Processing rules for device {device_id}')
        alert_data_model.process_rules(device_id, start_timestamp, end_timestamp)



