from RawDataModel import RawDataModel
from AggregateDataModel import AggregateDataModel
from AlertDataModel import AlertDataModel


# Two device ids
DEVICE_ID_1 = 'BSM_G101'
DEVICE_ID_2 = 'BSM_G102'

# Start and end timestamps based on the data generated
START_TIMESTAMP = '2022-10-29 08:31:00.000000'
END_TIMESTAMP = '2022-10-29 08:36:00.000000'

RULES_CONFIG = '/rules.json'


raw_data_model = RawDataModel()
aggregate_data_model = AggregateDataModel()
alert_data_model = AlertDataModel(RULES_CONFIG)

# Aggregating items from raw model and then inserting them in the aggregate model
agg_data = raw_data_model.aggregate_by_datatype_and_minute(DEVICE_ID_1, START_TIMESTAMP, END_TIMESTAMP)
aggregate_data_model.insert(DEVICE_ID_1, agg_data)

agg_data = raw_data_model.aggregate_by_datatype_and_minute(DEVICE_ID_2, START_TIMESTAMP, END_TIMESTAMP)
aggregate_data_model.insert(DEVICE_ID_2, agg_data)

# Processing the rules on both devices

