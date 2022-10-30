IoT Cloud Processing Project for ACSE May 22 Batch
==================================================

Copyright GreatLearning

This project is an implementation of IoT data ingestion into AWS IoT Core
and processing of the data. Processing invilves two steps, viz. aggregation
of the data captured in AWS DynamoDB and processing based on rules for
generation of alerts.


Setting Up
----------

To setup the required infrastructure on AWS, configure your
access for aws client (aws configure) using valid access_key
and install the required AWS Python SDK:

$ pip3 install AWSIoTPythonSDK
$ pip3 install awsiotsdk
$ pip3 install boto3

Now setup the devices, dynamodb tables, IAM roles and rule engine
by running the following command:

$ python3 setup.py

Once the cloud infrastructure is ready, run the BedSideMonitor.py script
to generate the data. The following wapper scripts can be run for ease:

$ ./bsm_101.sh
$ ./bsm_102.sh

Both can be run from different terminals for simultaneaous data generation.
After more than a hour of data generation, run the aggregation and alerting
program as follows: 

$ python3 main.py


