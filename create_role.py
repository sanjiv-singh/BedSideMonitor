import boto3
import json
import time

default_role_name = 'BSM_role'
default_rule_name = 'BSM_DynamoDB'

assume_role_policy_document = json.dumps({
    "Version": "2012-10-17",
    "Statement": [
        {
        "Effect": "Allow",
        "Principal": {
            "Service": "iot.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
        }
    ]
})

iot_registration_policy_arn = 'arn:aws:iam::aws:policy/service-role/AWSIoTThingsRegistration'
iot_logging_policy_arn = 'arn:aws:iam::aws:policy/service-role/AWSIoTLogging'
iot_role_actions_policy_arn = 'arn:aws:iam::aws:policy/service-role/AWSIoTRuleActions'
dynamodb_policy_arn = 'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess'


def delete_role(client, iam_role_name):
    client.detach_role_policy(
        RoleName=iam_role_name,
        PolicyArn=iot_registration_policy_arn
    )
    client.detach_role_policy(
        RoleName=iam_role_name,
        PolicyArn=iot_logging_policy_arn
    )
    client.detach_role_policy(
        RoleName=iam_role_name,
        PolicyArn=iot_role_actions_policy_arn
    )
    client.detach_role_policy(
        RoleName=iam_role_name,
        PolicyArn=dynamodb_policy_arn
    )
    client.delete_role(RoleName=iam_role_name)

def create_role(client, iam_role_name):
    resp = client.create_role(
        RoleName=iam_role_name,
        AssumeRolePolicyDocument=assume_role_policy_document
    )
    role = resp.get('Role')
    resp = client.attach_role_policy(RoleName=iam_role_name, PolicyArn=iot_registration_policy_arn)
    resp = client.attach_role_policy(RoleName=iam_role_name, PolicyArn=iot_logging_policy_arn)
    resp = client.attach_role_policy(RoleName=iam_role_name, PolicyArn=iot_role_actions_policy_arn)
    resp = client.attach_role_policy(RoleName=iam_role_name, PolicyArn=dynamodb_policy_arn)
    return role.get('Arn')

def delete_rule(client, rule_name):
    resp = client.delete_topic_rule(ruleName=rule_name)

def create_rule(client, role_arn):
    pass
    response = client.create_topic_rule(
        ruleName='string',
        topicRulePayload={
            'sql': 'SELECT * FROM "iot/bsm"',
            'description': 'Rule to ingest device data to DynamoDB',
            'actions': [
                {
                    'dynamoDBv2': {
                        'roleArn': role_arn,
                        'putItem': {
                            'tableName': 'bsm_data'
                        }
                    }
                }
            ]
        }
    )

if __name__ == '__main__':
    iam_client = boto3.client('iam')
    iam_role_name = input(f'Enter role name ({default_role_name}) : ') or default_role_name
    delete_role(iam_client, iam_role_name)
    role_arn = create_role(iam_client, iam_role_name)
    time.sleep(5)
    iot_client = boto3.client('iot')
    delete_rule(iot_client, default_rule_name)
    create_rule(iot_client, role_arn)
