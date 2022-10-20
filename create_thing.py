import boto3
import json

default_thing_name = 'BSM_G101'
default_thing_type = 'BSM_01'
default_thing_group = 'Hospital'
default_policy_name = 'BSM'

def create_certificate(client, thing_name):
    resp = client.create_keys_and_certificate(setAsActive=True)
    data = json.loads(json.dumps(resp, sort_keys=False, indent=4))
    for element in data:
        if element == 'certificateArn':
            cert_arn = data['certificateArn']
        if element == 'keyPair':
            key_pair = data['keyPair']
    with open(f'certs/{thing_name}_public.key', 'w') as pubkey_file:
        pubkey_file.write(key_pair['PublicKey'])
    with open(f'certs/{thing_name}_private.key', 'w') as prikey_file:
        prikey_file.write(key_pair['PrivateKey'])
    with open(f'certs/{thing_name}_device.pem', 'w') as cert_file:
        cert_file.write(data['certificatePem'])

    resp = client.attach_policy(
		policyName=default_policy_name,
		target=cert_arn
    )
    resp = client.attach_thing_principal(
		thingName=thing_name,
		principal=cert_arn
    )

def create_thing(client, thing_name, thing_type_name, thing_group_name):
    props = {'thingTypeDescription': 'Bed Side Monitor for Critical Care Units in hospitals'}
    type_resp = client.create_thing_type(
		thingTypeName=thing_type_name,
		thingTypeProperties=props
    )
    props = {'thingGroupDescription': 'Group of all devices deployed in hospital'}
    group_resp = client.create_thing_group(
		thingGroupName=thing_group_name,
		thingGroupProperties=props
    )
    resp = client.create_thing(thingName=thing_name, thingTypeName=thing_type_name)
    resp = client.add_thing_to_thing_group(thingName=thing_name, thingGroupName=thing_group_name)
    create_certificate(client, thing_name)

if __name__ == '__main__':
    client = boto3.client('iot')
    iot_thing_name = input(f'Enter thing name ({default_thing_name}) : ') or default_thing_name
    iot_thing_type = input(f'Enter thing type ({default_thing_type}) : ') or default_thing_type
    iot_thing_group = input(f'Enter thing group ({default_thing_group}) : ') or default_thing_group
    create_thing(client, iot_thing_name, iot_thing_type, iot_thing_group)
