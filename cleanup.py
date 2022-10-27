import boto3


default_table_name = 'bsm_data'
default_things = [
    'BSM_G101',
    'BSM_G102'
]

dynamodb = boto3.resource('dynamodb')
iot_client = boto3.client('iot')

def delete_tables():
    for table in dynamodb.tables.all():
        table.delete()

def delete_things():

    for thing in default_things:
        try:
            r_principals = iot_client.list_thing_principals(thingName=thing)
        except Exception as e:
            print("ERROR listing thing principals: {}".format(e))
            r_principals = {'principals': []}

        #print("r_principals: {}".format(r_principals))
        for arn in r_principals['principals']:
            cert_id = arn.split('/')[1]
            print("  arn: {} cert_id: {}".format(arn, cert_id))

            r_detach_thing = iot_client.detach_thing_principal(thingName=thing, principal=arn)
            print("  DETACH THING: {}".format(r_detach_thing))

            r_upd_cert = iot_client.update_certificate(certificateId=cert_id,newStatus='INACTIVE')
            print("  INACTIVE: {}".format(r_upd_cert))

            r_policies = iot_client.list_principal_policies(principal=arn)
            #print("    r_policies: {}".format(r_policies))

            for pol in r_policies['policies']:
                pol_name = pol['policyName']
                print("    pol_name: {}".format(pol_name))
                #policy_names[pol_name] = 1
                r_detach_pol = iot_client.detach_policy(policyName=pol_name,target=arn)
                print("    DETACH POL: {}".format(r_detach_pol))

            r_del_cert = iot_client.delete_certificate(certificateId=cert_id,forceDelete=True)
            print("  DEL CERT: {}".format(r_del_cert))
            del_thing = iot_client.delete_thing(thingName=thing)
            dep = iot_client.deprecate_thing_type(thingTypeName='BSM_01')
            del_thing_type = iot_client.delete_thing_type(thingTypeName='BSM_01')


if __name__ == '__main__':

    ans = input(f'Delete all tables (y/n)') or "n"
    if ans in ('y', 'Y'):
        delete_tables()

    ans = input(f'Delete all things (y/n)') or "n"
    if ans in ('y', 'Y'):
        delete_things()
    

