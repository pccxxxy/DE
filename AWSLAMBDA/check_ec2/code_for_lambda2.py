import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    ec2 = boto3.client('ec2', region_name=event['region'])
    instances = [event['instance_id']]
    inst = ec2.describe_instances(InstanceIds=[instances[0]])
    state = inst['Reservations'][0]['Instances'][0]['State']['Name']
    print(state)
    if(state == "stopped"):
        ec2.start_instances(InstanceIds=instances)
        print('Started your instances: ' + instances[0])
    else:
        print(instances[0] + ' is already started')
