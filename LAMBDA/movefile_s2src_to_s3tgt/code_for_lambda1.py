import json
import boto3
from datetime import datetime
#import os

s3 = boto3.resource('s3')
today = datetime.now()
date_time = today.strftime("%Y-%m-%d-%H-%M-%S")
#dest_bucket = os.environ['DEST_BUCKET']

def lambda_handler(event, context):
    print(event)
    records = event['Records']
    for record in records:
        src_bucket = record['s3']['bucket']['name']
        src_key = record['s3']['object']['key']
        copy_source = {
            'Bucket': src_bucket,
            'Key': src_key
        }
        dest_bucket = 'jrbutbucketdest'
        dest_key = f'source/{date_time}/' + src_key.split('/')[-1]
        print(dest_key)
        s3.meta.client.copy(copy_source, dest_bucket, dest_key)