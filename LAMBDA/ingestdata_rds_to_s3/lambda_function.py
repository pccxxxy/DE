from asyncio.log import logger
from multiprocessing.reduction import sendfds
import sys
import logging
import pymysql
import os
import pandas as pd
import boto3
from datetime import datetime
import csv

'''
1 Pre-config RDS information in SSM
2 Setup permission for S3 and SSM in IAM role
2.1 AmazonS3FullAccess
2.2 AmazonSSMReadOnlyAccess
3 Setup environment variable for BUCKET and TABLE
'''

bucket = os.environ['BUCKET']
ssm = boto3.client('ssm')
host = ssm.get_parameter(Name='/RDS/HOST', WithDecryption=False)['Parameter']['Value']
user = ssm.get_parameter(Name='/RDS/USER', WithDecryption=True)['Parameter']['Value']
password = ssm.get_parameter(Name='/RDS/PASSWORD', WithDecryption=True)['Parameter']['Value']
db_name = ssm.get_parameter(Name='/RDS/DB', WithDecryption=False)['Parameter']['Value']
table_name = os.environ['TABLE']

s3_client = boto3.client('s3')

try:
    conn = pymysql.connect(host = host, user = user, passwd = password, db = db_name, connect_timeout = 5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexcepted error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded.")
def handler(event, context):
    query_stmt = f'select * from {table_name};'
    try:
        with conn.cursor() as cur:
            cur.execute(query_stmt)
            rows = cur.fetchall()
            if not rows:
                return None
            data = pd.DataFrame(rows)
            data.columns = [desc[0] for desc in cur.description]
            file_key = 'rds_extraction/' + datetime.now().strftime("%Y/%m/%d/%H/%M/%S") + f'/{table_name}.csv.gz'
            local_file = '/tmp/' + file_key.replace('/', '-')
            data.to_csv(local_file, index=False, encoding='utf-8', compression='gzip', quoting=csv.QUOTE_NONNUMERIC)
            upload_file(bucket, file_key, local_file)
    except Exception as err:
        logger.error(f"ERROR: Unexcepted error: Extraction failed when running {query_stmt} in RDS MySQL instance")
        logger.error(err)

def upload_file(bucket, key_name, local_file):
    s3_client.upload_file(local_file, bucket, key_name)
    logger.info(f"SUCCESS: Uploaded {local_file} to {bucket} as {key_name}")
