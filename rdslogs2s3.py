#!/bin/env python
# coding=utf-8
#
import sys
import os
import gzip
from datetime import datetime
import time
import boto3
from botocore.exceptions import ClientError

REGION = os.environ['REGION']
rds_client = boto3.client('rds', region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION)

def lambda_handler(event, context):
    rdslogs2s3(
        os.environ['RDS_INSTANCE'],
        os.environ['LOG_NAME'],
        os.environ['S3_BUCKET'],
    )

def copy_log(instance, log_file_name, s3_bucket, s3_prefix):
    read_log_line_num = 2000
    tmp_file_name = '/tmp/tmp_log_file_{}'.format(log_file_name)
    with gzip.open(tmp_file_name, 'ab') as f:
        marker = '0'
        while True:
            log = rds_client.download_db_log_file_portion(DBInstanceIdentifier=instance, LogFileName=log_file_name, NumberOfLines=read_log_line_num, Marker=marker)
            if not log['LogFileData']:
                break
            if "[Your log message was truncated]" in log['LogFileData']:
                read_log_line_num -= int(read_log_line_num * 0.1)
                print("found `truncated` message. retry line num, {}".format(read_log_line_num))
                continue
            f.write(log['LogFileData'].encode('utf-8'))
            marker = log['Marker']

    try:
        put_log_name = '{0}{1}.gz'.format(s3_prefix, log_file_name)
        s3_client.upload_file(tmp_file_name, s3_bucket, put_log_name)
        print('put s3://{0}/{1}'.format(s3_bucket, put_log_name))
    except ClientError as e:
        print("Unexpected error: {}".format(e))
        return False
    finally:
        os.remove(tmp_file_name)

    return True

def fetch_updated_at(s3_bucket, filename):
    try:
        obj = s3_client.get_object(Bucket=s3_bucket, Key=filename)
        return str(object=obj['Body'].read(), encoding='utf-8')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return '0'
        raise e

def rdslogs2s3(RDS_INSTANCE, LOG_NAME, S3_BUCKET):
    s3_prefix = 'db{0}/{1}/'.format(RDS_INSTANCE, LOG_NAME)
    timestamp_filename = s3_prefix + 'updated_at'

    try:
        db_logs = rds_client.describe_db_log_files(
            DBInstanceIdentifier=RDS_INSTANCE,
            FilenameContains=LOG_NAME,
            FileLastWritten=fetch_updated_at(S3_BUCKET, timestamp_filename)
        )
    except ClientError as e:
        print("Unexpected error: {}".format(e))
        return False

    for db_log in db_logs['DescribeDBLogFiles']:
        log_file_name = db_log['LogFileName']
        copy_log(RDS_INSTANCE, log_file_name, S3_BUCKET, s3_prefix)

    checked_timestamp = int(time.mktime(datetime.now().timetuple())) * 1000
    s3_client.put_object(Bucket=S3_BUCKET, Key=timestamp_filename, Body=str(checked_timestamp))

    return True

if __name__ == "__main__":
    lambda_handler(None, None)
