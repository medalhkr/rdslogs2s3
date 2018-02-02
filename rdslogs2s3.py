import os
import gzip
from datetime import datetime
import time
import boto3
from botocore.exceptions import ClientError

REGION = os.environ['REGION']
RDS_CLIENT = boto3.client('rds', region_name=REGION)
S3_CLIENT = boto3.client('s3')

def lambda_handler(event, context):
    rdslogs2s3(
        os.environ['RDS_INSTANCE'],
        os.environ['LOG_NAME'],
        os.environ['S3_BUCKET'],
    )


def copy_log(instance, log_file_name, s3_bucket, s3_prefix):
    read_log_line_num = 2000
    tmp_file_name = '/tmp/tmp_log_file_{}'.format(log_file_name.replace('/', '-'))
    with gzip.open(tmp_file_name, 'ab') as f:
        marker = '0'
        while True:
            log = RDS_CLIENT.download_db_log_file_portion(DBInstanceIdentifier=instance, LogFileName=log_file_name, NumberOfLines=read_log_line_num, Marker=marker)
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
        S3_CLIENT.upload_file(tmp_file_name, s3_bucket, put_log_name)
        print('put s3://{0}/{1}'.format(s3_bucket, put_log_name))
    except ClientError as e:
        print("Unexpected error: {}".format(e))
        return False
    finally:
        os.remove(tmp_file_name)

    return True

def fetch_updated_at(s3_bucket, filename):
    try:
        obj = S3_CLIENT.get_object(Bucket=s3_bucket, Key=filename)
        return int(str(object=obj['Body'].read(), encoding='utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return 0
        raise e

def rdslogs2s3(rds_instance, log_name, s3_bucket):
    s3_prefix = 'db_{0}/'.format(rds_instance)
    timestamp_filename = '{0}{1}.updated_at'.format(s3_prefix, log_name)
    last_checked = fetch_updated_at(s3_bucket, timestamp_filename)

    try:
        db_logs = RDS_CLIENT.describe_db_log_files(
            DBInstanceIdentifier=rds_instance,
            FilenameContains=log_name + '.', # specify rotated log file
            FileLastWritten=last_checked
        )
    except ClientError as e:
        print("Unexpected error: {}".format(e))
        return False

    for db_log in db_logs['DescribeDBLogFiles']:
        log_file_name = db_log['LogFileName']
        last_checked = db_log['LastWritten']
        copy_log(rds_instance, log_file_name, s3_bucket, s3_prefix)

    S3_CLIENT.put_object(Bucket=s3_bucket, Key=timestamp_filename, Body=last_checked)

    return True

if __name__ == "__main__":
    lambda_handler(None, None)
