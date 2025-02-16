# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json
import time
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

# Add utility code here

# Load configuration

region = config['aws']['AwsRegionName']
sqs_queue_name = config['sqs1']['queue_name']
dynamodb_table_name = config['dynamodb']['table_name']
s3_bucket_name = config['s3']['bucket_name']
glacier_vault_name = config['glacier']['vault_name']

# AWS clients
sqs = boto3.client('sqs', region_name=region)
dynamo = boto3.resource('dynamodb', region_name=region)
s3 = boto3.client('s3', region_name=region)
glacier = boto3.client('glacier', region_name=region)
queue_url = config['sqs']['QueueUrl']

def archive_to_glacier(s3_bucket, s3_key, job_id):
    try:
        # Download the S3 object
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
        s3_object = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        s3_data = s3_object['Body'].read()

        # Upload to Glacier
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
        response = glacier.upload_archive(vaultName=glacier_vault_name, body=s3_data)
        archive_id = response['archiveId']

        # Update DynamoDB with the Glacier archive ID
        # https://stackoverflow.com/questions/55256127/update-item-in-dynamodb
        table = dynamo.Table(dynamodb_table_name)
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET results_file_archive_id = :archive_id',
            ExpressionAttributeValues={':archive_id': archive_id}
        )

        # Delete the S3 object after archiving
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
        s3.delete_object(Bucket=s3_bucket, Key=s3_key)

        print(f"Successfully archived job {job_id} to Glacier.")
    except ClientError as e:
        print(f"Failed to archive job {job_id}: {e}")


def process_messages():
    first_completion_time = {}
    while True:
        # Receive a message from SQS
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )

        messages = response.get('Messages', [])
        for message in messages:
            try:
                body = json.loads(message['Body'])
                message_data = json.loads(body['Message'])
                job_id = message_data['job_id']
                s3_key = message_data['s3_key']
                completion_time = datetime.fromtimestamp(message_data['completion_time'])
                # Check if the completion time for this job has been recorded before
                if job_id not in first_completion_time:
                    first_completion_time[job_id] = completion_time  # Record the first completion time
                if first_completion_time and (datetime.utcnow() - first_completion_time[job_id]) > timedelta(minutes=5):
                    archive_to_glacier(s3_bucket_name, s3_key, job_id)
                    # Delete the message from the queue after processing
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
                else:
                    time.sleep(60)
            except Exception as e:
                print(f"Failed to process message: {e}")

if __name__ == '__main__':
    process_messages()

### EOF
