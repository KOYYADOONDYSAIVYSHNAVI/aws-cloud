#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import time
import json
import boto3

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
from boto3.dynamodb.conditions import Key
config = SafeConfigParser(os.environ)
config.read('restore_config.ini')


region_name = config.get('aws', 'AwsRegionName')
vault_name = config.get('GLACIER', 'vault')
sqs_client = boto3.client('sqs', region_name = region_name)
sns_client = boto3.client('sns', region_name = region_name)
queue_url = config.get('SQS','queueUrl')
topic_arn_thaw = config.get('SNS','topic_arn_thaw')
glacier_client = boto3.client('glacier', region_name = region_name)


dynamodb = boto3.resource('dynamodb', region_name=region_name)
table_name = config.get('DynamoDB','table')
table = dynamodb.Table(table_name)

def get_archived_files(user_id):
    # # https://stackoverflow.com/questions/35758924/how-do-we-query-on-a-secondary-index-of-dynamodb-using-boto3
    response = table.query(
        IndexName='user_id_index',
        KeyConditionExpression=Key('user_id').eq(user_id)
    )
    items = response.get('Items', [])

    # Extract the archived_results_file from each item
    archived_files = [item['results_file_archive_id'] for item in items if 'results_file_archive_id' in item]
    return archived_files
# Add utility code here
def initiate_glacier_retrieval(archive_id, user_id, job_id, file_name):
    try:
        # Attempt expedited retrieval first
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
        response = glacier_client.initiate_job(
            vaultName=vault_name,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': 'Expedited'
            }
        )
    # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/exceptions/InsufficientCapacityException.html
    except glacier_client.exceptions.InsufficientCapacityException:
        # Fallback to standard retrieval
        response = glacier_client.initiate_job(
            vaultName=vault_name,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': 'Standard'
            }
        )

    job_ids = response['jobId']
    # Send message to SQS with job details
    sns_client.publish(
        TopicArn=topic_arn_thaw,
        Message=json.dumps({
            'archive_id': archive_id,
            'user_id': user_id,
            'job_ids': job_ids,
            'file_name' : file_name,
            'job_id': job_id
        })
    )

def process_initiation_messages():
    while True:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
        for message in response.get('Messages', []):
            receipt_handle = message.get('ReceiptHandle')
            body = json.loads(message['Body'])
            message = body.get('Message')
            data_dict = json.loads(message)
            user_id = data_dict.get("user_id")
            job_id = data_dict.get("job_id")
            file_name = data_dict.get("file_name")

            # Query database for archived files of the user
            archived_files = get_archived_files(user_id)  # Implement this function

            for archive in archived_files:
                initiate_glacier_retrieval(archive, user_id, job_id, file_name)

            # Delete the message from the queue
            sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )

if __name__ == "__main__":
    process_initiation_messages()

### EOF