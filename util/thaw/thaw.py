
# thaw.py
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
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers
import time

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')


region_name = config.get('aws', 'AwsRegionName')
vault_name = config.get('GLACIER', 'vault')
bucket_name = config.get('S3', 'results_bucket')
sqs_client = boto3.client('sqs', region_name=region_name)
glacier_client = boto3.client('glacier', region_name=region_name)
s3_client = boto3.client('s3', region_name=region_name)
queue_url = config.get('SQS', 'queueUrl')

# Add utility code here
def process_completed_jobs():
    restored_data = b''
    while True:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )

        for message in response.get('Messages', []):
            body = json.loads(message['Body'])
            message_dict = body.get('Message')
            data_dict = json.loads(message_dict)
            restore_job_id = data_dict.get('job_ids')
            job_id = data_dict.get('job_id')
            archive_id = data_dict.get('archive_id')
            user_id = data_dict.get('user_id')
            file_name = data_dict.get('file_name')
            status_code = None
            while status_code != 'Succeeded':
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/describe_job.html
                response_status = glacier_client.describe_job(vaultName=vault_name, jobId=restore_job_id)
                status_code = response_status['StatusCode']
                if status_code == 'InProgress':
                     print(f"Job status is {status_code}. Waiting for completion...")
                     time.sleep(60)

                elif status_code == 'Succeeded':
                     print("Job status is completed. Fetching the output...")
                     # Get the job output
                     # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
                     job_output = glacier_client.get_job_output(vaultName=vault_name, jobId=restore_job_id)

                     restored_data = job_output['body'].read()
                     break

                elif status_code == 'Failed':
                     print("Job failed. Please check the job details.")
                     break
                else:
                     print(f"Job status is {status_code}. Waiting for completion...")
                     time.sleep(60)
            # Move the object to S3
            file_name_correct = file_name.split(".")[0]
            s3_key = f'koyya/{user_id}/{job_id}/{file_name_correct}.annot.vcf'
            # https://stackoverflow.com/questions/40336918/how-to-write-a-file-or-data-to-an-s3-object-using-boto3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=restored_data
            )

            # Delete the archive from Glacier
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
            glacier_client.delete_archive(vaultName=vault_name, archiveId=archive_id)

            # Delete the message from the queue
            sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )

if __name__ == "__main__":
    process_completed_jobs()

### EOF