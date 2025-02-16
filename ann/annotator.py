
import os
import uuid
import subprocess
import urllib.parse
import boto3
import json
from flask import Flask, request, jsonify, session
import boto3.exceptions
import botocore.exceptions
from configparser import ConfigParser


app = Flask(__name__)


config = ConfigParser()
config.read('ann_config.ini')
region_name = config.get('AWS', 'region_name')
s3_client = boto3.client('s3', region_name=region_name)
sqs_client = boto3.client('sqs', region_name=region_name)
queue_url = config.get('SQS', 'queue_url')


def update_job_status(job_id, status):
    try:
        dynamo = boto3.resource('dynamodb', region_name = region_name)
        table = config.get('DynamoDB','table_name')
        job_status_table = dynamo.Table(table)
        response = job_status_table.get_item(Key = {'job_id':job_id})
        current_status = response.get('Item', {}).get('job_status', None)
        if current_status == 'PENDING':
            job_status_table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET job_status = :status',
                ExpressionAttributeValues={':status': status}
            )
            return True
        else:
            print(f"Job status is not PENDING. Current status: {current_status}")
            return False
    except boto3.exceptions.Boto3Error as be:
        print(f"Boto3 error occured: {str(be)}")
        return False
    except Exception as e:
        print(f"Error : {e}       ")
        return False
while True:
    response = sqs_client.receive_message(
        QueueUrl = queue_url,
        MaxNumberOfMessages = 1,
        WaitTimeSeconds= 20
    )
    if 'Messages' in response:
        message = response['Messages'][0]
        body = message['Body']
        receipt_handle = message['ReceiptHandle']
        try:
            data = json.loads(body)
            message=data.get('Message')
            message_data = json.loads(message)
            job_id = message_data.get('job_id')
            user_id = message_data.get('user_id')
            email_id = message_data.get('email_id')
            input_file_name = message_data.get('input_file_name')
            if not input_file_name:
                raise ValueError("Input file is missing")
            s3_inputs_bucket = message_data.get('s3_inputs_bucket')
            s3_key_input_file = message_data.get('s3_key_input_file')
            job_id_directory = f'job_ids/{user_id}'
            if not os.path.exists(job_id_directory):
                os.makedirs(job_id_directory)
            job_directory = os.path.join(job_id_directory, job_id)
            if not os.path.exists(job_directory):
                os.makedirs(job_directory)
            local_file_path = os.path.join(job_directory, input_file_name)
            s3_client.download_file(s3_inputs_bucket, s3_key_input_file, local_file_path)

            # Execute the annotator script using subprocess
            annotate_path = "anntools/run.py"
            subprocess.Popen(["python", annotate_path, job_id, local_file_path, email_id])
            update_job_status(job_id, "RUNNING")
            sqs_client.delete_message(
                QueueUrl = queue_url,
                ReceiptHandle = receipt_handle
            )
        except botocore.exceptions.ClientError as ce:
            print(f"S3 file download error: {str(ce)}")
        except subprocess.CalledProcessError as se:
            print(f"Subprocess error occured: {str(se)}")
        except ValueError as ve:
            print(f"ValueError: {str(ve)}")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
    else:
        print("No messages received. Waiting...")

