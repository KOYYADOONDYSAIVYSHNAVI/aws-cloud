#run.py

# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
import os
import boto3
import datetime
import json
from configparser import ConfigParser
from flask import session
from boto3.dynamodb.conditions import Key

config = ConfigParser()
# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the config file
config_file_path = os.path.join(script_dir, '../ann_config.ini')
config.read(config_file_path)


"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

def upload_to_s3(local_file, bucket_name, s3_key):
    s3_client = boto3.client('s3')
    s3_client.upload_file(local_file, bucket_name, s3_key)

def update_dynamodb_job(job_id, s3_key_results, s3_key_log):
     dynamodb = boto3.resource('dynamodb')
     table_name = config.get('DynamoDB','table_name')
     table = dynamodb.Table(table_name)
     completion_time = datetime.datetime.now()
     completion_time = int(round(completion_time.timestamp()))
     response = table.update_item(
          Key = {'job_id': job_id},
          UpdateExpression = 'SET s3_key_result_file = :results, s3_key_log_file = :log, s3_results_bucket = :bucket, completion_time = :comp_time, job_status = :status, user_id = :user_id',
           ExpressionAttributeValues={
            ':results': s3_key_results,
            ':log': s3_key_log,
            ':bucket' : s3_bucket,
            ':comp_time': completion_time,
            ':status': 'COMPLETED',
            ':user_id': user_id

        }

     )

     region_name = config.get('AWS', 'region_name')
     sns_client = boto3.client('sns', region_name = region_name)
     topic_arn_results_path = config.get('SNS', 'topic_arn_results')
     topic_arn_archive = config.get('SNS', 'topic_arn_archive')
     # https://stackoverflow.com/questions/35758924/how-do-we-query-on-a-secondary-index-of-dynamodb-using-boto3
     response1 = table.query(
            IndexName = 'user_id_index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
     user_roles = response1.get('Items')
     for item in response1.get('Items', []):
     # Check if the current item's job_id matches the desired job_id
           if item.get('job_id') == job_id:
                # Extract the user_role from the matching item
                user_role_extract = item.get('user_role')
                break

     message = {
          'job_id': job_id,
          'email' : email,
          's3_key': s3_key_results,
          'completion_time': completion_time

     }
     response = sns_client.publish(
          TopicArn = topic_arn_results_path,
          Message = json.dumps(message)
     )
     if user_role_extract == "free_user":
          response = sns_client.publish(
                TopicArn = topic_arn_archive,
                Message = json.dumps(message)
          )
          print("Notification published to archive SNS topic")
     elif user_role_extract == "premium_user":
          print("Notification not sent")

if __name__ == '__main__':
        # Call the AnnTools pipeline
        if len(sys.argv) > 1:
                input_file_name = sys.argv[2]
                user_id = input_file_name[8:44]
                email = sys.argv[3]
                input_file = input_file_name.split('/')[-1].split('.')[0]
                with Timer():
                        driver.run(input_file_name, 'vcf')
                # Upload the results file
                job_id = input_file_name.split('/')[-2]
                s3_bucket=config.get('S3', 'results_bucket')
                s3_folder = f'koyya/{user_id}/{job_id}/'
                input_file_base = os.path.dirname(input_file_name)
                results_file = f'/home/ec2-user/mpcs-cc/gas/ann/{input_file_base}/{input_file}.annot.vcf'
                s3_key_results = f'{s3_folder}{input_file}.annot.vcf'
                upload_to_s3(results_file, s3_bucket, s3_key_results)

                # Upload the log file
                log_file = f'/home/ec2-user/mpcs-cc/gas/ann/{input_file_base}/{input_file}.vcf.count.log'
                s3_key_log = f'{s3_folder}{input_file}.vcf.count.log'
                upload_to_s3(log_file, s3_bucket, s3_key_log)

                update_dynamodb_job(job_id, s3_key_results, s3_key_log)

                # clean up local job files
                os.remove(results_file)
                os.remove(log_file)
        else:
                print("A valid .vcf file must be provided as input to this program.")

### EOF
