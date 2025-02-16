
# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile
import requests

s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))
bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
dynamo = boto3.resource('dynamodb')
annotations_table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
annotations_table = dynamo.Table(annotations_table_name)
topic_arn = app.config['AWS_SNS_JOB_REQUEST_TOPIC']
topic_arn_result = app.config['AWS_SNS_JOB_COMPLETE_TOPIC']
topic_arn_restore = app.config['AWS_SNS_JOB_RESTORE_TOPIC']
topic_arn_thaw = app.config['AWS_SNS_JOB_THAW_TOPIC']

from auth import get_profile

def upload_file_to_s3(uploaded_file):
    if uploaded_file:
        unique_id = str(uuid.uuid4())
        user_id = session['primary_identity']
        folder_prefix = f'koyya/{user_id}/'
        file_name = f"{folder_prefix}{unique_id}~{uploaded_file.filename}"

        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        # Check if the folder exists, if not create it
        if 'Contents' not in response:
            print("No folder")
            s3.put_object(Bucket=bucket_name, Key=folder_prefix)

        try:
            s3.upload_fileobj(uploaded_file, bucket_name, file_name)
            return True, file_name
        except Exception as e:
            print("An error occurred:", e)
            return False, None
    return False, None


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name,
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)

  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  try :
    job_id = create_annotation_job(s3_bucket = bucket_name, s3_key = s3_key)
    if job_id:
      return render_template('annotate_confirm.html', job_id=job_id)
    else:
      abort(500)
  except Exception as e:
    app.logger.error(f"Failed to create annotation job: {e}")
    abort(500)

def create_annotation_job(s3_bucket, s3_key):
  job_id = str(uuid.uuid4())
  user_id = s3_key.split('/')[1]
  submit_time = int(datetime.now().timestamp())
  job_status = "PENDING"
  file_name = s3_key.rsplit('~',1)[-1]
  user_role = session.get('role')
  email_id = session.get('email')
  data = {
      "job_id": job_id,
      "user_id": user_id,
      "input_file_name": file_name,
      "s3_inputs_bucket": s3_bucket,
      "s3_key_input_file": s3_key,
      "submit_time": submit_time,
      "job_status": job_status,
      "user_role": user_role
  }
  try:
      annotations_table.put_item(Item=data)
      sns_client = boto3.client('sns', region_name = app.config['AWS_REGION_NAME'])
      message = {
          "job_id": job_id,
          "user_id": user_id,
          "input_file_name": file_name,
          "s3_inputs_bucket": s3_bucket,
          "s3_key_input_file": s3_key,
          "submit_time": submit_time,
          "job_status": job_status,
          "user_role": user_role,
          "email_id": email_id

      }
      sns_client.publish(
          TopicArn = topic_arn,
          Message = json.dumps(message)

      )
      return job_id
  except Exception as e:
      print(f"Failed to store job data in DynamoDB: {str(e)}")
      return None

def query_user_jobs(user_id):
    try:
        # Query DynamoDB to retrieve jobs for the specified user
        # https://stackoverflow.com/questions/35758924/how-do-we-query-on-a-secondary-index-of-dynamodb-using-boto3
        response = annotations_table.query(
            IndexName = 'user_id_index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )

        # Extract the jobs from the response
        jobs = response.get('Items')
        job_ids = [job['job_id'] for job in jobs]

        return job_ids
    except Exception as e:
        # Handle any errors that may occur during the query
        app.logger.error(f"Error querying user jobs: {e}")
        return []


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():

  # Get list of annotations to display
  jobs = list_user_jobs()
  job_details = [query_job_details(job_id) for job_id in jobs]
  return render_template('annotations.html', annotations=job_details)

def list_user_jobs():
    # Retrieve user's jobs from DynamoDB
    user_id = session['primary_identity']
    job_ids = query_user_jobs(user_id)
    return job_ids

def query_job_details(job_id):
    try:
        # Query the DynamoDB table to retrieve the job details
        response = annotations_table.get_item(Key={'job_id': job_id})
        item = response.get('Item')

        # Check if the item exists and return it
        if item:
            submit_time = item.get('submit_time')
            if submit_time:
               item['submit_time'] = datetime.fromtimestamp(submit_time).strftime('%Y-%m-%d %H:%M:%S')
            job_status = item.get('job_status')
            if job_status == 'COMPLETED':
                complete_time = item.get('completion_time')
                if complete_time:
                    item['complete_time'] = datetime.fromtimestamp(complete_time).strftime('%Y-%m-%d %H:%M:%S')
                result_file_url = item.get('s3_key_result_file', None)
                item['result_file_url'] = result_file_url
                annotation_log = item.get('s3_key_log_file')
                item['annotation_log'] = annotation_log
            return item
        else:
            return None  # Job with the given ID not found
    except Exception as e:
        # Handle any errors that may occur during the query
        print(f"Error querying job details: {e}")
        return None
    
"""Display details of a specific annotation job
"""
@app.route('/annotations/<job_id>', methods=['GET'])
@authenticated
def job_details(job_id):
    # Retrieve job details from the database
    job = query_job_details(job_id)
    free_access_expired = False
    if job is None:
        abort(404)

    # Check if the job belongs to the authenticated user
    if job['user_id'] != session['primary_identity']:
        abort(403)
    if job['job_status'] == 'COMPLETED' and 's3_key_result_file' in job:
        if session.get('role') == 'premium_user':
            # Provide a direct download link for premium users
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
            result_file_url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'], 'Key': job['s3_key_result_file']},
                ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
            )

            job['result_file_url'] = result_file_url
        else:
            # Calculate time elapsed since job completion
            completion_time_str = job['completion_time']
            job_completion_time = datetime.fromtimestamp(completion_time_str)
            current_time = datetime.utcnow()
            time_elapsed = current_time - job_completion_time

            # Check if more than 5 minutes have passed since job completion
            if time_elapsed > timedelta(minutes=5):
                # Offer upgrade option for free users after 5 minutes
                free_access_expired = True
            else:
                # Provide a direct download link for free users within 5 minutes
                result_file_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'], 'Key': job['s3_key_result_file']},
                    ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
                )
                job['result_file_url'] = result_file_url

    # Render the annotation_details.html template with the job information
    return render_template('annotation_details.html', free_access_expired=free_access_expired, annotation=job)



@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  pass


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  job = query_job_details(id)
  if job is None:
    abort(404)

  # Check if the job belongs to the authenticated user
  if job['user_id'] != session['primary_identity']:
    abort(403)

  # Retrieve log file content from S3
  try:
    # https://medium.com/@rohitshrivastava87/2-how-to-read-data-from-s3-bucket-using-python-945324d73d61
    log_file_obj = s3.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=job['s3_key_log_file'])
    log_file_contents = log_file_obj['Body'].read().decode('utf-8')
  except ClientError as e:
    app.logger.error(f"Unable to retrieve log file from S3: {e}")
    abort(500)

  # Render the view_log.html template with the log file content
  return render_template('view_log.html', id=id, log_file_contents=log_file_contents)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    restore_archived_result_files(session['primary_identity'])

    # Display confirmation page
    return render_template('subscribe_confirm.html')
def restore_archived_result_files(user_id):
    response = annotations_table.query(
        IndexName='user_id_index',
        KeyConditionExpression=Key('user_id').eq(user_id)
    )
    
    items = response.get('Items', [])

    # Process each archived file
    for item in items:
        if 'results_file_archive_id' in item:
            archive_id = item['results_file_archive_id']
            job_id = item.get('job_id', str(uuid.uuid4()))
            file_name = item.get('input_file_name')
            sns_client = boto3.client('sns', region_name = app.config['AWS_REGION_NAME'])
            sns_client.publish(TopicArn = topic_arn_restore,  Message=json.dumps({'user_id': user_id, 'job_id':job_id, 'file_name':file_name}))

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html',
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF


