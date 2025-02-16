# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

# Description of archive process
## Message Processing - 

### Job Completion in run.py:
When a job completes, run.py updates DynamoDB and checks the user's role.
If the user is a free_user, it sends an SNS notification to trigger archiving.

### Archiving Trigger in archive.py:
The archive.py script, upon receiving an SQS message triggered by the SNS notification, processes the message to extract job details - job_id, s3_key, completion_time.
It waits for at least 5 minutes after job completion to ensure any final processing is complete.
If 5 minutes have passed, then it archives the file uploaded to the glacier vault (archive_to_glacier(s3_bucket, s3_key, job_id)).
It first downloads the file from the s3 bucket and uploads to glacier vault and then the archive id is updated in dyanmodb with column as results_file_archive_id.
Finally it deletes the file from the S3 bucket, ensuring efficient storage management.

# Description of restore process
### Subscription and Role Update in views.py:
User subscribes then the user_role updated to "Premium".
Query the DynamoDB table to retrieve items related to the user, extracting job_id, user_id, and file_name.
Publishes SNS restore notification.

### Restore Process in restore.py:
Poll SQS then Retrieve archive details from DynamoDB.
Initiate Glacier retrieval (expedited first, then standard if necessary).
Send an SNS notification with the details (archive_id, user_id, job_id, file_name) to trigger the thaw process.

### Thaw Process in thaw.py:
Poll SQS, For each message, extract restore_job_id, job_id, archive_id, user_id, and file_name.
Then monitors job status.
Retrieves job output from Glacier.
Move file to S3 bucket.
Cleanup Glacier and SQS.

### File Download:
Premium user downloads the restored file from S3.