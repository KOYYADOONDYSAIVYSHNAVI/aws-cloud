# AWS

A secure, scalable web framework built on **Flask** that supports user authentication via **Globus Auth**, modular UI components, and seamless integration with AWS services for job processing, archiving, and restoration. Styled with **Bootstrap** for a clean user experience.

---

## 📁 Directory Structure

* `/web` - Core web application logic
* `/ann` - Annotation processing modules
* `/util` - Utility scripts (notifications, archival, restore)
* `/aws` - AWS configurations and user data files


---

## ⚙️ Job Processing & Archiving

### 📝 Job Completion (`run.py`)
- Updates job metadata in **DynamoDB** upon completion.
- If the job belongs to a `free_user`, sends an **SNS** notification to trigger the archival pipeline.

### 📦 Archiving Workflow (`archive.py`)
- Listens to **SQS** messages triggered by SNS.
- Extracts job metadata: `job_id`, `s3_key`, `completion_time`.
- Waits 5 minutes post-completion before:
  - Downloading the file from **S3**
  - Uploading it to **Amazon Glacier**
  - Storing the `archive_id` in DynamoDB under `results_file_archive_id`
  - Deleting the file from S3 to free up space

---

## 🔄 File Restoration Flow

### 💼 Role Upgrade & Subscription (`views.py`)
- Upgrades user to "Premium" upon subscription.
- Queries **DynamoDB** for the user’s archived jobs.
- Publishes an SNS message to begin the restore process.

### ♻️ Restore Phase (`restore.py`)
- Listens for SQS messages containing restore requests.
- Fetches archive details from DynamoDB.
- Tries **Expedited** retrieval from Glacier, falls back to **Standard** if needed.
- Sends an SNS message with restore metadata.

### 🔓 Thawing Archived Files (`thaw.py`)
- Monitors SQS for thaw requests.
- Extracts and tracks restore job status.
- Retrieves the file from Glacier, moves it back to **S3**.
- Cleans up Glacier and deletes the processed SQS message.

---

## ⬇️ File Access

Premium users can directly download their restored results from the S3 bucket via the web app interface.

---

## 🛠 Tech Stack

- **Backend**: Python, Flask  
- **Authentication**: Globus Auth  
- **Cloud Services**: AWS S3, Glacier, SNS, SQS, DynamoDB  
- **Frontend**: Jinja2 Templates, Bootstrap  
- **Database**: AWS DynamoDB

