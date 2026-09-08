"""
Deploys (creates or updates) the Federal Register Glue ETL job.

Run this from your local machine whenever you change federal_register_etl.py.
It uploads the script to S3 and points the Glue job definition at it.

Usage:
    python deploy_glue_job.py
"""

import os

import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SCRIPT_BUCKET = os.getenv("S3_SCRIPTS_BUCKET", "reg-doc-rag")
SCRIPT_KEY = "glue_scripts/federal_register_etl.py"
JOB_NAME = "federal-register-etl"
GLUE_ROLE_ARN = os.getenv("GLUE_ROLE_ARN")  # IAM role Glue assumes when running

RAW_S3_PATH = f"s3://{SCRIPT_BUCKET}/raw/federal_register/"
CURATED_S3_PATH = f"s3://{SCRIPT_BUCKET}/curated/federal_register/"
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "reg-doc-metadata")

s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)


def upload_script():
    local_path = os.path.join(os.path.dirname(__file__), "federal_register_etl.py")
    s3.upload_file(local_path, SCRIPT_BUCKET, SCRIPT_KEY)
    script_location = f"s3://{SCRIPT_BUCKET}/{SCRIPT_KEY}"
    print(f"Uploaded script to {script_location}")
    return script_location


def create_or_update_job(script_location):
    job_config = {
        "Role": GLUE_ROLE_ARN,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--job-language": "python",
            "--RAW_S3_PATH": RAW_S3_PATH,
            "--CURATED_S3_PATH": CURATED_S3_PATH,
            "--DYNAMODB_TABLE": DYNAMODB_TABLE,
            "--TempDir": f"s3://{SCRIPT_BUCKET}/glue_temp/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
        },
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "MaxRetries": 0,
        "Timeout": 60,  # minutes
    }

    existing_jobs = glue.get_jobs()["Jobs"]
    job_exists = any(j["Name"] == JOB_NAME for j in existing_jobs)

    if job_exists:
        glue.update_job(JobName=JOB_NAME, JobUpdate=job_config)
        print(f"Updated existing Glue job: {JOB_NAME}")
    else:
        glue.create_job(Name=JOB_NAME, **job_config)
        print(f"Created new Glue job: {JOB_NAME}")


def run_job_now():
    response = glue.start_job_run(JobName=JOB_NAME)
    print(f"Started job run: {response['JobRunId']}")
    return response["JobRunId"]


if __name__ == "__main__":
    if not GLUE_ROLE_ARN:
        raise SystemExit(
            "Set GLUE_ROLE_ARN in your .env — the IAM role Glue will assume "
            "when it runs (needs S3 read/write + DynamoDB write permissions)."
        )

    location = upload_script()
    create_or_update_job(location)

    trigger = input("Run the job now? (y/n): ").strip().lower()
    if trigger == "y":
        run_job_now()
