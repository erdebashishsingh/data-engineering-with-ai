"""
Creates (or updates) a Glue Database + Crawler for the curated Federal
Register data, then runs the crawler and polls until it finishes.

Once the crawler completes, a queryable table will exist in the Glue
Data Catalog — visible in Athena, Redshift Spectrum, and the Glue console.

Usage:
    python deploy_crawler.py
"""

import os
import time

import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SCRIPT_BUCKET = os.getenv("S3_SCRIPTS_BUCKET", "your-bucket-name")
GLUE_ROLE_ARN = os.getenv("GLUE_ROLE_ARN")

DATABASE_NAME = "reg_doc_rag_db"
CRAWLER_NAME = "federal-register-curated-crawler"
CURATED_S3_PATH = f"s3://{SCRIPT_BUCKET}/curated/federal_register/"

glue = boto3.client("glue", region_name=REGION)


def create_database():
    try:
        glue.create_database(
            DatabaseInput={
                "Name": DATABASE_NAME,
                "Description": "Catalog for reg-doc-rag curated regulatory documents",
            }
        )
        print(f"Created database: {DATABASE_NAME}")
    except glue.exceptions.AlreadyExistsException:
        print(f"Database already exists: {DATABASE_NAME}")


def create_or_update_crawler():
    crawler_config = {
        "Role": GLUE_ROLE_ARN,
        "DatabaseName": DATABASE_NAME,
        "Targets": {"S3Targets": [{"Path": CURATED_S3_PATH}]},
        # Only add new partitions/columns automatically; never delete
        # existing table metadata just because a run doesn't see every file.
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG",
        },
    }

    existing = glue.get_crawlers()["Crawlers"]
    exists = any(c["Name"] == CRAWLER_NAME for c in existing)

    if exists:
        glue.update_crawler(Name=CRAWLER_NAME, **crawler_config)
        print(f"Updated existing crawler: {CRAWLER_NAME}")
    else:
        glue.create_crawler(Name=CRAWLER_NAME, **crawler_config)
        print(f"Created new crawler: {CRAWLER_NAME}")


def run_crawler_and_wait(poll_seconds=15):
    glue.start_crawler(Name=CRAWLER_NAME)
    print(f"Started crawler: {CRAWLER_NAME}")

    while True:
        response = glue.get_crawler(Name=CRAWLER_NAME)
        state = response["Crawler"]["State"]  # READY | RUNNING | STOPPING
        print(f"  crawler state: {state}")

        if state == "READY":
            last_run = response["Crawler"].get("LastCrawl", {})
            status = last_run.get("Status", "UNKNOWN")
            print(f"Crawler finished. Last run status: {status}")
            if status == "FAILED":
                print(f"  error: {last_run.get('ErrorMessage', 'no details')}")
            break

        time.sleep(poll_seconds)


def show_discovered_tables():
    tables = glue.get_tables(DatabaseName=DATABASE_NAME)["TableList"]
    print(f"\nTables in {DATABASE_NAME}:")
    for t in tables:
        cols = [c["Name"] for c in t["StorageDescriptor"]["Columns"]]
        partitions = [p["Name"] for p in t.get("PartitionKeys", [])]
        print(f"  - {t['Name']}")
        print(f"      columns: {cols}")
        print(f"      partitions: {partitions}")


if __name__ == "__main__":
    if not GLUE_ROLE_ARN:
        raise SystemExit("Set GLUE_ROLE_ARN in your .env first.")

    create_database()
    create_or_update_crawler()
    run_crawler_and_wait()
    show_discovered_tables()
