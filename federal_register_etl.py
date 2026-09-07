"""
AWS Glue ETL job (PySpark).

Reads raw, date-partitioned Federal Register JSON from S3 (landed by
ingestion/federal_register_ingest.py), flattens/cleans it, writes curated
Parquet back to S3, and writes lightweight metadata records to DynamoDB
for fast lookups later in the pipeline.

This file is meant to be uploaded to S3 and run AS A GLUE JOB, not locally.
See glue_jobs/deploy_glue_job.py for how to create/update the job in AWS.
"""

import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, concat_ws, explode_outer, lit
from pyspark.sql.types import StringType

# --- Job boilerplate -------------------------------------------------------
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "RAW_S3_PATH", "CURATED_S3_PATH", "DYNAMODB_TABLE"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_S3_PATH = args["RAW_S3_PATH"]          # e.g. s3://your-bucket/raw/federal_register/
CURATED_S3_PATH = args["CURATED_S3_PATH"]  # e.g. s3://your-bucket/curated/federal_register/
DYNAMODB_TABLE = args["DYNAMODB_TABLE"]    # e.g. reg-doc-metadata

# --- Read raw JSON Lines -----------------------------------------------------
raw_df = spark.read.json(RAW_S3_PATH)

# --- Clean / flatten ----------------------------------------------------------
# `agencies` is a nested array of structs in the API response; flatten to a
# comma-separated string so it's easy to query in Athena/Redshift downstream.
cleaned_df = (
    raw_df.withColumn("agency_names", explode_outer(col("agencies.name")))
    .groupBy(
        "document_number",
        "title",
        "type",
        "publication_date",
        "abstract",
        "html_url",
        "pdf_url",
    )
    .agg(concat_ws(", ", col("agency_names")).alias("agencies"))
    .withColumn("source", lit("federal_register"))
)

# --- Write curated Parquet, partitioned by publication date --------------------
cleaned_df = cleaned_df.withColumn("year", col("publication_date").substr(1, 4))
cleaned_df = cleaned_df.withColumn("month", col("publication_date").substr(6, 2))

cleaned_df.write.mode("append").partitionBy("year", "month").parquet(CURATED_S3_PATH)

# --- Populate DynamoDB metadata table -------------------------------------------
# Small enough to do row-by-row via boto3; for larger volumes switch to
# Glue's DynamoDB connector or an EMR-based bulk writer instead.
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)

for row in cleaned_df.select(
    "document_number", "title", "publication_date", "agencies", "source"
).toLocalIterator():
    table.put_item(
        Item={
            "document_id": row["document_number"],
            "title": row["title"] or "",
            "publication_date": row["publication_date"] or "",
            "agencies": row["agencies"] or "",
            "source": row["source"],
        }
    )

job.commit()
