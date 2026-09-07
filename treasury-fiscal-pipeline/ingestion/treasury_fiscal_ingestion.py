"""
Treasury Fiscal API -> S3 ingestion script.

Pulls documents from the Treasury Fiscal API for a given date range,
handles pagination (max per_page=100, hard 400 error past 2000 results
so we partition by date range), and lands raw JSON in S3 partitioned
by publication date: s3://<bucket>/raw/treasury_fiscal/year=YYYY/month=MM/day=DD/

Usage:
    python treasury_fiscal_ingest.py --start 2026-08-01 --end 2026-08-31
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

import requests

BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

resp = requests.get(
    f"{BASE_URL}/v2/accounting/od/debt_to_penny",
    params={"sort": "-record_date", "page[size]": 1}
)
data = resp.json()["data"][0]
print(f"Total public debt as of {data['record_date']}: ${float(data['tot_pub_debt_out_amt']):,.0f}")