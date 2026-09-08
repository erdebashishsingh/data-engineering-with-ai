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

BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

def fetch_all_pages(url, base_params, page_size=1000):
    all_records = []
    page_number = 1
    while True:
        params = {**base_params, "page[size]": page_size, "page[number]": page_number}
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        records = payload["data"]
        all_records.extend(records)
        total_count = payload["meta"]["count"]
        print(f"Page {page_number}: got {len(records)} records ({len(all_records)}/{total_count})")
        if len(all_records) >= total_count or not records:
            break
        page_number += 1
        time.sleep(0.3)
    return all_records


# ---- This is where you actually pass the URL and params ----
all_rates = fetch_all_pages(
    url=f"{BASE_URL}/v1/accounting/od/rates_of_exchange",
    base_params={
        "fields": "record_date,country_currency_desc,exchange_rate",
        "sort": "-record_date",
    }
)

print(f"\nTotal fetched: {len(all_rates)} records")

# BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

# resp = requests.get(
#     f"{BASE_URL}/v2/accounting/od/debt_to_penny",
#     params={"sort": "-record_date", "page[size]": 20}
# )
# data = resp.json()["data"][0]
# print(f"Total public debt as of {data['record_date']}: ${float(data['tot_pub_debt_out_amt']):,.0f}")

# resp_rates_of_exchange = requests.get(
#     f"{BASE_URL}/v1/accounting/od/rates_of_exchange",
#     params={
#         "fields": "record_date,country_currency_desc,exchange_rate",  # WHICH columns
#         "sort": "-record_date",                                        # WHAT ORDER (newest first)
#         "page[size]": 100                                              # HOW MANY rows
#     }
# )
# print("Status code:", resp_rates_of_exchange.status_code)
# print("Raw response:", resp_rates_of_exchange.text[:500])  # first 500 chars

