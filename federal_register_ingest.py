"""
Federal Register API -> S3 ingestion script.

Pulls documents from the Federal Register API for a given date range,
handles pagination (max per_page=100, hard 400 error past 2000 results
so we partition by date range), and lands raw JSON in S3 partitioned
by publication date: s3://<bucket>/raw/federal_register/year=YYYY/month=MM/day=DD/

Usage:
    python federal_register_ingest.py --start 2026-08-01 --end 2026-08-31
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

BASE_URL = "https://www.federalregister.gov/api/v1/documents.json"
PER_PAGE = 100  # API max
MAX_RESULTS_PER_QUERY = 2000  # API hard-fails (400) beyond this
S3_BUCKET = os.getenv("S3_RAW_BUCKET", "your-bucket-name")
S3_PREFIX = "raw/federal_register"

s3 = boto3.client("s3")  # picks up credentials from `aws configure` automatically


def daterange_chunks(start_date, end_date, chunk_days=7):
    """
    Yield (chunk_start, chunk_end) date pairs. Federal Register can return
    well over 2000 results for broad date ranges, so we pull in weekly
    chunks to stay under the API's pagination ceiling.
    """
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def fetch_documents(start_date, end_date):
    """Fetch all documents published in [start_date, end_date], paginated."""
    all_docs = []
    page = 1

    while True:
        params = {
            "per_page": PER_PAGE,
            "page": page,
            "conditions[publication_date][gte]": start_date.isoformat(),
            "conditions[publication_date][lte]": end_date.isoformat(),
            "order": "oldest",
            "fields[]": [
                "document_number",
                "title",
                "type",
                "publication_date",
                "agencies",
                "abstract",
                "html_url",
                "pdf_url",
                "body_html_url",
            ],
        }

        resp = requests.get(BASE_URL, params=params, timeout=30)

        if resp.status_code == 404:
            # 404 here means "no matches for this query", not a broken endpoint
            break

        if resp.status_code == 400:
            # Likely exceeded the 2000-result pagination ceiling for this range.
            # Caller should use smaller date chunks.
            raise RuntimeError(
                f"400 error at page {page} for {start_date}..{end_date} — "
                "narrow the date range (pagination ceiling likely exceeded)."
            )

        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])

        if not results:
            break

        all_docs.extend(results)

        if len(all_docs) >= data.get("count", 0):
            break

        page += 1
        time.sleep(0.3)  # polite rate limiting

    return all_docs


def land_to_s3(documents, pub_date):
    """Write one JSON file per day to S3, partitioned by year/month/day."""
    if not documents:
        return

    key = (
        f"{S3_PREFIX}/year={pub_date.year}/month={pub_date.month:02d}/"
        f"day={pub_date.day:02d}/documents.json"
    )

    body = "\n".join(json.dumps(doc) for doc in documents)  # JSON Lines format

    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"  -> wrote {len(documents)} docs to s3://{S3_BUCKET}/{key}")


def run(start_date, end_date):
    for chunk_start, chunk_end in daterange_chunks(start_date, end_date):
        print(f"Fetching {chunk_start} to {chunk_end}...")
        docs = fetch_documents(chunk_start, chunk_end)
        print(f"  fetched {len(docs)} documents")

        # Group by actual publication_date since a chunk can span multiple days
        by_date = {}
        for doc in docs:
            pub_date_str = doc.get("publication_date")
            by_date.setdefault(pub_date_str, []).append(doc)

        for pub_date_str, docs_for_day in by_date.items():
            pub_date = datetime.strptime(pub_date_str, "%Y-%m-%d").date()
            land_to_s3(docs_for_day, pub_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    run(start, end)
