import os
import requests
import boto3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from botocore.exceptions import ClientError

# ---- CONFIG ----
BLS_SUBDIRS = [
    "cu/",  # Consumer Price Index
    "ap/",  # Average Prices
    "ce/",  # Consumer Expenditure
]

BASE_URL = "https://download.bls.gov/pub/time.series/"
S3_BUCKET = "dataquestrichardcarter"
S3_PREFIX = "/"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

s3 = boto3.client('s3')


def get_files_from_subdir(subdir_url):
    try:
        response = requests.get(subdir_url, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Failed to access {subdir_url}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    files = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith(".txt"):
            files.append((href, urljoin(subdir_url, href)))
    return files


def s3_file_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        raise


def get_s3_keys(bucket, prefix=""):
    keys = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.add(obj['Key'])
    return keys


def sync():
    print("Starting sync...")
    existing_keys = get_s3_keys(S3_BUCKET, S3_PREFIX)
    new_keys = set()

    for subdir in BLS_SUBDIRS:
        subdir_url = urljoin(BASE_URL, subdir)
        files = get_files_from_subdir(subdir_url)
        print(f"Found {len(files)} files in {subdir}")

        for filename, file_url in files:
            s3_key = os.path.join(S3_PREFIX, subdir, filename)
            new_keys.add(s3_key)

            if s3_key in existing_keys:
                print(f"Exists: {s3_key} — skip")
                continue

            print(f"Downloading: {file_url}")
            resp = requests.get(file_url, headers=HEADERS)
            resp.raise_for_status()

            print(f"Uploading: {s3_key}")
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=resp.content
            )

    # Delete stale files
    for key in existing_keys - new_keys:
        print(f"Deleting stale file: {key}")
        s3.delete_object(Bucket=S3_BUCKET, Key=key)

    print("Sync complete.")


if __name__ == "__main__":
    sync()
