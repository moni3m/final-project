import json

import boto3
import pandas as pd
import pprint as pp
# import requests

bucket = 'data-eng-30-final-project-files'


def download_json_filenames(bucket_name: str, aws_prefix):
    # This function loads a list of object names within a given aws bucket
    # when provided with a specific key string
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    contents = paginator.paginate(Bucket=bucket_name, Prefix=aws_prefix)
    aws_files = []
    for page in contents:
        if "Contents" in page:
            for key in page["Contents"]:
                if ".json" and key["Key"].endswith(".json"):  # checks to see if files are in csv
                    keystring = key["Key"]
                    aws_files.append(keystring)
    return aws_files

