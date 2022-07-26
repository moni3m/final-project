import json

import boto3
import pandas as pd
import pprint as pp
# import requests

bucket = 'data-eng-30-final-project-files'


def download_json_filenames(bucket_name: str, aws_prefix, keyword='json'):
    # This function loads a list of object names within a given aws bucket
    # when provided with a specific key string
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    contents = paginator.paginate(Bucket=bucket_name, Prefix=aws_prefix)
    aws_files = []
    for page in contents:
        if "Contents" in page:
            for key in page["Contents"]:
                if keyword and key["Key"].endswith(keyword):  # checks to see if files are in csv
                    aws_files.append(key["Key"])
    return aws_files


def get_json_object(bucket, file_object):
    # This function returns a pandas dataframe when provided an aws bucket and csv object.
    s3_client = boto3.client('s3')
    s3_object = s3_client.get_object(Bucket=bucket, Key=file_object)
    json_object = s3_object['Body']
    json_string = json.load(json_object)
    return json_string


def unique_keyword_generator_for_dictionaries(json_files, keyword):
    key_list = []
    for file in json_files:
        json_object = get_json_object(bucket, file)
        if keyword in list(json_object.keys()):
            for key in list(json_object[keyword].keys()):
                key_list.append(key)
    distinct_keys = list(set(key_list))
    return distinct_keys


def unique_keyword_generator_for_lists(json_files, keyword):
    value_list = []
    for file in json_files:
        json_object = get_json_object(bucket, file)
        if keyword in list(json_object.keys()):
            for key in json_object[keyword]:
                value_list.append(key)
    distinct_values = list(set(value_list))
    return distinct_values
