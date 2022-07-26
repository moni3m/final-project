import boto3
from pprint import pprint
import pandas as pd

pd.options.display.max_rows = 99999
s3_resource = boto3.resource("s3")
# bucket_name = "data-eng-30-final-project-files"
# aws_prefix = "Talent/"


def connect_to_bucket():
    s3_client = boto3.client("s3")
    return s3_client


def bucket_contents(bucket_name: str, aws_prefix: str):
    paginator = connect_to_bucket().get_paginator("list_objects_v2")
    contents = paginator.paginate(Bucket=bucket_name, Prefix=aws_prefix)
    return contents


def get_all_files(bucket_name: str, aws_prefix: str):  # function iterates through each page
    csv_list = []
    for page in bucket_contents(bucket_name, aws_prefix):
        if "Contents" in page:
            for key in get_all_files("data-eng-30-final-project-files", "Talent/")["Contents"]:
                #print(key)
                if ".csv" and key["Key"].endswith(".csv"):  # checks to see if files are in csv
                    keystring = key["Key"]  # gets filename of all csv files
                    objectbody = connect_to_bucket().get_object(Bucket=bucket_name, Key=keystring)  # retrieves the body data from each csv
                    readbody = pd.read_csv(objectbody["Body"])
                    pd.set_option("display.max_columns", None)
                    csv_list.append(readbody)
            return csv_list



# def get_csv(bucket_name: str, aws_prefix: str):
#     csv_list = []
#     for key in get_all_files(bucket_name, aws_prefix)["Contents"]:
#         if ".csv" and key["Key"].endswith(".csv"):  # checks to see if files are in csv
#             keystring = key["Key"]  # gets filename of all csv files
#             objectbody = connect_to_bucket().get_object(Bucket=bucket_name, Key=keystring)  # retrieves the body data from each csv
#             readbody = pd.read_csv(objectbody["Body"])
#             #pd.set_option("display.max_columns", None)
#             csv_list.append(readbody)
#             return readbody
#
#
# print(get_csv("data-eng-30-final-project-files", "Talent/"))