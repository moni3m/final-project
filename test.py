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


def get_all_files(bucket_name: str, aws_prefix: str, file_ending: str =".txt"):  # function iterates through each page
    csv_list = []
    for page in bucket_contents(bucket_name, aws_prefix):
        if "Contents" in page:
            for key in page["Contents"]:
                #print(key)
                if file_ending and key["Key"].endswith(file_ending):  # checks to see if files are in csv
                    keystring = key["Key"]  # gets filename of all csv files
                    objectbody = connect_to_bucket().get_object(Bucket=bucket_name, Key=keystring)  # retrieves the body data from each csv
                    readbody = pd.read_csv(objectbody["Body"], sep="delimiter", header=None)
                    pd.set_option("display.max_columns", None)
                    csv_list.append(readbody)
    return csv_list


def merge_csv(bucket_name: str, aws_prefix: str, file_ending: str =".txt"):
    all_csv = pd.concat(get_all_files(bucket_name, aws_prefix, file_ending), ignore_index=True)
    return all_csv


def convert_to_csv(bucket_name: str, aws_prefix: str, file_ending: str =".txt"):
    return merge_csv(bucket_name, aws_prefix, file_ending).to_csv("Candidates3.csv")


def execute_all(bucket_name: str, aws_prefix: str,file_ending: str =".txt"):
    get_all_files(bucket_name, aws_prefix, file_ending)
    merge_csv(bucket_name, aws_prefix, file_ending)
    return convert_to_csv(bucket_name, aws_prefix, file_ending)


print(execute_all("data-eng-30-final-project-files", "Talent/", ".txt"))


