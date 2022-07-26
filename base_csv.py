import boto3
from pprint import pprint
import pandas as pd


pd.options.display.max_rows = 99999

# connect to S3 client and bucket
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

# Name of bucket
bucket_name = "data-eng-30-final-project-files"
aws_prefix = "Talent/"

# code that allows us to iterate through all pages in S3 bucket
paginator = s3_client.get_paginator("list_objects_v2")
contents = paginator.paginate(Bucket=bucket_name, Prefix=aws_prefix)

# extract all CSv files and places it into a list
csv_files = []
for page in contents:
    if "Contents" in page:
        for key in page["Contents"]:
            if ".csv" and key["Key"].endswith(".csv"):  # checks to see if files are in csv
                keystring = key["Key"]# gets filename of all csv files
                objectbody = s3_client.get_object(Bucket=bucket_name, Key=keystring)["Body"] # retrieves the body data from each csv
                readbody = pd.read_csv(objectbody)
                pd.set_option("display.max_columns", None)
                csv_files.append(readbody)

# merges all csv files into one list
all_csv = pd.concat(csv_files, ignore_index=True)
print(all_csv)

# converts list into a csv file
all_csv.to_csv("Candidates.csv")

