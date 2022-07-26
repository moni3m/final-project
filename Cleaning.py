import boto3
from pprint import pprint
import pandas as pd
import re

pd.options.display.max_rows = 99999
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
bucket_name = "data-eng-30-final-project-files"
aws_prefix = "Talent/"

s3_object = s3_client.get_object(Bucket=bucket_name, Key="Talent/April2019Applicants.csv")
strbody = s3_object["Body"]
pd.set_option("display.max_columns", None)
data = pd.read_csv(strbody)
pprint(data.head())


#print(data.phone_number.dtypes)