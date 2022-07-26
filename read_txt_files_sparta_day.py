import boto3
import pandas

df = pandas.DataFrame()

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
bucket_name = "data-eng-30-final-project-files"

AWS_BUCKET_PREFIX = 'Talent/Sparta'  # Access the Talent txt files
bucket_contents = s3_client.list_objects(Bucket=bucket_name, Prefix=AWS_BUCKET_PREFIX)

for file in bucket_contents['Contents']:  # Accessing the file from the Bucket
    file_name = file["Key"]

    s3_client.download_file(   # Download the file one by one
        Filename="Sparta_day.txt",
        Bucket=bucket_name,
        Key=file_name)
    try:
        data = pandas.read_csv(  # Converting file CSV file
            "Sparta_day.txt",
            sep=";",
            header=None)
        df = df.append(data)  # Append file to CSV file
        df.to_csv("Sparta_day.csv")  # Save data to CSV file
    except:
        pass


