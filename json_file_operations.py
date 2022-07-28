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
    # This function returns a json object when provided an aws bucket and json file path name.
    s3_client = boto3.client('s3')
    s3_object = s3_client.get_object(Bucket=bucket, Key=file_object)
    json_object = s3_object['Body']
    json_string = json.load(json_object)
    return json_string


def unique_keyword_non_dict_list_data_types(json_files, primary_identifier='name'):
    # This function loops through a list of json files and returns a list of unique values
    # of a provided key-value pair (values must be in dict form)
    key_list = []
    for file in json_files:
        json_object = get_json_object(bucket, file)
        for key in list(json_object.keys()):
            if type(json_object[key]) != dict and type(json_object[key]) != list:
                key_list.append(key)
    distinct_keys = list(set(key_list))
    distinct_keys.remove(primary_identifier)
    new_keys = [primary_identifier] + distinct_keys
    return new_keys


def unique_keyword_generator_for_dictionaries(json_files, keyword):
    # This function loops through a list of json files and returns a list of unique values
    # of a provided key-value pair (values must be in dict form)
    key_list = []
    for file in json_files:
        json_object = get_json_object(bucket, file)
        if keyword in list(json_object.keys()):
            for key in list(json_object[keyword].keys()):
                key_list.append(key)
    distinct_keys = list(set(key_list))
    return distinct_keys


def unique_keyword_generator_for_lists(json_files, keyword):
    # This function loops through a list of json files and returns a list of unique values
    # of a provided key-value pair (values must be in list form)
    value_list = []
    for file in json_files:
        json_object = get_json_object(bucket, file)
        if keyword in list(json_object.keys()):
            for key in json_object[keyword]:
                value_list.append(key)
    distinct_values = list(set(value_list))
    return distinct_values


def list_to_txt_converter(name_of_file, listed_values):
    # This stores a list within a text file. This helps saves time by providing a txt file to work with
    # instead of having to run the code again and again to generate a list.
    with open(f'{name_of_file}.txt', 'w') as output:
        output.write(str(listed_values))


def txt_to_list_converter(name_of_file):
    # This converts a given txt file into a single list
    final_list = []
    with open(name_of_file, 'r') as data:
        converted_list = data.read()
        new_converted_list = converted_list[1:-1]
        new_converted_list = new_converted_list.replace("'","").split(',')
        for item in new_converted_list:
            final_list.append(item.lstrip())
        return final_list


def convert_non_list_dict_values_to_df(json_files, df_headers):
    # This loops through a list of specified json files and appends a specified key value pair
    # (values must be in list form) to a list and aggregates it all to a dataframe.
    data = []
    for file in json_files:
        index_list = []
        json_object = get_json_object(bucket, file)
        for key in df_headers:
            if type(json_object[key]) != dict and type(json_object[key]) != list:
                index_list.append(json_object[key])
        data.append(index_list)

    df = pd.DataFrame(data, columns=df_headers)
    return df


def convert_dictionary_values_to_df(json_files, keyword, df_headers, primary_identifier='name'):
    # This loops through a list of specified json files and appends a specified key value pair
    # (values must be in dict form) to a list aggregates it all to a dataframe.
    data = []
    for file in json_files:
        index_list = []
        json_object = get_json_object(bucket, file)
        index_list.insert(0, json_object[primary_identifier])
        index = 1
        for subject in df_headers[1:]:
            if subject in json_object[keyword].keys():
                index_list.insert(index, json_object[keyword][subject])
            else:
                index_list.insert(index, None)
            index += 1
        data.append(index_list)
    df = pd.DataFrame(data, columns=df_headers)
    return df


def convert_list_values_to_df(json_files, keyword, df_headers, primary_identifier='name'):
    # This loops through a list of specified json files and appends a specified key value pair
    # (values must be in list form) to a list and aggregates it all to a dataframe.
    data = []
    for file in json_files:
        index_list = []
        json_object = get_json_object(bucket, file)
        index_list.insert(0, json_object[primary_identifier])
        index = 1
        for subject in df_headers[1:]:
            if subject in json_object[keyword]:
                index_list.insert(index, True)
            else:
                index_list.insert(index, False)
            index += 1
        data.append(index_list)
    df = pd.DataFrame(data, columns=df_headers)
    return df


def generate_csv_file(df, filename):
    # Converts a given dataframe to CSV format
    df.to_csv(f'{filename}.csv', header=True, mode='w')
    print(f'{filename}.csv has been generated')
