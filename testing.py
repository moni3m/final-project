import pprint

import pandas as pd

from json_file_operations import *

bucket = 'data-eng-30-final-project-files'
aws_prefix = 'Talent/'

# 1 Generate a list of all the json files we will be working with
json_files = download_json_filenames(bucket, aws_prefix)

# 2. Generate sets of unique values for the tech_self_score, strengths and weaknesses values.
# Convert them into a text file to save time and resources.
list_to_txt_converter('sparta_candidate_info', unique_keyword_non_dict_list_data_types(json_files))
list_to_txt_converter('sparta_subjects', unique_keyword_generator_for_dictionaries(json_files, 'tech_self_score'))
list_to_txt_converter('sparta_strengths', unique_keyword_generator_for_lists(json_files, 'strengths'))
list_to_txt_converter('sparta_weaknesses', unique_keyword_generator_for_lists(json_files, 'weaknesses'))


# 3. Generate the headers for the tables we will be creating
candidate_information_headers = txt_to_list_converter('sparta_candidate_info.txt')
sparta_subjects_headers = ['name'] + txt_to_list_converter('sparta_subjects.txt')
sparta_strengths_headers = ['name'] + txt_to_list_converter('sparta_strengths.txt')
sparta_weaknesses_headers = ['name'] + txt_to_list_converter('sparta_weaknesses.txt')


# 4. Generate dataframes
candidate_information_df = convert_non_list_dict_values_to_df(json_files, candidate_information_headers)
candidate_tech_scores_df = convert_dictionary_values_to_df(json_files, 'tech_self_score', sparta_subjects_headers)
candidate_strengths_df = convert_list_values_to_df(json_files, 'strengths', sparta_strengths_headers)
candidate_weaknesses_df = convert_list_values_to_df(json_files, 'weaknesses', sparta_weaknesses_headers)


# 5. Generate CSV files
candidate_information_df.to_csv('candidate_information.csv', header=True, mode='w')
candidate_tech_scores_df.to_csv('candidate_test_scores.csv', header=True, mode='w')
candidate_strengths_df.to_csv('candidate_strengths.csv', header=True, mode='w')
candidate_weaknesses_df.to_csv('candidate_weaknesses.csv', header=True, mode='w')


# data = []
# for file in json_files:
#     json_object = get_json_object(bucket, file)
#     data.append([json_object['name'],
#                  json_object['date'],
#                  json_object['self_development'],
#                  json_object['geo_flex'],
#                  json_object['financial_support_self'],
#                  json_object['result'],
#                  json_object['course_interest']])
#
# df = pd.DataFrame(data, columns=candidate_information_headers)
# df.to_csv('candidate_information.csv', header=True, mode='w')
# print(df.head())
#
#
#
#
# subjects_data = []
# for file in json_files:
#     index_list = []
#     json_object = get_json_object(bucket, file)
#     index_list.insert(0, json_object['name'])
#     index = 1
#     for subject in sparta_subjects_headers[1:]:
#         if subject in json_object['tech_self_score'].keys():
#             index_list.insert(index, json_object['tech_self_score'][subject])
#         else:
#             index_list.insert(index, None)
#         index += 1
#     subjects_data.append(index_list)
#
# subjects_df = pd.DataFrame(subjects_data, columns=sparta_subjects_headers)
# print(subjects_df.head())
# subjects_df.to_csv('candidate_subjects.csv', header=True, mode='w')
# print()
#
#
#
# strength_data = []
# for file in json_files:
#     index_list = []
#     json_object = get_json_object(bucket, file)
#     index_list.insert(0, json_object['name'])
#     index = 1
#     for attribute in sparta_strengths_headers[1:]:
#         if attribute in json_object['strengths']:
#             index_list.insert(index, True)
#         else:
#             index_list.insert(index, False)
#         index += 1
#     strength_data.append(index_list)
#
# strength_df = pd.DataFrame(strength_data, columns=sparta_strengths_headers)
# print(strength_df.head())
# strength_df.to_csv('candidate_strengths.csv', header=True, mode='w')
# print()
#
# weakness_data = []
# for file in json_files:
#     index_list = []
#     json_object = get_json_object(bucket, file)
#     index_list.insert(0, json_object['name'])
#     index = 1
#     for attribute in sparta_weaknesses_headers[1:]:
#         if attribute in json_object['weaknesses']:
#             index_list.insert(index, True)
#         else:
#             index_list.insert(index, False)
#         index += 1
#     weakness_data.append(index_list)
#
# weakness_df = pd.DataFrame(weakness_data, columns=sparta_weaknesses_headers)
# weakness_df.to_csv('candidate_weaknesses.csv', header=True, mode='w')
# print(weakness_df.head())





