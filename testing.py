from text_file_operations import *

bucket = 'data-eng-30-final-project-files'
aws_prefix = 'Talent/'


json_files = download_json_filenames(bucket, aws_prefix)
tech_scores_headers = 'name' + unique_keyword_generator_for_dictionaries(json_files, 'tech_self_score')
strength_headers = 'name' + unique_keyword_generator_for_lists(json_files[:11], 'strengths')
weakness_headers = 'name' + unique_keyword_generator_for_lists(json_files[:11], 'weaknesses')

print(unique_keyword_generator_for_dictionaries(json_files[:11], 'tech_self_score'))
print()
print(unique_keyword_generator_for_lists(json_files[:11], 'strengths'))




# json_object = get_json_object(bucket, json_files[0])
# headers = list(json_object.keys())
# values = list(json_object.values())
# tech_self_score_tuple = [(v, k) for v, k in json_object['tech_self_score'].items()]
# pprint.pp(json_object)
# json_object['tech_self_score'] = tech_self_score_tuple


# print(len(values))
# print(len(headers))



#
# pprint.pp(consolidated_json_object)
#
# with open('consolidated_applicant_data.json', 'a') as outfile:
#     for file in json_files:
#         json_object = get_json_object(bucket, file)
#         json.dump(json_object, outfile)


# pprint.pp(json_object)
# print()
# headers = list(json_object.keys())
# values = json_object.values()
# pprint.pp(list(values))

# df = pd.DataFrame.from_dict(json_object)
# pprint.pp(df)
# df = pd.DataFrame(values, columns=headers)
# pprint.pp(df.transpose())

