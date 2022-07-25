import pprint

import pandas as pd
import ppprint
import json

from json_file_operations import *

bucket = 'data-eng-30-final-project-files'
aws_prefix = 'Talent/'


json_files = download_json_filenames(bucket, aws_prefix)
pprint.pp(json_files)
consolidated_json_object = list()

pprint.pp(len(json_files))

# for file in json_files:
#     json_object = get_json_object(bucket, file)
#     consolidated_json_object.append(json_object)
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

