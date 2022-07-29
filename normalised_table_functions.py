from CSV_operations import *  # This runs the code necessary to produce the applicant tables/files
from normalisation_functions import *  # This imports the functions to generate the id tables
from json_operations import *  # This runs the code necessary to produce the candidate tables from the json files
from read_txt_files_sparta_day import *  # This runs the code necessary to produce the candidate tables/files from the txt files
from trainee_academy_operations import * # This runs the code necessary to produce the trainee, trainers and workstream files

pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 20)

# Generate Applicant dataframe
applicant_initial_df = pd.read_csv('Applicants.csv')


# Generate Candidate dataframes
candidate_information_initial_df = pd.read_csv('candidate_information.csv')
candidate_tech_scores_initial_df = pd.read_csv('candidate_test_scores.csv')
candidate_strengths_initial_df = pd.read_csv('candidate_strengths.csv')
candidate_weaknesses_initial_df = pd.read_csv('candidate_weaknesses.csv')

candidate_day_df = pd.read_csv('candidate_day_information.csv')

# Generate Trainee Data
trainee_initial_df = pd.read_csv('consolidated_trainee_information.csv')

# Generate various lists
recruiters_list = column_to_list_generator(applicant_initial_df, 'invited_by')
names_list = column_to_list_generator(applicant_initial_df, 'name')
tech_workstream_list = column_to_list_generator(trainee_initial_df, 'workstream_name')
trainer_information_list = column_to_list_generator(trainee_initial_df, 'trainer')
academy_list = column_to_list_generator(candidate_day_df, 'Academy Location')

# Generate the primary key tables
recruiters_table = df_id_generator(recruiters_list, 'REC00', 'recruiters')
spartans_table = df_id_generator(names_list, 'SP00', 'spartan')
tech_workstream_table = df_id_generator(tech_workstream_list, 'TECH00', 'tech_workstream')
trainer_table = df_id_generator(trainer_information_list, 'TR00', 'trainer')
academy_table = df_id_generator(academy_list, 'AC00', 'academy')



# Join the tables
# 1. Applicants Table
applicant_information_initial = pd.merge(spartans_table, applicant_initial_df, left_on=['spartan_name'], right_on=['name'])
applicant_information = pd.merge(applicant_information_initial, recruiters_table, left_on=['invited_by'], right_on=['recruiters_name'])
dropped_applicant_columns = ['Unnamed: 0', 'spartan_name', 'name', 'id', 'recruiters_name', 'invited_by', 'invited_date','month']

applicant_information = applicant_information.drop(dropped_applicant_columns, axis=1)

#######################################################################################################################

# 2. Candidates Table
candidate_pp_scores = pd.merge(spartans_table, candidate_day_df, left_on=['spartan_name'], right_on=['Candidate Name'])
candidate_pp_scores = pd.merge(candidate_pp_scores, academy_table, left_on=['Academy Location'], right_on=['academy_name'])
dropped_candidate_pp_columns = ['Unnamed: 0', 'spartan_name', 'Candidate Name', 'Academy Location', 'academy_name']
candidate_pp_scores = candidate_pp_scores.drop(dropped_candidate_pp_columns, axis=1)

candidate_information = pd.merge(spartans_table, candidate_information_initial_df, left_on=['spartan_name'], right_on=['name'])
dropped_candidate_info_columns = ['Unnamed: 0', 'spartan_name']
candidate_information = candidate_information.drop(dropped_candidate_info_columns, axis=1)

candidate_strengths = pd.merge(spartans_table, candidate_strengths_initial_df, left_on=['spartan_name'], right_on=['name'])
dropped_candidate_info_columns = ['Unnamed: 0', 'spartan_name', 'name']
candidate_strengths = candidate_strengths.drop(dropped_candidate_info_columns, axis=1)

candidate_weaknesses = pd.merge(spartans_table, candidate_weaknesses_initial_df, left_on=['spartan_name'], right_on=['name'])
dropped_candidate_info_columns = ['Unnamed: 0', 'spartan_name', 'name']
candidate_weaknesses = candidate_weaknesses.drop(dropped_candidate_info_columns, axis=1)

candidate_tech_scores = pd.merge(spartans_table, candidate_tech_scores_initial_df, left_on=['spartan_name'], right_on=['name'])
dropped_candidate_info_columns = ['Unnamed: 0', 'spartan_name', 'name']
candidate_tech_scores = candidate_tech_scores.drop(dropped_candidate_info_columns, axis=1)

#######################################################################################################################

# 3. Trainees Table
trainees = pd.merge(spartans_table, trainee_initial_df, left_on=['spartan_name'], right_on=['name'])
trainees = pd.merge(trainees, tech_workstream_table, left_on=['workstream_name'], right_on='tech_workstream_name')
trainees = pd.merge(trainees, trainer_table, left_on=['trainer'], right_on=['trainer_name'])

trainee_information = trainees[['spartan_id', 'tech_workstream_id', 'trainer_id', 'status']]
dropped_trainee_info_for_scores = ['spartan_name', 'Unnamed: 0', 'name', 'status', 'workstream_name', 'trainer', 'tech_workstream_name', 'trainer_name', 'trainer_id']
trainee_scores = trainees.drop(dropped_trainee_info_for_scores, axis=1)

tech_workstream = pd.merge(tech_workstream_table, trainee_information, left_on='tech_workstream_id', right_on='tech_workstream_id')
dropped_tech_workstream_columns = ['spartan_id', 'status']
tech_workstream = tech_workstream.drop(dropped_tech_workstream_columns, axis=1)

#######################################################################################################################

# 4. List out normalised dataframes
spartan_information = applicant_information
candidate_pp_scores = candidate_pp_scores
candidate_tech_scores = candidate_tech_scores
candidate_information = candidate_information
candidate_strengths = candidate_strengths
candidate_weaknesses = candidate_weaknesses
trainee_weekly_scores = trainee_scores
tech_workstream = tech_workstream
trainer_information = trainer_table
academy_information = academy_table
recruiter_information = recruiters_table
trainee_information = trainee_information

#######################################################################################################################

# 5. Generate the csv files for the normalised dataframes
applicant_information.to_csv('spartan_information_normalised.csv', header=True, mode='w')
candidate_pp_scores.to_csv('candidate_pp_scores_normalised.csv', header=True, mode='w')
candidate_tech_scores.to_csv('candidate_tech_scores_normalised.csv', header=True, mode='w')
candidate_information.to_csv('candidate_information_normalised.csv', header=True, mode='w')
candidate_strengths.to_csv('candidate_strengths_normalised.csv', header=True, mode='w')
candidate_weaknesses.to_csv('candidate_weaknesses_normalised.csv', header=True, mode='w')
trainee_information.to_csv('trainee_information_normalised.csv', header=True, mode='w')
trainee_weekly_scores.to_csv('trainee_weekly_scores_normalised.csv', header=True, mode='w')
tech_workstream.to_csv('tech_workstream_normalised.csv', header=True, mode='w')
trainer_information.to_csv('trainer_information_normalised.csv', header=True, mode='w')
academy_information.to_csv('academy_information_normalised.csv', header=True, mode='w')
recruiter_information.to_csv('applicant_information_normalised.csv', header=True, mode='w')




