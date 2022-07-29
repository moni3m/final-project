from normalised_table_functions import *
import sqlite3



sqliteConnection = sqlite3.connect('project.db')
conn = sqlite3.connect('sparta_global.db')

c = sqliteConnection.cursor()

applicant_information.to_sql('applicant_information', conn, if_exists='append', index = False)
candidate_pp_scores.to_sql('candidate_pp_scores', conn, if_exists='append', index = False)
candidate_tech_scores.to_sql('candidate_tech_scores', conn, if_exists='append', index = False)
candidate_information.to_sql('candidate_information', conn, if_exists='append', index = False)
candidate_strengths.to_sql('candidate_strengths', conn, if_exists='append', index = False)
candidate_weaknesses.to_sql('candidate_weaknesses', conn, if_exists='append', index = False)
trainee_weekly_scores.to_sql('trainee_weekly_scores', conn, if_exists='append', index = False)
tech_workstream.to_sql('tech_workstream', conn, if_exists='append', index = False)
trainer_information.to_sql('trainer_information', conn, if_exists='append', index = False)
academy_information.to_sql('academy_information', conn, if_exists='append', index = False)
recruiter_information.to_sql('recruiter_information', conn, if_exists='append', index = False)
trainee_information.to_sql('trainee_information', conn, if_exists='append', index = False)

