from trainee_academy_functions import *

bucket = 'data-eng-30-final-project-files'
aws_prefix = 'Academy/'

# Generate a list of the names of all the csv files we will be working with
files = download_aws_filenames(bucket, aws_prefix)


# Pulling the csv data from the file names, converting it to dataframe,
# then generating a list of dataframes
df_list = create_df_list(files)


# Combining all the dataframes together
combined_df = combine_dfs(df_list)

# Converting combined dataframe to csv format.
combined_df.to_csv('trainee_information.csv', header=True, mode='w')



