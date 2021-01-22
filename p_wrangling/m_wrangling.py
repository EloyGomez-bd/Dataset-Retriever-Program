import pandas as pd
import json
import requests
from tqdm import tqdm


def data_processing():
    """
    Obtain the full data set with all columns and return a data frame
    only with wrangled columns applicable for the project.
    """
    print('Reading file from folder...')

    df = pd.read_csv('./data/raw/full_data.csv')

    applicable_columns = [df['country_code'], df['normalized_job_code'], df['rural']]

    headers = ['Country', 'Job Title', 'Rural']

    df_processing = pd.concat(applicable_columns, axis=1, keys=headers)

    print('Processing data...')

    df_processing['Rural'] = df_processing['Rural'].str.capitalize()

    df_processing = df_processing.replace(['Countryside', 'Country'], 'Rural')

    df_processing = df_processing.replace(['City', 'Urban'], 'Non-rural')

    return df_processing


def obtain_api(job):
    """
    Make a request to the API and receive the json where the job title is stored
    """

    response = requests.get(f'http://api.dataatwork.org/v1/jobs/{job}')

    results = response.json()

    if pd.isna(job):
        return 'Not job found'
    else:
        return results


def access_job(job):
    """
    Get the job title stored in data obtained from the API
    """

    if 'title' in job:
        return job['title']
    else:
        return job


def create_column_job():
    """
    Get the job title stored in data obtained from the API and create a new column
    """

    df_api_collected = data_processing()

    print('Obtaining data from API...')

    tqdm.pandas()
    df_api_collected['job_extracted'] = df_api_collected.head(5).progress_apply(lambda x:
                                                                                obtain_api(x['Job Title']), axis=1)

    print('Normalizing job titles...')

    df_api_collected['Job Title'] = df_api_collected.head(5).apply(lambda x: access_job(x.job_extracted), axis=1)

    return df_api_collected


def create_df_processed():

    df_api_collected = create_column_job()

    df_wrangled = df_api_collected.drop(columns='job_extracted')

    return df_wrangled

