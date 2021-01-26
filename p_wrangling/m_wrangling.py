import pandas as pd
import json
import requests
from tqdm import tqdm
import re


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

    df_processing = df_processing.replace({'Country': 'GB'}, 'UK')
    df_processing = df_processing.replace({'Country': 'GR'}, 'EL')

    df_processing['Rural'] = df_processing['Rural'].str.capitalize()

    df_processing = df_processing.replace(['Countryside', 'Country'], 'Rural')

    df_processing = df_processing.replace(['City', 'Urban'], 'Non-rural')

    return df_processing


def data_scrapping(dataframe):
    """
    Obtain countries and equivalent codes from url
    """
    url = 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes'

    print(f'Scraping data from {url}')

    tables_df = pd.read_html(url)

    table1_df = tables_df[0]
    table2_df = tables_df[1]
    table3_df = tables_df[2]
    table4_df = tables_df[3]
    table5_df = tables_df[4]
    table6_df = tables_df[5]
    table7_df = tables_df[6]
    table8_df = tables_df[7]
    table9_df = tables_df[8]

    table2_df = table2_df.drop(columns=[2])
    table3_df = table3_df.drop(columns=[2])
    table6_df = table6_df.drop(columns=[2, 5])
    table7_df = table7_df.drop(columns=[2, 5])
    table9_df = table9_df.drop(columns=[2, 5, 8])

    columns = ['Country', 'CountryCode']

    table1_1 = table1_df[[0, 1]]
    table1_1.columns = columns
    table1_2 = table1_df[[2, 3]]
    table1_2.columns = columns
    table1_3 = table1_df[[4, 5]]
    table1_3.columns = columns
    table1_4 = table1_df[[6, 7]]
    table1_4.columns = columns
    table1 = pd.concat([table1_1, table1_2, table1_3, table1_4], axis=0)
    table1.reset_index(drop=True, inplace=True)

    table2_1 = table2_df[[0, 1]]
    table2_1.columns = columns
    table2_2 = table2_df[[3, 4]]
    table2_2.columns = columns
    table2 = pd.concat([table2_1, table2_2], axis=0)
    table2.reset_index(drop=True, inplace=True)

    table3_df.columns = columns

    table4_df.columns = columns

    table5_df.columns = columns

    table6_1 = table6_df[[0, 1]]
    table6_1.columns = columns
    table6_2 = table6_df[[3, 4]]
    table6_2.columns = columns
    table6_3 = table6_df[[6, 7]]
    table6_3.columns = columns
    table6 = pd.concat([table6_1, table6_2, table6_3], axis=0)
    table6.reset_index(drop=True, inplace=True)

    table7_1 = table7_df[[0, 1]]
    table7_1.columns = columns
    table7_2 = table7_df[[3, 4]]
    table7_2.columns = columns
    table7_3 = table7_df[[6, 7]]
    table7_3.columns = columns
    table7 = pd.concat([table7_1, table7_2, table7_3], axis=0)
    table7.reset_index(drop=True, inplace=True)

    table8_df.columns = columns

    table9_1 = table9_df[[0, 1]]
    table9_1.columns = columns
    table9_2 = table9_df[[3, 4]]
    table9_2.columns = columns
    table9_3 = table9_df[[6, 7]]
    table9_3.columns = columns
    table9_4 = table9_df[[9, 10]]
    table9_4.columns = columns
    table9 = pd.concat([table9_1, table9_2, table9_3, table9_4], axis=0)
    table9.reset_index(drop=True, inplace=True)

    table = pd.concat([table1, table2, table3_df, table4_df, table5_df, table6, table7, table8_df, table9], axis=0)

    table.reset_index(drop=True, inplace=True)

    table = table[table['Country'].notna()]

    table['CountryCode'] = table.apply(lambda x: clean_codes(x['CountryCode']), axis=1)

    dataframe['Country'] = dataframe.apply(lambda x: replace_country(x['Country'],
                                                                     list(table['Country']),
                                                                     list(table['CountryCode'])), axis=1)

    return dataframe


def clean_codes(column: str):
    """
    Removes non-letter characters from text
    """

    clean_text = re.sub("[^a-zA-Z \n\.]", '', column)
    return clean_text


def replace_country(code, country_list, code_list):
    """
    Replace the country code for country name
    """
    for i in range(len(code_list)):
        if code == code_list[i]:
            return country_list[i]


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
    df_api_collected['job_extracted'] = df_api_collected.progress_apply(lambda x:
                                                                                  obtain_api(x['Job Title']), axis=1)

    print('Normalizing job titles...')

    df_api_collected['Job Title'] = df_api_collected.apply(lambda x: access_job(x.job_extracted), axis=1)

    return df_api_collected


def create_df_processed():

    df_api_collected = create_column_job()

    df_applicable_columns = df_api_collected.drop(columns='job_extracted')

    df_wrangled = data_scrapping(df_applicable_columns)

    return df_wrangled

