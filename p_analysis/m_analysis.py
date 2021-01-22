import pandas as pd


def obtain_processed_data():
    """
    Read the csv from directory
    """
    df_processed = pd.read_csv('./data/processed/df_processed.csv')

    return df_processed


def check_quantity(df_processed):
    """
    Creates a column with the number of jobs per country and if people comes from rural o non-rural places
    """
    df_analysis_1 = df_processed.groupby(['Country',
                                          'Rural',
                                          'Job Title'])['Job Title'].count().reset_index(name="Quantity")

    return df_analysis_1


def calc_percentage(quantity, dataframe_column):

    percentage = quantity / dataframe_column.sum() * 100

    return round(percentage, 1)


def percentage(df_analysis_1):
    """
    calculate the percentage of a job (rural or non-rural) in a country with respect to the total of all countries
    """
    df_analysis_1['Percentage'] = df_analysis_1.apply(lambda x:
                                                      f'{calc_percentage(x.Quantity, df_analysis_1.Quantity)}%', axis=1)

    return df_analysis_1

def analysis():
    """
    Covers the two analysis done for the processed data frame
    """
    df_processed = obtain_processed_data()

    df_analysis_1 = check_quantity(df_processed)

    df_analysed = percentage(df_analysis_1)

    return df_analysed