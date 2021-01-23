import pandas as pd


def report():
    """
    Filters the data by country
    """
    data = pd.read_csv('./data/results/df_analysed.csv')

    country = (input('Please introduce a country (\'all\' for complete data): '))

    if country == 'all':
        return data
    else:
        country_filter = data.loc[data['Country'] == country]
        return country_filter





