import pandas as pd


def report():
    """
    Filters the data by country
    """
    data = pd.read_csv('./data/results/df_analysed.csv')

    country = (input('Please introduce a country (\'all\' for complete data): '))

    list_of_countries = ', '.join(list(data.Country.unique()))

    if country in list(data.Country.unique()):
        final_data = data[(data.Country == country)]
        return final_data

    elif country == 'all':
        return data

    else:
        print(f'No data available for {country}. Here is a list of countries: {list_of_countries}.\n')
        country = (input('Please introduce a country (\'all\' for complete data): '))







