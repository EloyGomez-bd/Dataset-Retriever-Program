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


def bonus1(df_raw):
    """
    Function to obtain table from Bonus 1
    """

    df_raw.rename(columns={'dem_education_level': 'Education Level',
                       'question_bbi_2016wave4_basicincome_argumentsfor': 'Pro Arguments',
                       'question_bbi_2016wave4_basicincome_argumentsagainst': 'Cons Arguments',
                       'question_bbi_2016wave4_basicincome_vote': 'Position',
                       'question_bbi_2016wave4_basicincome_effect': 'Effect',
                       'question_bbi_2016wave4_basicincome_awareness': 'Awareness'}, inplace=True)

    def split(x):
        return x.split('|')

    df_raw['Pro Arguments'] = df_raw.apply(lambda x: split(x['Pro Arguments']), axis=1)

    df_raw['Cons Arguments'] = df_raw.apply(lambda x: split(x['Cons Arguments']), axis=1)

    def ind_range(x):
        if x == ['None of the above']:
            return 0
        else:
            return len(x)

    df_raw['Pro Arguments'] = df_raw.apply(lambda x: ind_range(x['Pro Arguments']), axis=1)

    df_raw['Cons Arguments'] = df_raw.apply(lambda x: ind_range(x['Cons Arguments']), axis=1)

    def position(x):
        if 'vote for' in x:
            return 'In Favor'
        elif 'vote against' in x:
            return 'Against'
        else:
            return 'Impartial'

    df_raw['Position'] = df_raw.apply(lambda x: position(x['Position']), axis=1)

    table = df_raw.groupby('Position')['Pro Arguments', 'Cons Arguments'].sum()

    table = table.rename(
        columns={'Pro Arguments': 'Number of Pro Arguments', 'Cons Arguments': 'Number of Cons Arguments'}).drop(
        index='Impartial')

    return table


def bonus2(df_raw, df_processed):
    """
    Function to obtain table from Bonus 2
    """
    df_raw.rename(columns={'dem_education_level': 'Education Level', 'normalized_job_code': 'Job Title'}, inplace=True)
    df_raw['Job Title'] = df_processed['Job Title']
    df_raw['Education Level'] = df_raw['Education Level'].replace('no', 'no education')

    ed_list = list(df_raw['Education Level'].fillna('None').unique())
    ed_list.remove('None')
    new_order = [1, 2, 3, 0]
    ed_list = [ed_list[i] for i in new_order]

    filtered_dfs = [df_raw[df_raw['Education Level'] == level] for level in ed_list]

    dfs_skills = [i['Job Title'].value_counts().head(11) for i in filtered_dfs]

    final_df = [i.drop(index='Not job found') for i in dfs_skills]

    table = pd.DataFrame(
        {'Education Level': ed_list, 'Top 10 Skills': [list(final_df[i].index) for i in range(len(final_df))]},
        index=None)

    table['Top 10 Skills'] = table.apply(lambda x: ', '.join(x['Top 10 Skills']), axis=1)

    return table



