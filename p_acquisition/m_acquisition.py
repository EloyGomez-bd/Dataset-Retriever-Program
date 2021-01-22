import pandas as pd
from sqlalchemy import create_engine


def connection_to_db(path):

    print(f'Establishing connection to {path}')

    engine = create_engine(f'sqlite:///{path}')

    print('Connexion to the database has been a success.')

    return engine


def data_collection(path):

    # SQL Query to get all data from database

    query = """
            SELECT * FROM country_info
            LEFT JOIN personal_info ON country_info.uuid = personal_info.uuid
            LEFT JOIN career_info ON country_info.uuid = career_info.uuid
            LEFT JOIN poll_info ON country_info.uuid = poll_info.uuid
            """

    engine = connection_to_db(path)

    read_query = pd.read_sql_query(query, engine)

    full_data_frame = pd.DataFrame(read_query)

    # With query above we obtain a data frame with several columns (uuid) duplicated. We want just one..

    data_frame: object = full_data_frame.loc[:, ~full_data_frame.columns.duplicated()]

    return data_frame




