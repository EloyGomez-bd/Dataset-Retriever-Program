import argparse
from p_acquisition.m_acquisition import data_collection
from p_wrangling.m_wrangling import create_df_processed
from p_analysis.m_analysis import analysis
from p_reporting.m_reporting import report


def argument_parser():
    """
    parse arguments to script
    """

    parser = argparse.ArgumentParser()

    # Arguments here
    parser.add_argument("-p", "--path", help="specify database location", type=str, required=True)

    # Arguments here

    args = parser.parse_args()

    return args


def main(arguments):

    print('Starting process...')

    path = arguments.path

    full_df = data_collection(path=path)

    print('Exporting data to directory...')

    full_df.to_csv('./data/raw/full_data.csv', index=False)

    df_processed = create_df_processed()

    df_processed.to_csv('./data/processed/df_processed.csv', index=False)

    print('Analysing data and exporting data...')

    df_analysed = analysis()

    df_analysed.to_csv('./data/results/df_analysed.csv', index=False)

    results = report()

    print('Exporting results')

    results.to_csv('./data/results/results.csv', index=False)


if __name__ == '__main__':

    arguments = argument_parser()

    main(arguments)
