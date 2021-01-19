import argparse
from p_acquisition.m_acquisition import data_collection


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

    print('Importing data to directory...')

    full_df.to_csv('./data/raw/full_data.csv', index=False)

    print('Finished process...')



if __name__ == '__main__':

    arguments = argument_parser()

    main(arguments)