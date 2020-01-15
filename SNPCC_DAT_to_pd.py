import re
import os
import glob
import argparse
import numpy as np
import pandas as pd
import multiprocessing
from io import StringIO
from functools import partial
from concurrent.futures import ProcessPoolExecutor


def read_dat(fname):
    """Load SNPCC formatted data and cast it to a PANDAS dataframe
    Args:
        fname (str): path + name to .DAT file
    Returns:
        (pandas.DataFrame) dataframe with data and metadata
    """

    # Read photometry
    #
    # Identify header rows
    with open(fname, 'r') as fin:
        idx = next(i for i, j in enumerate(fin) if j.startswith('VARLIST'))
    # read DataFrame without header
    df = pd.read_csv(fname, skiprows=idx, delimiter=" ",
                     index_col=False, skipinitialspace=True, skipfooter=1, engine='python')
    # eliminate rows that are not observations
    df = df[df['VARLIST:'] == 'OBS:']
    cols_to_keep = [k for k in df.keys() if k not in ['FIELD', 'VARLIST:']]
    df = df[cols_to_keep]

    # Read metadata and save info
    # TODO: save also errors
    with open(fname, 'r') as fhin:
        for line in fhin:
            words = line.strip().split(':')
            if len(words) > 1 and len(words[1]) > 1:
                # formatting
                val = re.findall(r'\S+', words[1])[0]
                df[words[0].strip(" ")] = np.ones(len(df))*float(val) if re.match(
                    r'^-?\d+(?:\.\d+)?$', val) is not None else [str(val) for i in range(len(df))]
            if 'NOBS:' in line:
                break
    # some reformatting
    df['SNID'] = df['SNID'].astype(int)
    keys_ibc = [1, 5, 6, 7, 8, 9, 10, 11, 13, 14, 16,
                18, 22, 23, 29, 45, 28]
    keys_ii = [2, 3, 4, 12, 15, 17, 19, 20, 21, 24, 25, 26, 27,
               30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44]
    df['TYPE'] = df['SNTYPE'].apply(
        lambda x: 'Ia' if x == 0 else (
            'Ibc' if x in keys_ibc else ('II' if x in keys_ii else 'unknown'))
    )

    return df


def only_metadata(fname):
    """Load SNPCC formatted data and cast its metadata to a PANDAS dataframe
    Args:
        fname (str): path + name to .DAT file
    Returns:
        (pandas.DataFrame) dataframe with metadata only
    """
    dic = {}
    # Read metadata and save info
    # TODO: save also errors
    with open(fname, 'r') as fhin:
        for line in fhin:
            words = line.strip().split(':')
            if len(words) > 1 and len(words[1]) > 1:
                # formatting
                val = re.findall(r'\S+', words[1])[0]
                dic[words[0].strip(" ")] = [float(val) if re.match(
                    r'^-?\d+(?:\.\d+)?$', val) is not None else str(val)]
            if 'NOBS:' in line:
                break
    # some reformatting
    keys_ibc = [1, 5, 6, 7, 8, 9, 10, 11, 13, 14, 16,
                18, 22, 23, 29, 45, 28]
    keys_ii = [2, 3, 4, 12, 15, 17, 19, 20, 21, 24, 25, 26, 27,
               30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44]
    dic['SNID'] = [int(i) for i in dic['SNID']]
    dic['TYPE'] = 'Ia' if dic['SNTYPE'] == 0 else (
        'Ibc' if dic['SNTYPE'] in keys_ibc else ('II' if dic['SNTYPE'] in keys_ii else 'unknown'))
    df = pd.DataFrame.from_dict(dic)

    return df


if __name__ == '__main__':
    '''Read SNPCC data format DAT and convert to csv with all light-curves
        Not optimized for very large number of light-curves == number of DAT files
    '''
    parser = argparse.ArgumentParser(
        description='Selection function data vs simulations')

    parser.add_argument('--path_data', type=str,
                        default='./SIMGEN_PUBLIC_DES/DES_*DAT',
                        help="Path to data files in .DAT SPCC format")

    parser.add_argument('--path_dump', type=str,
                        default='./dump/',
                        help="Path to dump a csv database")

    parser.add_argument('--test', action="store_true",
                        help="Only load one file to test it works")

    parser.add_argument('--only_metadata', action="store_true",
                        help="Only load one file to test it works")

    # Init
    args = parser.parse_args()
    path_data = args.path_data
    path_dump = args.path_dump
    os.makedirs(path_dump, exist_ok=True)

    # Init parallization
    max_workers = multiprocessing.cpu_count()

    # Get all files
    list_files = glob.glob(path_data)
    print(f'Files to process {len(list_files)}')

    if args.test:
        list_files = list_files[:10]
        print(f'Files to process shortened to {len(list_files)}')

    if not args.only_metadata:
        list_df = []
        if not args.test:
            # Process the whole data
            process_fn = partial(read_dat)
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                list_df = executor.map(process_fn, list_files)
        else:
            for i in range(len(list_files)):
                list_df.append(read_dat(list_files[i]))
        df = pd.concat(list_df, sort=False)
        df.to_csv(f'{path_dump}/database.csv')

    else:
        # Fetch only metadata
        list_df = []
        if not args.test:
            process_fn = partial(only_metadata)
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                list_df = executor.map(process_fn, list_files)
        else:
            for i in range(len(list_files)):
                list_df.append(only_metadata(list_files[i]))
        df = pd.concat(list_df, sort=False)
        df.to_csv(f'{path_dump}/metadata.csv')
