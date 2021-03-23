import numpy as np
import pandas as pd
from pathlib import Path
from astropy.table import Table

"""
    SNANA simulation/data format to pandas
    Special case when SNID/CID is a str
"""


def read_fits(fname, drop_separators=False):
    """Load SNANA formatted data and cast it to a PANDAS dataframe

    Args:
        fname (str): path + name to PHOT.FITS file
        drop_separators (Boolean): if -777 are to be dropped

    Returns:
        (pandas.DataFrame) dataframe from PHOT.FITS file (with ID)
        (pandas.DataFrame) dataframe from HEAD.FITS file
    """

    # load photometry
    dat = Table.read(fname, format="fits")
    df_phot = dat.to_pandas()
    # failsafe
    if df_phot.MJD.values[-1] == -777.0:
        df_phot = df_phot.drop(df_phot.index[-1])
    if df_phot.MJD.values[0] == -777.0:
        df_phot = df_phot.drop(df_phot.index[0])

    # load header
    header = Table.read(fname.replace("PHOT", "HEAD"), format="fits")
    df_header = header.to_pandas()
    df_header["SNID"] = df_header["SNID"].str.decode("utf-8")
    df_header["SNID"] = df_header["SNID"].str.strip()

    arr_ID = np.chararray(len(df_phot), itemsize=15)
    # New light curves are identified by MJD == -777.0
    arr_idx = np.where(df_phot["MJD"].values == -777.0)[0]
    arr_idx = np.hstack((np.array([0]), arr_idx, np.array([len(df_phot)])))
    # Fill in arr_ID
    for counter in range(1, len(arr_idx)):
        start, end = arr_idx[counter - 1], arr_idx[counter]
        # index starts at zero
        arr_ID[start:end] = df_header.SNID.iloc[counter - 1]
    df_phot["SNID"] = arr_ID.astype(str)
    df_phot["SNID"] = df_phot["SNID"].str.strip()

    if drop_separators:
        df_phot = df_phot[df_phot.MJD != -777.000]

    return df_header, df_phot


def save_fits(df, fname):
    """Save data frame in fits table

    Arguments:
        df {pandas.DataFrame} -- data to save
        fname {str} -- outname, must end in .FITS
    """

    keep_cols = df.keys()
    df = df.reset_index()
    df = df[keep_cols]

    outtable = Table.from_pandas(df)
    Path(fname).parent.mkdir(parents=True, exist_ok=True)
    outtable.write(fname, format="fits", overwrite=True)
