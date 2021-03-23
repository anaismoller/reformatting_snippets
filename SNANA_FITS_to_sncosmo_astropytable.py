import numpy as np
import pandas as pd
from pathlib import Path
from astropy.table import Table
from astropy.table import QTable


"""
    SNANA simulation/data format to sncosmo
"""


def read_snana_fits_to_sncosmo_table(fname):
    """Load SNANA formatted data and cast it to sncosmo table
    Args:
        fname (str): path + name to PHOT.FITS file
    Returns:
        (astropy.Table) table with photometry in sncosmo format
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
    df_header["SNID"] = df_header["SNID"].astype(np.int32)

    # add SNID to phot for skimming
    arr_ID = np.zeros(len(df_phot), dtype=np.int32)
    # New light curves are identified by MJD == -777.0
    arr_idx = np.where(df_phot["MJD"].values == -777.0)[0]
    arr_idx = np.hstack((np.array([0]), arr_idx, np.array([len(df_phot)])))
    # Fill in arr_ID
    for counter in range(1, len(arr_idx)):
        start, end = arr_idx[counter - 1], arr_idx[counter]
        # index starts at zero
        arr_ID[start:end] = df_header.SNID.iloc[counter - 1]
    df_phot["SNID"] = arr_ID

    df_phot = df_phot[df_phot.MJD != -777.000]

    df_tmp = pd.DataFrame()
    df_tmp["SNID"] = df_phot["SNID"]
    df_tmp["time"] = df_phot["MJD"]
    df_tmp["band"] = "des" + df_phot["FLT"].str.decode("utf-8").str.strip(" ")
    df_tmp["zp"] = df_phot["ZEROPT"]
    # FLUXCAL is zp = 27.5
    df_tmp["flux"] = df_phot["FLUXCAL"]
    df_tmp["fluxerr"] = df_phot["FLUXCALERR"]
    df_tmp["zp"] = np.repeat(27.5, len(df_phot))
    df_tmp["zpsys"] = np.repeat("ab", len(df_phot))

    at_phot = QTable.from_pandas(df_tmp)

    return at_phot


#
# Use examples
#

# SNANA.FITS to sncosmo astropy table
at_phot = read_snana_fits_to_sncosmo_table("./raw/DES_nonIa-0001_PHOT.FITS")
