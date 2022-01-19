import numpy as np
import pandas as pd
from collections import OrderedDict

""" Create MNRAS format authorlist and affiliations from csv
    NOT optimized, many loops!
    This code assumes that columns in csv are:
    - "Authorname"
    - "Affiliation"
"""

if __name__ == "__main__":
    # Read your csv author list
    df = pd.read_csv("yourcsvfile.csv")
    # drop bad entries
    df = df[~df["Affiliation"].isna()]

    # assiggn your first authors
    first_authors = ['A.~M\\"oller']
    # the rest will follow alphabetically

    # add exceptions in your database to correct bad affiliations
    exceptions = {
        'A.~M\\"oller': [
            "Centre for Astrophysics \\& Supercomputing, Swinburne University of Technology, Victoria 3122, Australia",
            "LPC, Université Clermont Auvergne, CNRS/IN2P3, F-63000 Clermont-Ferrand, France.",
        ],
    }

    # get all affiliations correctly and in order
    list_affiliations = []
    all_authors = first_authors + [
        k for k in df["Authorname"].unique() if k not in first_authors
    ]
    for author in all_authors:
        associated_affiliations = df[df["Authorname"] == author][
            "Affiliation"
        ].to_list()
        to_append = (
            associated_affiliations
            if author not in exceptions.keys()
            else exceptions[author]
        )
        list_affiliations.append(to_append)

    # flatten and drop dubplicates while keeping order
    flat_affiliation_list = [item for sublist in list_affiliations for item in sublist]
    unique_ordered_affiliations = list(OrderedDict.fromkeys(flat_affiliation_list))

    # give the affiliations a number
    # there must be a better way but this is the fastest in my head
    df_affiliations = pd.DataFrame()
    df_affiliations["Affiliation"] = unique_ordered_affiliations
    df_affiliations["number"] = np.arange(len(df_affiliations)) + 1

    # now get the string with the lastname + numbers of affiliations
    list_authors_withnumbers = []
    for author in all_authors:
        associated_affiliations = df[df["Authorname"] == author][
            "Affiliation"
        ].to_list()
        affiliations = (
            associated_affiliations
            if author not in exceptions.keys()
            else exceptions[author]
        )
        nums = df_affiliations[df_affiliations["Affiliation"].isin(affiliations)][
            "number"
        ].to_list()
        tmp = [f"{n}" for n in nums]
        tmp2 = (",").join(tmp)
        list_authors_withnumbers.append(author + "$^{" + f"{tmp2}" + "}$")

    print("AUTHOR LIST")
    tmp = (",\n").join(list_authors_withnumbers)
    print(tmp)

    print("AFFILIATIONS")
    tmp = (
        "$^{"
        + df_affiliations["number"].astype(str)
        + "}$ "
        + df_affiliations["Affiliation"]
    )
    tmp2 = (" \\\ \n").join(tmp.to_list())
    print(tmp2)
