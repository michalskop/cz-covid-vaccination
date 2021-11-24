"""Vaccination CZ - tables for maps."""

import datetime
import numpy as np
import pandas as pd

url = 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-profese.csv'

data = pd.read_csv(url)

# data = pd.read_csv("../orp/ockovani-profese.csv")

pt_db = data.pivot_table(index=["kraj_nuts_kod", 'vekova_skupina', 'poradi_davky'], values='id', aggfunc='count', dropna=False).reset_index().fillna(0)

pt_db.to_csv("test.csv")