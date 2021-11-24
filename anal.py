"""Vaccination CZ - tables for maps."""

import datetime
import numpy as np
import pandas as pd

import requests
from contextlib import closing
import csv

url = 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-profese.csv'

i = 0
print(datetime.datetime.now(), i)
with closing(requests.get(url, stream=True)) as r:
    f = (line.decode('utf-8') for line in r.iter_lines())
    reader = csv.reader(f, delimiter=',', quotechar='"')
    for row in reader:
        if i == 0:
            data = pd.DataFrame(columns=row)
        else:
            data.loc[i] = row
        i += 1
        if (i % 10000) == 0:
            print(datetime.datetime.now(), i)
        # if i > 10:
        #     break

# data = pd.read_csv(url)

# data = pd.read_csv("../orp/ockovani-profese.csv")

pt_db = data.pivot_table(index=["kraj_nuts_kod", 'vekova_skupina', 'poradi_davky'], values='id', aggfunc='count', dropna=False).reset_index().fillna(0)

pt_db.to_csv("test.csv")