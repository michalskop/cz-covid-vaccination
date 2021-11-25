"""Vaccination CZ - tables for maps."""

import csv
import datetime
import io
import numpy as np
import pandas as pd
import requests
# from contextlib import closing



url = 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-profese.csv'

r0 = requests.head(url)
nbytes = int(r0.headers['Content-Length'])
step = 10000000


def create_range(i):
    """Creates Range header."""
    return f'bytes={i * step}-{(i + 1) * step + 1000}'

# first row is header
r = requests.get(url, headers={'Range': 'bytes=3-1000'})    # first bytes are something else
buff = io.StringIO(r.text)
dr = csv.reader(buff)
for row in dr:
    header = row
    break
data = pd.DataFrame(columns=header)

# get all data
for i in range(0, nbytes // step + 1):
    print(datetime.datetime.now(), i)
    r = requests.get(url, headers={"Range": create_range(i)})
    print(datetime.datetime.now(), i, 'downloaded')
    # pd.read_csv(io.StringIO(r.text), header=None, skiprows=1, error_bad_lines=False, engine="python").to_csv('data_' + str(i) + '.csv', index=False)
    t = pd.read_csv(io.StringIO(r.text), header=None, skiprows=1, error_bad_lines=False, engine="python")
    # t = pd.read_csv('data_' + str(i) + '.csv', header=None).reset_index(drop=True)
    t.columns = header
    data = data.append(t.iloc[1:])


    # buff = io.StringIO(r.text)
    # dr = csv.reader(buff)
    # for row in dr:
    #     if len(row[0]) == 36:
    #         if row[0] not in ids:
    #             try:
    #                 data.loc[j] = row
    #                 ids.append(row[0])
    #                 j += 1
    #             except:
    #                 print(row)
    #                 nothing = 1
    # if i > 5:
    #     break

data.duplicated().sum()
data.duplicated('id').sum()
# drop duplicates
data.drop_duplicates(subset=['id'], keep='last', inplace=True)
# drop rows with len(id) != 36
data = data.where(data['id'].apply(len) == 36)

# r = requests.get(url, encoding="utf-8", headers={"Range": "bytes=10000-20000"})
# buff = io.StringIO(r.text)
# dr = csv.reader(buff)
# for row in dr:
#     print(row)


# i = 0
# print(datetime.datetime.now(), i)
# with closing(requests.get(url, stream=True)) as r:
#     f = (line.decode('utf-8') for line in r.iter_lines())
#     reader = csv.reader(f, delimiter=',', quotechar='"')
#     for row in reader:
#         if i == 0:
#             data = pd.DataFrame(columns=row)
#         else:
#             try:
#                 data.loc[i] = row
#             except:
#                 print(row)
#         i += 1
#         if (i % 10000) == 0:
#             print(datetime.datetime.now(), i)
        # if i > 10:
        #     break

# data = pd.read_csv(url)

# data = pd.read_csv("../orp/ockovani-profese.csv")

pt_db = data.pivot_table(index=["kraj_nuts_kod", 'vekova_skupina', 'poradi_davky'], values='id', aggfunc='count', dropna=False).reset_index().fillna(0)

pt_db.to_csv("test.csv")