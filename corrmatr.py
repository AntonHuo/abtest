import pymongo
from pymongo import MongoClient
import csv
from collections import defaultdict
import pandas as pd
import numpy as np
import MySQLdb
from sqlalchemy import create_engine
import seaborn as sns


host = '134.99.112.190'
client = MongoClient(host, 27017)
db = client.finfraud3
db.authenticate("read_user", "tepco11x?z")

print('gut')

COLLECTION = "original"
db_coll_ori = db[COLLECTION]

data = db_coll_ori.find()
data = list(data)

df = pd.DataFrame(data)

print('bitte')

data_dict=df.to_dict(orient= 'dict')

keys = []
yearly_data = data_dict['1998'][0]
tech_names = {'FQ-1', 'FQ-3', 'FQ-4', 'FQ0'}
yearly_data_without_FQ = {key: value for key, value in yearly_data.items() if key not in tech_names}
for b in yearly_data_without_FQ:
    keys.append(b)

print(keys)

dic = dict()

for y in range(1998, 2018):
    for x in range(0, df.shape[0]):
        for key1 in keys:
            yearly_data = data_dict[str(y)][x]
            print(x)

            if key1 in yearly_data.keys():
                dic.setdefault(key1, []).append(data_dict[str(y)][x][key1])
            else:
                dic.setdefault(key1, []).append('nan')

df = pd.DataFrame(dic,dtype=np.float64)
df.drop(df.columns[3], axis=1, inplace=True)

df.corr(method='pearson')
corr = df.corr(method='pearson')

print('corr gut')

host = '127.0.0.1'
port = 3306
db = 'test'
user = 'root'
password = 'osboxes.org'

engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s") % (user, password, host, db))

print('mysql gut')

corr.to_sql('test1',con=engine,if_exists='append',index=False)

print('endlich')