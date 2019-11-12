import pymongo
from pymongo import MongoClient
import csv
from collections import defaultdict
import pandas as pd
import numpy as np
import MySQLdb
from sqlalchemy import create_engine


host = '134.99.112.190'
client = MongoClient(host, 27017)
db = client.finfraud3
db.authenticate("read_user", "tepco11x?z")

COLLECTION = "original"
db_coll_ori = db[COLLECTION]

rics = []
queryArgs = {}
projectionFields = {'ric':True}
searchRes = db_coll_ori.find(queryArgs, projection = projectionFields)

for record in searchRes:
    rics.append(record["ric"])


restatements_dict = dict()
#amount=10
for document in db_coll_ori.find():
    #if amount < 0:
        #break
    #amount = amount -1
    #print(document['ric'])
    restatements_dict[document['ric']]={}
    for y in range(1998,2017):
        restatements_dict[document['ric']][str(y)]={
            "all":0,
            "relevant":0,
            "relevant5%":0
        }


COLLECTION = "ground_truth"
db_coll_rest = db[COLLECTION]

for ric1 in rics:
    for y in range(1998, 2017):
        a = db_coll_rest.find_one({'ric': ric1})
        if a is not None:
            if str(y) in a.keys():
                relevant5 = 0
                relevant = 0
                all_r = 0
                yearly_data = dict(a[str(y)])
                if ('TR-NetIncome' in yearly_data.keys()):
                    if (a[str(y)]['TR-NetIncome'] >= 5):
                        relevant5 = 1
                    else:
                        relevant = 1
                else:
                    all_r = 1

                if ('TR-TotalEquity' in yearly_data.keys()):
                    if (a[str(y)]['TR-TotalEquity'] >= 5):
                        relevant5 = 1
                    else:
                        relevant = 1
                else:
                    all_r = 1

                if ('TR-CashFromOperatingAct' in yearly_data.keys()):
                    if (a[str(y)]['TR-CashFromOperatingAct'] >= 5):
                        relevant5 = 1
                    else:
                        relevant = 1
                else:
                    all_r = 1

                if ('TR-NetSales' in yearly_data.keys()):
                    if (a[str(y)]['TR-NetSales'] >= 5):
                        relevant5 = 1
                    else:
                        relevant = 1
                else:
                    all_r = 1

                if relevant5 == 1:
                    restatements_dict[ric1][str(y)]['relevant5%'] = 1
                    restatements_dict[ric1][str(y)]['relevant'] = 1
                    restatements_dict[ric1][str(y)]['all'] = 1
                elif relevant == 1:
                    restatements_dict[ric1][str(y)]['relevant'] = 1
                    restatements_dict[ric1][str(y)]['all'] = 1
                elif all_r == 1:
                    restatements_dict[ric1][str(y)]['all'] = 1

with open(f"restatements.csv", "w") as csvfileWriter:
    writer = csv.writer(csvfileWriter)

    fieldList = [
        "ric",
        "year",
        "all",
        "relevant",
        "relevant5%"
    ]
    writer.writerow(fieldList)

    for ric1 in rics:
        for y in range(1998, 2017):
            recordValueLst = [ric1,
                              str(y),
                              restatements_dict[ric1][str(y)]['all'],
                              restatements_dict[ric1][str(y)]['relevant'],
                              restatements_dict[ric1][str(y)]['relevant5%']]
            try:
                writer.writerow(recordValueLst)
            except Exception as e:
                print(f"write csv exception. e = {e}")

keys = []

a = db_coll_ori.find_one({'ric': 'WMT'})
yearly_data = dict(a['1998'])

tech_names = {'FQ-1', 'FQ-3', 'FQ-4', 'FQ0'}
yearly_data_without_FQ = {key: value for key, value in yearly_data.items() if key not in tech_names}

for b in yearly_data_without_FQ:
    keys.append(b)

print(keys)

dic = dict()
amo = 3
for ric1 in rics:
    if amo < 0:
        break
    amo = amo - 1

    a = db_coll_ori.find_one({'ric': ric1})

    for key1 in keys:
        for y in range(1998, 2018):
            dic.setdefault(key1, []).append(a[str(y)][key1])

print(dic)



df = pd.DataFrame(dic,dtype=np.float64)
df.drop(df.columns[3], axis=1, inplace=True)
print(df)

df.corr(method='pearson')
corr = df.corr(method='pearson')
print(corr)

host = '127.0.0.1'
port = 3306
db = 'test'
user = 'root'
password = 'H19930918'

engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s") % (user, password, host, db))

corr.to_sql('test1',con=engine,if_exists='append',index=False)