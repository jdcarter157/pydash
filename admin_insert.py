# imports
import json
import os
import pandas as pd
import numpy as np
import time
from datetime import datetime

now = datetime.now()
import mysql.connector
# connect to db
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="ecoins"
)
# path we are working with
dir_path = r'/Users/jordan_carter/Downloads/jim_jsons/data/'
# list to store files
res = []
changelist = []
# go through all the files in the directory
for path in os.listdir(dir_path):
    if os.path.isfile(os.path.join(dir_path, path)):
        res.append(path)
filename = "/Users/jordan_carter/Downloads/jim_jsons/data/" + res[0]

mycursor = mydb.cursor()
## each file contains dates, and within them prices of coins

print("loading data...")
for nm in res:
    print(nm)
    filename = "/Users/jordan_carter/Downloads/jim_jsons/data/" + nm
    try:
        f = open(filename, 'r')
        data = json.load(f)
        for i in data['data']:
            cryptotype = (i['d'][1])
            cystring = str(cryptotype)
            finds = str(i['s'])
### for now, use only 3 coins
###
            if (finds == "BITMEX:BLTC" or finds == "BITMEX:LTCUSD" or finds == "BITMEX:ETHUSD_ETH"):
                stringbreak = str(nm)
                print("stringbreak-->", stringbreak)
                year = stringbreak[6:10]
                month = stringbreak[0:2]
                day = stringbreak[3:5]
                hour = stringbreak[11:13]
                minute = stringbreak[14:16]
                second = 00
                price = str(i['d'][3])
                formatted_date = str(year) + "-" + str(month) + "-" + str(day) + "-" + str(hour) + "-" + str(minute) + "-" + str(second)
                print("formatted date-->", formatted_date)
                sql1 = "INSERT INTO coin_data ( cname, cdate, price ) VALUES (%s, %s, %s)"
                sql2 = (finds, formatted_date, price,)

                mycursor.execute(sql1, sql2)
                mydb.commit()
    except: pass
