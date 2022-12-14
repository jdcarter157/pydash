# imports

import json
import os
import pandas as pd
import numpy as np
import time
import mysql.connector
from datetime import datetime

now = datetime.now()


# connect to db
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="ecoins"
)

mycursor = mydb.cursor()

# path we are working with
dir_path = r'/Users/jordan_carter/Downloads/jim_jsons/data'

### select coin
cname ="BITMEX:ETHUSD_ETH"
# cname = ["BITMEX:LTCUSD","BITMEX:BLTC","BITMEX:ETHUSD_ETH"]
# c=0
# if c<=len(cname):
#     sql1 = "SELECT COUNT(*) FROM coin_data where cname = %s"
#     sql2 = (cname[c])
#     mycursor.execute(sql1, sql2)
#     myresult = mycursor.fetchall()
#     numofrows = myresult[0][0]
#     print("number of rows-->", numofrows)
#     c=c+1
# for c in cname:
sql1 = "SELECT COUNT(*) FROM coin_data where cname = %s"
sql2 = (cname,)
mycursor.execute(sql1, sql2)
myresult = mycursor.fetchall()
numofrows = myresult[0][0]
print("number of rows-->", numofrows)

### ------------------------ lows!!!!!!!

### numrows don't go too far!
numofrows -= 10

baseplus = 0
goback = 10
for x in range(0, numofrows):
    baseplus = numofrows + goback
    sql1 = " SELECT * FROM coin_data WHERE cname = %s LIMIT %s , %s"
    sql2 = (cname, x, baseplus)

    mycursor.execute(sql1, sql2)
    # mydb.commit()

    myresult = mycursor.fetchall()

    low = 10000
    thislow = 0
    for x in range(0, 10):
            thislow = float(myresult[x][3])
            if (thislow < low):
                low = thislow
                theid = myresult[x][0]
                sql1 = "UPDATE coin_data SET lows = %s where id = %s "
                sql2 = (low, theid)
                mycursor.execute(sql1, sql2)
                mydb.commit()

                print("low:", low)
                print("the id:", theid)

### ---------------------------- highs!!!!!!!!!!!!

                baseplus = 0
                goback = 10
for x in range(0, numofrows):
    baseplus = numofrows + goback
    sql1 = " SELECT * FROM coin_data WHERE cname = %s LIMIT %s , %s"
    sql2 = (cname, x, baseplus)

    mycursor.execute(sql1, sql2)
    # mydb.commit()

    myresult = mycursor.fetchall()

    high = 0
    thishigh = 0
    for x in range(0, 10):
        thishigh = float(myresult[x][3])
        if (thishigh > high):
            high = thishigh
            print(" new high : ", high)
            theid = myresult[x][0]
            print("theid:", theid)
            sql1 = "UPDATE coin_data SET highs = %s where id = %s "
            sql2 = (high, theid)
            mycursor.execute(sql1, sql2)
            mydb.commit()

            print("high", high)
            print("the id:", theid)

### --------------------------------------
