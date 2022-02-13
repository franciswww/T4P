import sys
import glob
import re
import urllib.request
from datetime import timedelta,datetime
import pandas as pd
import mysql.connector
import math

mydb = mysql.connector.connect(
  host="francisww.asuscomm.com",
  port=9906,
  user="t4user",
  password="t4user",
  database="world"
)

# import sqlalchemy
# database_username = 'ENTER USERNAME'
# database_password = 'ENTER USERNAME PASSWORD'
# database_ip       = 'ENTER DATABASE IP'
# database_name     = 'ENTER DATABASE NAME'
# database_connection = sa.create_engine('mysql+pyodbc://{0}:{1}@{2}/{3}'.
#                                                format('t4user', 't4user', 
#                                                       '192.168.1.128', 'world'))

from pandas.io import sql
#import MySQLdb                                                      

def xstr(s):
    if s is None:
        return ''
    return str(s)

basepath = 'Z:\\hkexdata\\'
months = ['202007','202009']
files = ['*_TR_AHT.csv' , '*_01_TR.csv']


#my_conn=create_engine("mysql+mysqldb://t4user:t4user@francisww.asuscomm.com:9906/world")
cursor=mydb.cursor()
for mth in months:
    path  = basepath + '\\' + mth + '\\'
    for f in files:
        for file in glob.glob(path + mth  + f):
            print(file)
            table = 'temp'+mth
            #print ('tablename:' + table)
            df = pd.read_csv(file, dtype=str, header=None)
            #print(df)
            # creating column list for insertion
            cols = "`,`".join([str(i) for i in df.columns.tolist()])
            #print(cols)
            i=1
            my_data = []
            sql = "INSERT INTO trades (dt,underly,contractmonth,strike,cp,p,v) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            for i,row in df.iterrows():

                # if row[1]!='F':
                #     continue
                dt=datetime.strptime(str(row[5])+str(row[6]),'%Y%m%d%H%M%S')
                contractmonth = datetime.strptime(str(row[2]),'%Y%m%d')
                contractmonth=contractmonth.replace(day=1)
                strike=row[3]
                if row[4]!='C' and row[4]!='P':
                    cp=''
                else:
                    cp=row[4]
                # cp=row[4]
                p=row[7]
                v=row[8]
                underly=row[0]
                #print(row)
                #print(tuple(row))

                val = (dt,underly,contractmonth,strike,cp,p,v)
                #print (val)

                try:
                    sql = "INSERT INTO trades (dt,underly,contractmonth,strike,cp,p,v) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                    #print (sql)
                    #print(val)
                    #cursor.execute(sql, val)
                    my_data.append(val)
                    i=i+1

                except:
                    print(sql)
                    sys.exit()
                # if i==10:
                #     break
                if i%10000==0:
                    #mydb.commit()
                    #Use Execute Many to save time
                    cursor.executemany(sql, my_data)
                    print(file + ' batch inserted ' + str(i) + ' rows')
                    mydb.commit()
                    my_data = []

            cursor.executemany(sql, my_data)
            print(file + ' batch inserted ' + str(i) + ' rows')
            mydb.commit()
            # df.to_sql(con=database_connection, name=table, if_exists='replace', index=False)
            # sql.write_frame(df, con=mydb, name='t202008', 
            #                 if_exists='replace', flavor='mysql')