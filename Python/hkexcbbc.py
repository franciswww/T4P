from cmath import nan
import sys
import glob
import re
import urllib.request
from datetime import timedelta,datetime
import pandas as pd
import mysql.connector
import math
import zipfile
import csv

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
import numpy as np

def xstr(s):
    if s is None:
        return ''
    return str(s)

def agg_ohlcv(x):
    arr = x['price'].values
    names = {
        'low': min(arr) if len(arr) > 0 else np.nan,
        'high': max(arr) if len(arr) > 0 else np.nan,
        'open': arr[0] if len(arr) > 0 else np.nan,
        'close': arr[-1] if len(arr) > 0 else np.nan,
        'volume': sum(x['quantity'].values) if len(x['quantity'].values) > 0 else 0,
    }
    return pd.Series(names)

basepath = 'Z:\\hkexdata\\cbbc\\'

#my_conn=create_engine("mysql+mysqldb://t4user:t4user@francisww.asuscomm.com:9906/world")
cursor=mydb.cursor()
cursor.fast_executemany = True


def Fillna(data_frame):
    data_frame['quantity'] = data_frame['quantity'].fillna(0.0)  # volume should always be 0 (if there were no trades in this interval)
    data_frame['close'] = data_frame.fillna(method='pad')  # ie pull the last close into this close
    # now copy the close that was pulled down from the last timestep into this row, across into o/h/l
    data_frame['open'] = data_frame['open'].fillna(data_frame['close']) 
    data_frame['low'] = data_frame['low'].fillna(data_frame['close'])
    data_frame['high'] = data_frame['high'].fillna(data_frame['close'])

def GetOrInsertContract( type, underly, contract, strike, cp):

    if (len(contract)==4):
        contract = "20" + contract + "01"

    mycursor = mydb.cursor()
    sql = """SELECT id FROM `hkex_contract_spec` WHERE type = '%s' and underly = '%s' and contractmonth = '%s' and strike = '%s' and cp = '%s'""" % (type, underly, contract, strike, cp)
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    #if (myresult[0][0] != None):
    if (len(myresult) != 0):
        return myresult[0][0]
    else:
        val = (type,contract,underly,strike,cp)
        sql = "INSERT INTO `hkex_contract_spec` (type,contractMonth,underly,strike,cp) VALUES (%s,%s,%s,%s,%s)"
        mycursor.execute(sql, val)
        id = mycursor.lastrowid
        mydb.commit()
        return id  

def walkfolderforCBBC():
    for f in glob.glob(basepath + '**\\CBBC*.zip', recursive=True):
        yield(f)

def NAtoFloat(f):
    val=0
    if (f==nan or f=='-'):
        val=0
    else:
        val=f
    return val

def ProcessHKExCBBCFilesZip(zipfn):

    mycursor = mydb.cursor()
    dict = {}
    zip = zipfile.ZipFile(zipfn)
    zip.namelist()
    print(zipfn)
    mth = zipfn[-6:-4]
    if not ( mth=='01'):
        return

    fn = "CBBC" + zipfn[-6:-4] + ".csv"        
    #with zip.open(fn,mode='r') as thefile:
    #    #Let us verify the operation..
    #    print(thefile.read())

    #print(fn[-6:-4])
    df = pd.read_csv(zip.open(fn), encoding='utf-16', sep='\t', quoting=csv.QUOTE_ALL, error_bad_lines=False )
    
    df = df[pd.to_numeric(df['CBBC Code'], errors='coerce').notnull()]
    df = df.fillna(0)
    print(df)

    i=1
    my_data = []
    sql = "REPLACE INTO hkex_cbbc (cbbc_code,cbbc_name,dt,bv,bavg,sv,savg,os,pctos,ttl_iss_size,ccy,h,l,c,v,turnover,issuer,underly,bullbear,type,category,listing_date,last_trading_date,maturity_date,mce,strike_ccy,strike_lvl,call_lvl,ent_ratio,delisting_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for i,row in df.iterrows():
        cbbc_code=row[0]
        cbbc_name=row[1]
        dt=datetime.strptime(str(row[2]),'%Y-%m-%d')
        bv=row[3]
        bavg=row[4]
        sv=row[5]
        savg=row[6]
        os=row[7]
        pctos=row[8]
        ttl_iss_size=row[9]
        ccy=row[10]
        h=NAtoFloat(row[11])
        l=NAtoFloat(row[12])
        c=NAtoFloat(row[13])
        v=NAtoFloat(row[14])
        turnover=NAtoFloat(row[15])
        issuer=row[16]
        underly=row[17]
        bullbear=row[18]
        type=row[19]
        category=row[20]
        listing_date=datetime.strptime(str(row[21]),'%Y-%m-%d')
        if str(row[22])!='-':
            last_trading_date=datetime.strptime(str(row[22]),'%Y-%m-%d')
        else:
            last_trading_date=None
        maturity_date=datetime.strptime(str(row[23]),'%Y-%m-%d')
        mce=row[24]
        strike_ccy=row[25]
        strike_lvl=row[26]
        call_lvl=row[27]
        ent_ratio=row[28]
        if str(row[29])!='-':
            delisting_date=datetime.strptime(str(row[29]),'%Y-%m-%d')
        else:
            delisting_date=None
        val = (cbbc_code,cbbc_name,dt,bv,bavg,sv,savg,os,pctos,ttl_iss_size,ccy,h,l,c,v,turnover,issuer,underly,bullbear,type,category,listing_date,last_trading_date,maturity_date,mce,strike_ccy,strike_lvl,call_lvl,ent_ratio,delisting_date)
        #print(val)
        i=i+1
        my_data.append(val)

        try:
            if i%10000==0:
            #if i>40000 and i<50000:
                #mydb.commit()
                #Use Execute Many to save time
                cursor.executemany(sql, my_data)
                print(' batch inserted ' + str(i) + ' rows')
                mydb.commit()
                my_data = []
        except mysql.connector.Error as err:
            print(err)
            #sys.exit()

    cursor.executemany(sql, my_data)
    print(' batch inserted ' + str(i) + ' rows')
    mydb.commit()
    my_data = []


#ProcessHKExCBBCFilesZip()
for fn in walkfolderforCBBC():
    ProcessHKExCBBCFilesZip(fn)